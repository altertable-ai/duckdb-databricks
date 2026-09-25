#include "databricks_scanner.hpp"

#include "databricks_arrow.hpp"
#include "databricks_expression.hpp"
#include "databricks_utils.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/table_column.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <algorithm>

namespace duckdb {

unique_ptr<FunctionData> DatabricksScanBindData::Copy() const {
	auto result = make_uniq<DatabricksScanBindData>();
	result->session = session;
	result->catalog = catalog;
	result->schema = schema;
	result->table = table;
	result->query = query;
	result->executed = executed;
	result->result = this->result;
	result->columns = columns;
	result->table_entry = table_entry;
	result->lifetime = lifetime;
	result->filter_pushdown = filter_pushdown;
	result->extra_filter = extra_filter;
	result->order_by_clause = order_by_clause;
	result->limit_clause = limit_clause;
	return std::move(result);
}

bool DatabricksScanBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<DatabricksScanBindData>();
	return session == other.session && catalog == other.catalog && schema == other.schema && table == other.table &&
	       query == other.query && executed == other.executed && filter_pushdown == other.filter_pushdown &&
	       order_by_clause == other.order_by_clause && limit_clause == other.limit_clause &&
	       extra_filter == other.extra_filter;
}

string DatabricksScanFunction::BuildQuery(const DatabricksScanBindData &bind_data, const vector<column_t> &column_ids,
                                          optional_ptr<TableFilterSet> filters) {
	bool only_virtual = column_ids.empty();
	vector<string> select_list;
	for (auto column_id : column_ids) {
		if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
			select_list.push_back("CAST(NULL AS BIGINT)");
			continue;
		}
		if (column_id == COLUMN_IDENTIFIER_EMPTY) {
			only_virtual = select_list.empty();
			continue;
		}
		only_virtual = false;
		if (column_id >= bind_data.columns.size()) {
			throw InternalException("Databricks scan column id %llu is out of range", column_id);
		}
		auto &column = bind_data.columns[column_id];
		if (StringUtil::CIEquals(column.type_text, "VOID")) {
			select_list.push_back("CAST(NULL AS INT)");
		} else {
			select_list.push_back(DatabricksQuoteIdentifier(column.name));
		}
	}
	if (select_list.empty() || only_virtual) {
		select_list = {"1"};
	}
	auto sql = "SELECT " + StringUtil::Join(select_list, ", ") + " FROM " +
	           DatabricksQualifiedName(bind_data.catalog, bind_data.schema, bind_data.table);
	vector<string> predicates;
	if (bind_data.filter_pushdown && filters) {
		auto where = DatabricksExpressions::TransformFilters(column_ids, filters, bind_data.columns);
		if (!where.empty()) {
			predicates.push_back(where);
		}
	}
	if (bind_data.filter_pushdown && !bind_data.extra_filter.empty()) {
		predicates.push_back(bind_data.extra_filter);
	}
	if (!predicates.empty()) {
		sql += " WHERE " + StringUtil::Join(predicates, " AND ");
	}
	sql += bind_data.order_by_clause;
	sql += bind_data.limit_clause;
	return sql;
}

namespace {

struct DatabricksScanGlobalState : public GlobalTableFunctionState {
	shared_ptr<DatabricksSession> session;
	DatabricksStatementResult result;
	atomic<idx_t> next_chunk {0};
	vector<column_t> column_ids;
	vector<idx_t> projection_ids;
	string sql;
	idx_t max_threads = 1;

	idx_t MaxThreads() const override {
		return max_threads;
	}
};

struct DatabricksScanLocalState : public LocalTableFunctionState {
	vector<DatabricksArrowBatch> batches;
	idx_t batch_index = 0;
	idx_t offset = 0;
	unique_ptr<ArrowScanLocalState> arrow_state;
	bool exhausted = false;
};

optional_ptr<TableFilterSet> StaticScanFilters(TableFunctionInitInput &input) {
	if (input.op) {
		return input.op->Cast<PhysicalTableScan>().table_filters.get();
	}
	return input.filters;
}

unique_ptr<GlobalTableFunctionState> ScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<DatabricksScanBindData>();
	auto result = make_uniq<DatabricksScanGlobalState>();
	result->session = bind_data.session;
	result->column_ids = input.column_ids;
	result->projection_ids = input.projection_ids;
	if (bind_data.executed) {
		result->result = bind_data.result;
		result->sql = bind_data.query;
	} else {
		result->sql = DatabricksScanFunction::BuildQuery(bind_data, input.column_ids, StaticScanFilters(input));
		result->result = bind_data.session->Execute(context, DatabricksStatementMode::SCAN, result->sql,
		                                            bind_data.catalog, bind_data.schema, {});
	}
	auto chunks = std::max(result->result.total_chunk_count, idx_t(1));
	if (bind_data.order_by_clause.empty()) {
		auto threads = std::max(context.db->NumberOfThreads(), idx_t(1));
		result->max_threads = std::min(chunks, threads);
	}
	return std::move(result);
}

unique_ptr<LocalTableFunctionState> ScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                  GlobalTableFunctionState *global_state) {
	(void)context;
	(void)input;
	(void)global_state;
	return make_uniq<DatabricksScanLocalState>();
}

bool LoadNextChunk(ClientContext &context, DatabricksScanGlobalState &global, DatabricksScanLocalState &local) {
	local.arrow_state.reset();
	local.batches.clear();
	local.batch_index = 0;
	local.offset = 0;
	while (true) {
		auto chunk_index = global.next_chunk++;
		if (chunk_index >= global.result.total_chunk_count) {
			local.exhausted = true;
			return false;
		}
		auto payloads = global.session->DownloadChunk(context, global.result, chunk_index);
		for (auto &payload : payloads) {
			auto decoded = DatabricksDecodeArrow(context, payload);
			for (auto &batch : decoded) {
				local.batches.push_back(std::move(batch));
			}
		}
		if (!local.batches.empty()) {
			return true;
		}
	}
}

void ScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<DatabricksScanBindData>();
	auto &global = data.global_state->Cast<DatabricksScanGlobalState>();
	auto &local = data.local_state->Cast<DatabricksScanLocalState>();
	if (local.exhausted) {
		output.SetCardinality(0);
		return;
	}
	while (local.batch_index >= local.batches.size()) {
		if (!LoadNextChunk(context, global, local)) {
			output.SetCardinality(0);
			return;
		}
	}
	auto &batch = local.batches[local.batch_index];
	auto length = static_cast<idx_t>(batch.array->arrow_array.length);
	if (local.offset >= length) {
		local.batch_index++;
		local.offset = 0;
		local.arrow_state.reset();
		ScanFunction(context, data, output);
		return;
	}
	auto count = std::min(length - local.offset, static_cast<idx_t>(STANDARD_VECTOR_SIZE));
	output.SetCardinality(count);
	if (output.ColumnCount() == 0) {
		local.offset += count;
		if (local.offset >= length) {
			local.batch_index++;
			local.offset = 0;
			local.arrow_state.reset();
		}
		return;
	}
	if (!local.arrow_state) {
		auto wrapper = make_uniq<ArrowArrayWrapper>();
		wrapper->arrow_array = batch.array->arrow_array;
		batch.array->arrow_array.release = nullptr;
		local.arrow_state = make_uniq<ArrowScanLocalState>(std::move(wrapper), context);
		if (!bind_data.query.empty()) {
			local.arrow_state->column_ids = global.column_ids;
		} else if (!global.projection_ids.empty()) {
			local.arrow_state->column_ids = global.projection_ids;
		}
	}
	local.arrow_state->chunk_offset = local.offset;
	auto projected = bind_data.query.empty() && global.projection_ids.empty();
	ArrowTableFunction::ArrowToDuckDB(*local.arrow_state, batch.schema.GetColumns(), output, projected,
	                                  COLUMN_IDENTIFIER_ROW_ID);
	local.offset += count;
	if (local.offset >= length) {
		local.batch_index++;
		local.offset = 0;
		local.arrow_state.reset();
	}
}

InsertionOrderPreservingMap<string> ScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<DatabricksScanBindData>();
	if (bind_data.query.empty()) {
		result["Table"] = bind_data.catalog + "." + bind_data.schema + "." + bind_data.table;
	} else {
		result["Query"] = bind_data.query;
	}
	auto pushed = bind_data.order_by_clause + bind_data.limit_clause;
	StringUtil::Trim(pushed);
	if (!pushed.empty()) {
		result["Pushed Down"] = pushed;
	}
	return result;
}

InsertionOrderPreservingMap<string> ScanDynamicToString(TableFunctionDynamicToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	if (input.global_state) {
		result["SQL"] = input.global_state->Cast<DatabricksScanGlobalState>().sql;
	}
	return result;
}

bool SupportsPushdownType(const FunctionData &bind_data_p, idx_t column_index) {
	auto &bind_data = bind_data_p.Cast<DatabricksScanBindData>();
	if (!bind_data.filter_pushdown || column_index >= bind_data.columns.size()) {
		return false;
	}
	return DatabricksExpressions::SupportsFilterPushdown(bind_data.columns[column_index]);
}

static bool IsPrefixLikePattern(const string &pattern) {
	if (pattern.empty() || pattern.back() != '%' || pattern.size() == 1) {
		return false;
	}
	auto prefix = pattern.substr(0, pattern.size() - 1);
	return prefix.find('%') == string::npos && prefix.find('_') == string::npos;
}

void PushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                           vector<unique_ptr<Expression>> &filters) {
	(void)context;
	if (!bind_data_p) {
		return;
	}
	auto &bind_data = bind_data_p->Cast<DatabricksScanBindData>();
	if (!bind_data.filter_pushdown) {
		return;
	}
	vector<unique_ptr<Expression>> remaining;
	vector<string> parts;
	for (auto &filter : filters) {
		string sql;
		if (DatabricksExpressions::TryTranslateExpression(bind_data.columns, get, *filter, sql)) {
			parts.push_back(sql);
		} else {
			remaining.push_back(std::move(filter));
		}
	}
	filters = std::move(remaining);
	if (!parts.empty()) {
		if (!bind_data.extra_filter.empty()) {
			bind_data.extra_filter += " AND ";
		}
		bind_data.extra_filter += StringUtil::Join(parts, " AND ");
	}
}

bool PushdownExpression(ClientContext &context, const LogicalGet &get, Expression &expr) {
	(void)context;
	if (!get.bind_data) {
		return false;
	}
	auto &bind_data = get.bind_data->Cast<DatabricksScanBindData>();
	if (!bind_data.filter_pushdown) {
		return false;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_BETWEEN) {
		auto &between = expr.Cast<BoundBetweenExpression>();
		return between.input->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
		       between.lower->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
		       between.upper->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.function.name != "~~" || func.children.size() < 2) {
			return false;
		}
		if (func.children[0]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
		    func.children[1]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
			return false;
		}
		auto &constant = func.children[1]->Cast<BoundConstantExpression>();
		if (constant.value.IsNull() || constant.value.type().id() != LogicalTypeId::VARCHAR) {
			return false;
		}
		return IsPrefixLikePattern(StringValue::Get(constant.value));
	}
	return false;
}

BindInfo GetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<DatabricksScanBindData>();
	if (bind_data.table_entry) {
		return BindInfo(*bind_data.table_entry);
	}
	return BindInfo(ScanType::EXTERNAL);
}

virtual_column_map_t GetVirtualColumns(ClientContext &context, optional_ptr<FunctionData> bind_data) {
	(void)context;
	(void)bind_data;
	virtual_column_map_t result;
	result.emplace(COLUMN_IDENTIFIER_ROW_ID, TableColumn("rowid", LogicalType::BIGINT));
	return result;
}

} // namespace

void DatabricksScanFunction::SetScanCallbacks(TableFunction &function) {
	function.init_global = ScanInitGlobal;
	function.init_local = ScanInitLocal;
	function.function = ScanFunction;
	function.to_string = ScanToString;
	function.dynamic_to_string = ScanDynamicToString;
	function.get_bind_info = GetBindInfo;
	function.projection_pushdown = true;
	function.filter_pushdown = true;
	function.filter_prune = true;
	function.supports_pushdown_type = SupportsPushdownType;
	function.pushdown_complex_filter = PushdownComplexFilter;
	function.pushdown_expression = PushdownExpression;
	function.get_virtual_columns = GetVirtualColumns;
}

DatabricksScanFunction::DatabricksScanFunction() : TableFunction("databricks_scan", {}, ScanFunction, nullptr) {
	SetScanCallbacks(*this);
}

} // namespace duckdb
