#include "databricks_scanner.hpp"

#include "databricks_arrow.hpp"
#include "databricks_utils.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/table_column.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"

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
	return std::move(result);
}

bool DatabricksScanBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<DatabricksScanBindData>();
	return session == other.session && catalog == other.catalog && schema == other.schema && table == other.table &&
	       query == other.query && executed == other.executed;
}

string DatabricksScanFunction::BuildQuery(const DatabricksScanBindData &bind_data, const vector<column_t> &column_ids) {
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
	return "SELECT " + StringUtil::Join(select_list, ", ") + " FROM " +
	       DatabricksQualifiedName(bind_data.catalog, bind_data.schema, bind_data.table);
}

namespace {

struct DatabricksScanGlobalState : public GlobalTableFunctionState {
	shared_ptr<DatabricksSession> session;
	DatabricksStatementResult result;
	atomic<idx_t> next_chunk {0};
	vector<column_t> column_ids;
	string sql;

	idx_t MaxThreads() const override {
		return 1;
	}
};

struct DatabricksScanLocalState : public LocalTableFunctionState {
	vector<DatabricksArrowBatch> batches;
	idx_t batch_index = 0;
	idx_t offset = 0;
	unique_ptr<ArrowScanLocalState> arrow_state;
	bool exhausted = false;
};

unique_ptr<GlobalTableFunctionState> ScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<DatabricksScanBindData>();
	auto result = make_uniq<DatabricksScanGlobalState>();
	result->session = bind_data.session;
	result->column_ids = input.column_ids;
	if (bind_data.executed) {
		result->result = bind_data.result;
		result->sql = bind_data.query;
		return std::move(result);
	}
	result->sql = DatabricksScanFunction::BuildQuery(bind_data, input.column_ids);
	result->result = bind_data.session->Execute(context, DatabricksStatementMode::SCAN, result->sql, bind_data.catalog,
	                                            bind_data.schema, {});
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
		}
	}
	local.arrow_state->chunk_offset = local.offset;
	auto projected = bind_data.query.empty();
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
	return result;
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
	function.get_bind_info = GetBindInfo;
	function.projection_pushdown = true;
	function.get_virtual_columns = GetVirtualColumns;
}

DatabricksScanFunction::DatabricksScanFunction() : TableFunction("databricks_scan", {}, ScanFunction, nullptr) {
	SetScanCallbacks(*this);
}

} // namespace duckdb
