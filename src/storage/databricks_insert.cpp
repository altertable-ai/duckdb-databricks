#include "storage/databricks_insert.hpp"

#include "databricks_literal.hpp"
#include "databricks_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "storage/databricks_catalog.hpp"
#include "databricks_ddl.hpp"
#include "storage/databricks_table_entry.hpp"

namespace duckdb {

static constexpr idx_t INSERT_HARD_CAP = static_cast<idx_t>(16) * 1024 * 1024;

DatabricksInsert::DatabricksInsert(PhysicalPlan &physical_plan, LogicalOperator &op, DatabricksTableEntry &table,
                                   vector<DatabricksInsertColumn> columns_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1),
      catalog_name(table.catalog.GetName()), schema_name(table.schema.name), table_name(table.name),
      columns(std::move(columns_p)) {
	vector<string> names;
	for (auto &column : columns) {
		names.push_back(DatabricksQuoteIdentifier(column.name));
	}
	insert_prefix =
	    "INSERT INTO " +
	    DatabricksQualifiedName(table.catalog.Cast<DatabricksCatalog>().GetConfig().catalog, schema_name, table_name) +
	    " (" + StringUtil::Join(names, ", ") + ") VALUES ";
}

DatabricksInsert::DatabricksInsert(PhysicalPlan &physical_plan, LogicalOperator &op, DatabricksCatalog &catalog,
                                   const string &schema, unique_ptr<CreateTableInfo> create_info_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), catalog_name(catalog.GetName()),
      schema_name(schema), table_name(create_info_p->table), create_info(std::move(create_info_p)) {
}

vector<DatabricksInsertColumn>
DatabricksInsert::GetInsertColumns(DatabricksTableEntry &table,
                                   const physical_index_vector_t<idx_t> &column_index_map) {
	table.ThrowIfColumnsCollide();
	auto &table_columns = table.GetColumns();
	vector<DatabricksInsertColumn> result;
	for (idx_t i = 0; i < table_columns.size(); i++) {
		auto source_index = i;
		if (!column_index_map.empty()) {
			source_index = column_index_map[PhysicalIndex(i)];
			if (source_index == DConstants::INVALID_INDEX) {
				continue;
			}
		}
		result.push_back(DatabricksInsertColumn {table_columns[i].name, source_index});
	}
	if (result.empty()) {
		throw NotImplementedException("INSERT into Databricks table \"%s\" must write at least one column", table.name);
	}
	return result;
}

namespace {

struct DatabricksInsertGlobalState : public GlobalSinkState {
	mutex lock;
	atomic<idx_t> rows_sent {0};
	idx_t max_bytes = 12582912;
	bool ready = false;
	bool skip = false;
	bool created = false;
	shared_ptr<DatabricksSession> session;
	string catalog;
	string schema;
	vector<DatabricksInsertColumn> columns;
	string prefix;
};

struct DatabricksInsertLocalState : public LocalSinkState {
	string values;
	idx_t rows = 0;
};

static idx_t InsertLimit(ClientContext &context) {
	auto limit = DatabricksSettingIndex(context, "dbx_insert_max_statement_bytes", 12582912);
	if (limit == 0 || limit > INSERT_HARD_CAP) {
		limit = INSERT_HARD_CAP;
	}
	return limit;
}

static void PrepareTarget(const DatabricksInsert &op, ClientContext &context, DatabricksInsertGlobalState &gstate) {
	lock_guard<mutex> guard(gstate.lock);
	if (gstate.ready) {
		return;
	}
	gstate.ready = true;
	gstate.max_bytes = InsertLimit(context);
	auto &catalog = DatabricksCatalog::GetAttachedDatabase(context, op.catalog_name, "INSERT");
	gstate.session = catalog.GetSession();
	gstate.catalog = catalog.GetConfig().catalog;
	gstate.schema = op.schema_name;
	if (!op.create_info) {
		gstate.columns = op.columns;
		gstate.prefix = op.insert_prefix;
		return;
	}
	auto &info = *op.create_info;
	if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		catalog.InvalidateTables(op.schema_name);
		auto transaction = catalog.GetCatalogTransaction(context);
		auto schema = catalog.LookupSchema(transaction, EntryLookupInfo(CatalogType::SCHEMA_ENTRY, op.schema_name),
		                                   OnEntryNotFound::RETURN_NULL);
		if (schema && schema->LookupEntry(transaction, EntryLookupInfo(CatalogType::TABLE_ENTRY, info.table))) {
			gstate.skip = true;
			return;
		}
	}
	DatabricksDdl::CreateTable(context, catalog, op.schema_name, info);
	gstate.created = true;
	idx_t source = 0;
	for (auto &column : info.columns.Logical()) {
		gstate.columns.push_back(DatabricksInsertColumn {column.Name(), source++});
	}
	vector<string> names;
	for (auto &column : gstate.columns) {
		names.push_back(DatabricksQuoteIdentifier(column.name));
	}
	gstate.prefix = "INSERT INTO " + DatabricksQualifiedName(gstate.catalog, op.schema_name, info.table) + " (" +
	                StringUtil::Join(names, ", ") + ") VALUES ";
}

static void Flush(const DatabricksInsert &op, ClientContext &context, DatabricksInsertGlobalState &gstate,
                  DatabricksInsertLocalState &local) {
	if (local.rows == 0) {
		return;
	}
	auto sql = gstate.prefix + local.values;
	auto sent = local.rows;
	local.values.clear();
	local.rows = 0;
	auto &catalog = DatabricksCatalog::GetAttachedDatabase(context, op.catalog_name, "INSERT");
	try {
		catalog.ExecuteWrite(context, sql, gstate.schema);
	} catch (std::exception &ex) {
		if (gstate.created) {
			try {
				DropInfo drop;
				drop.type = CatalogType::TABLE_ENTRY;
				drop.schema = op.schema_name;
				drop.name = op.table_name;
				drop.if_not_found = OnEntryNotFound::RETURN_NULL;
				DatabricksDdl::DropTable(context, catalog, drop);
			} catch (std::exception &drop_error) {
				DUCKDB_LOG_WARNING(context,
				                   "Databricks CREATE TABLE AS failed and the new table could not be dropped: %s",
				                   drop_error.what());
			}
		}
		throw;
	}
	gstate.rows_sent += sent;
}

static string RenderRow(DataChunk &chunk, idx_t row, const vector<DatabricksInsertColumn> &columns) {
	vector<string> values;
	for (auto &column : columns) {
		values.push_back(DatabricksLiteral::Render(chunk.data[column.source_index].GetValue(row)));
	}
	return "(" + StringUtil::Join(values, ", ") + ")";
}

} // namespace

unique_ptr<GlobalSinkState> DatabricksInsert::GetGlobalSinkState(ClientContext &context) const {
	(void)context;
	return make_uniq<DatabricksInsertGlobalState>();
}

unique_ptr<LocalSinkState> DatabricksInsert::GetLocalSinkState(ExecutionContext &context) const {
	(void)context;
	return make_uniq<DatabricksInsertLocalState>();
}

SinkResultType DatabricksInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<DatabricksInsertGlobalState>();
	auto &local = input.local_state.Cast<DatabricksInsertLocalState>();
	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	PrepareTarget(*this, context.client, gstate);
	if (gstate.skip) {
		return SinkResultType::FINISHED;
	}
	for (idx_t row = 0; row < chunk.size(); row++) {
		auto rendered = RenderRow(chunk, row, gstate.columns);
		auto separator = local.rows == 0 ? idx_t(0) : idx_t(2);
		auto next_size = gstate.prefix.size() + local.values.size() + separator + rendered.size();
		if (next_size > gstate.max_bytes) {
			if (local.rows > 0) {
				Flush(*this, context.client, gstate, local);
			}
			if (gstate.prefix.size() + rendered.size() > gstate.max_bytes) {
				throw InvalidInputException(
				    "A single Databricks INSERT row is larger than dbx_insert_max_statement_bytes (%llu)",
				    gstate.max_bytes);
			}
		}
		if (local.rows > 0) {
			local.values += ", ";
		}
		local.values += rendered;
		local.rows++;
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType DatabricksInsert::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<DatabricksInsertGlobalState>();
	auto &local = input.local_state.Cast<DatabricksInsertLocalState>();
	if (local.rows > 0) {
		Flush(*this, context.client, gstate, local);
	}
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType DatabricksInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
	(void)pipeline;
	(void)event;
	auto &gstate = input.global_state.Cast<DatabricksInsertGlobalState>();
	if (create_info && !gstate.ready) {
		PrepareTarget(*this, context, gstate);
	}
	return SinkFinalizeType::READY;
}

SourceResultType DatabricksInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSourceInput &input) const {
	(void)context;
	(void)input;
	auto &gstate = sink_state->Cast<DatabricksInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.rows_sent.load())));
	return SourceResultType::FINISHED;
}

string DatabricksInsert::GetName() const {
	return create_info ? "DATABRICKS_CREATE_TABLE_AS" : "DATABRICKS_INSERT";
}

InsertionOrderPreservingMap<string> DatabricksInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table"] = schema_name + "." + table_name;
	return result;
}

} // namespace duckdb
