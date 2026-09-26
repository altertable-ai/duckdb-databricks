#include "storage/databricks_table_entry.hpp"

#include "databricks_scanner.hpp"
#include "databricks_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "storage/databricks_catalog.hpp"
#include "storage/databricks_schema_entry.hpp"

namespace duckdb {

DatabricksTableEntry::DatabricksTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                           vector<DatabricksColumn> columns_p, string table_type_p,
                                           string column_collision_p)
    : TableCatalogEntry(catalog, schema, info), columns(std::move(columns_p)), table_type(std::move(table_type_p)),
      column_collision(std::move(column_collision_p)) {
}

void DatabricksTableEntry::ThrowIfColumnsCollide() const {
	if (column_collision.empty()) {
		return;
	}
	throw InvalidInputException("Databricks table \"%s\".\"%s\" has columns whose names differ only in case (%s), "
	                            "which DuckDB cannot tell apart; read the table with databricks_query()",
	                            schema.name, name, column_collision);
}

unique_ptr<BaseStatistics> DatabricksTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	(void)context;
	(void)column_id;
	return nullptr;
}

TableFunction DatabricksTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	ThrowIfColumnsCollide();
	auto result = make_uniq<DatabricksScanBindData>();
	auto &dbx_catalog = catalog.Cast<DatabricksCatalog>();
	result->session = dbx_catalog.GetSession();
	result->schema_lifetime = dbx_catalog.GetSchemaEntryOwner(schema.name);
	result->table_lifetime = schema.Cast<DatabricksSchemaEntry>().GetTableOwner(name);
	result->table_entry = result->table_lifetime ? &result->table_lifetime->Cast<TableCatalogEntry>() : this;
	result->catalog = dbx_catalog.GetConfig().catalog;
	result->schema = schema.name;
	result->table = name;
	result->columns = columns;
	result->filter_pushdown = DatabricksSettingBool(context, "dbx_filter_pushdown", true);
	bind_data = std::move(result);
	return DatabricksScanFunction();
}

TableStorageInfo DatabricksTableEntry::GetStorageInfo(ClientContext &context) {
	(void)context;
	return TableStorageInfo();
}

} // namespace duckdb
