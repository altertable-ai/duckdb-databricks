#include "storage/databricks_table_entry.hpp"

#include "databricks_scanner.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "storage/databricks_catalog.hpp"

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
	(void)context;
	ThrowIfColumnsCollide();
	auto result = make_uniq<DatabricksScanBindData>();
	auto &dbx_catalog = catalog.Cast<DatabricksCatalog>();
	result->session = dbx_catalog.GetSession();
	result->table_entry = this;
	result->lifetime = dbx_catalog.GetSchemaEntryOwner(schema.name);
	result->catalog = dbx_catalog.GetConfig().catalog;
	result->schema = schema.name;
	result->table = name;
	result->columns = columns;
	bind_data = std::move(result);
	return DatabricksScanFunction();
}

TableStorageInfo DatabricksTableEntry::GetStorageInfo(ClientContext &context) {
	(void)context;
	return TableStorageInfo();
}

} // namespace duckdb
