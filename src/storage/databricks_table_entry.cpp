#include "storage/databricks_table_entry.hpp"

#include "databricks_scanner.hpp"
#include "databricks_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "storage/databricks_catalog.hpp"
#include "storage/databricks_schema_entry.hpp"

namespace duckdb {

DatabricksTableEntry::DatabricksTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                           vector<DatabricksColumn> columns_p, string table_type_p,
                                           string column_collision_p)
    : TableCatalogEntry(catalog, schema, info), column_list(std::move(info.columns)), columns(std::move(columns_p)),
      table_type(std::move(table_type_p)), column_collision(std::move(column_collision_p)) {
}

const Identifier DatabricksTableEntry::USER_ASSIGNMENT_ALIAS("databricks_user_assignment");

void DatabricksTableEntry::ThrowIfColumnsCollide() const {
	if (column_collision.empty()) {
		return;
	}
	throw InvalidInputException("Databricks table \"%s\".\"%s\" has columns whose names differ only in case (%s), "
	                            "which DuckDB cannot tell apart; read the table with databricks_query()",
	                            schema.name.GetIdentifierName(), name.GetIdentifierName(), column_collision);
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
	result->schema_lifetime = dbx_catalog.GetSchemaEntryOwner(schema.name.GetIdentifierName());
	result->table_lifetime = schema.Cast<DatabricksSchemaEntry>().GetTableOwner(name.GetIdentifierName());
	result->table_entry = result->table_lifetime ? &result->table_lifetime->Cast<TableCatalogEntry>() : this;
	result->catalog = dbx_catalog.GetConfig().catalog;
	result->schema = schema.name.GetIdentifierName();
	result->table = name.GetIdentifierName();
	result->columns = columns;
	result->filter_pushdown = DatabricksSettingBool(context, "dbx_filter_pushdown", true);
	bind_data = std::move(result);
	return DatabricksScanFunction();
}

unique_ptr<Expression> DatabricksTableEntry::GetDefaultExpressionForColumn(ClientContext &context,
                                                                           const LogicalType &input_type,
                                                                           const LogicalType &result_type,
                                                                           ColumnBinding binding,
                                                                           const Expression &constant_value) const {
	auto expr =
	    TableCatalogEntry::GetDefaultExpressionForColumn(context, input_type, result_type, binding, constant_value);
	expr->SetAlias(USER_ASSIGNMENT_ALIAS);
	return expr;
}

TableStorageInfo DatabricksTableEntry::GetStorageInfo(ClientContext &context) {
	(void)context;
	return TableStorageInfo();
}

} // namespace duckdb
