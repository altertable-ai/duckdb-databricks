#include "databricks_query.hpp"

#include "databricks_scanner.hpp"
#include "databricks_types.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/settings.hpp"
#include "storage/databricks_catalog.hpp"

namespace duckdb {

static unique_ptr<FunctionData> QueryBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	if (!Settings::Get<EnableExternalAccessSetting>(context)) {
		throw PermissionException("Reading from Databricks is disabled through configuration");
	}
	for (auto &value : input.inputs) {
		if (value.IsNull()) {
			throw BinderException("Parameters to databricks_query cannot be NULL");
		}
	}
	auto database_name = StringValue::Get(input.inputs[0]);
	auto sql = StringValue::Get(input.inputs[1]);
	auto &catalog = DatabricksCatalog::GetAttachedDatabase(context, database_name, "databricks_query");
	auto result = make_uniq<DatabricksScanBindData>();
	result->session = catalog.GetSession();
	result->catalog = catalog.GetConfig().catalog;
	result->schema = catalog.GetDefaultSchema();
	result->query = sql;
	result->result =
	    result->session->Execute(context, DatabricksStatementMode::SCAN, sql, result->catalog, result->schema, {});
	result->executed = true;
	if (result->result.columns.empty()) {
		throw BinderException("Databricks query returned no schema");
	}
	for (auto &column : result->result.columns) {
		auto type_text = column.type_text.empty() ? column.type_name : column.type_text;
		auto type = DatabricksTypes::Parse(type_text);
		names.push_back(column.name);
		return_types.push_back(type);
		DatabricksColumn stored;
		stored.name = column.name;
		stored.type_text = type_text;
		stored.type = type;
		result->columns.push_back(std::move(stored));
	}
	return std::move(result);
}

DatabricksQueryFunction::DatabricksQueryFunction()
    : TableFunction("databricks_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, nullptr, QueryBind) {
	DatabricksScanFunction::SetScanCallbacks(*this);
}

} // namespace duckdb
