#include "databricks_execute.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/settings.hpp"
#include "storage/databricks_catalog.hpp"

namespace duckdb {

struct DatabricksExecuteData : public TableFunctionData {
	bool finished = false;
	string database_name;
	string sql;
};

static unique_ptr<FunctionData> ExecuteBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	if (!Settings::Get<EnableExternalAccessSetting>(context)) {
		throw PermissionException("Writing to Databricks is disabled through configuration");
	}
	for (auto &value : input.inputs) {
		if (value.IsNull()) {
			throw BinderException("Parameters to databricks_execute cannot be NULL");
		}
	}
	auto result = make_uniq<DatabricksExecuteData>();
	result->database_name = StringValue::Get(input.inputs[0]);
	result->sql = StringValue::Get(input.inputs[1]);
	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(result);
}

static void ExecuteFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->CastNoConst<DatabricksExecuteData>();
	if (data.finished) {
		return;
	}
	auto &catalog = DatabricksCatalog::GetAttachedDatabase(context, data.database_name, "databricks_execute");
	catalog.ExecuteWrite(context, data.sql, catalog.GetDefaultSchema());
	catalog.ClearCache();
	data.finished = true;
	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BOOLEAN(true));
}

DatabricksExecuteFunction::DatabricksExecuteFunction()
    : TableFunction("databricks_execute", {LogicalType::VARCHAR, LogicalType::VARCHAR}, ExecuteFunction, ExecuteBind) {
}

} // namespace duckdb
