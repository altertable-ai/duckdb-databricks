#include "storage/databricks_clear_cache.hpp"

#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "storage/databricks_catalog.hpp"

namespace duckdb {

struct DatabricksClearCacheData : public TableFunctionData {
	bool finished = false;
};

static unique_ptr<FunctionData> ClearCacheBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	(void)context;
	(void)input;
	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return make_uniq<DatabricksClearCacheData>();
}

void DatabricksClearCacheFunction::ClearDatabricksCaches(ClientContext &context) {
	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &database : databases) {
		auto &catalog = database->GetCatalog();
		if (catalog.GetCatalogType() != DatabricksCatalog::CATALOG_TYPE) {
			continue;
		}
		catalog.Cast<DatabricksCatalog>().ClearCache();
	}
}

static void ClearCacheFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<DatabricksClearCacheData>();
	if (data.finished) {
		return;
	}
	DatabricksClearCacheFunction::ClearDatabricksCaches(context);
	data.finished = true;
	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BOOLEAN(true));
}

DatabricksClearCacheFunction::DatabricksClearCacheFunction()
    : TableFunction("databricks_clear_cache", {}, ClearCacheFunction, ClearCacheBind) {
}

} // namespace duckdb
