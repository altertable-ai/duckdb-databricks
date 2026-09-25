#define DUCKDB_EXTENSION_MAIN

#include "databricks_extension.hpp"

#include "databricks_execute.hpp"
#include "databricks_query.hpp"
#include "databricks_secrets.hpp"
#include "databricks_type_mapping_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "storage/databricks_clear_cache.hpp"
#include "storage/databricks_storage_extension.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	loader.RegisterFunction(DatabricksTypeMappingFunction());
	loader.RegisterFunction(DatabricksQueryFunction());
	loader.RegisterFunction(DatabricksExecuteFunction());
	loader.RegisterFunction(DatabricksClearCacheFunction());

	loader.RegisterSecretType(DatabricksSecrets::CreateType());
	CreateSecretFunction secret_function = {DatabricksSecrets::TYPE_NAME, "config", DatabricksSecrets::CreateFunction};
	DatabricksSecrets::SetSecretParameters(secret_function);
	loader.RegisterFunction(secret_function);

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	auto storage_extension = make_shared_ptr<DatabricksStorageExtension>();
	StorageExtension::Register(config, "databricks", storage_extension);
}

void DatabricksExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string DatabricksExtension::Name() {
	return "databricks";
}

std::string DatabricksExtension::Version() const {
#ifdef EXT_VERSION_DATABRICKS
	return EXT_VERSION_DATABRICKS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(databricks, loader) {
	duckdb::LoadInternal(loader);
}
}
