#define DUCKDB_EXTENSION_MAIN

#include "databricks_extension.hpp"

#include "databricks_execute.hpp"
#include "databricks_query.hpp"
#include "databricks_secrets.hpp"
#include "databricks_type_mapping_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "storage/databricks_clear_cache.hpp"
#include "storage/databricks_optimizer.hpp"
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

	auto reject_insert_bytes = [](ClientContext &context, SetScope scope, Value &parameter) {
		(void)context;
		(void)scope;
		auto bytes = parameter.DefaultCastAs(LogicalType::UBIGINT);
		auto value = UBigIntValue::Get(bytes);
		if (value == 0 || value > 16 * 1024 * 1024) {
			throw InvalidInputException("dbx_insert_max_statement_bytes must be between 1 and 16777216");
		}
		parameter = bytes;
	};
	config.AddExtensionOption("dbx_filter_pushdown", "Push filters into Databricks SQL", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(true));
	config.AddExtensionOption("dbx_order_pushdown", "Push ORDER BY ... LIMIT into Databricks SQL", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(true));
	config.AddExtensionOption("dbx_insert_max_statement_bytes",
	                          "Maximum SQL text of one INSERT statement, at most 16 MiB", LogicalType::UBIGINT,
	                          Value::UBIGINT(12582912), reject_insert_bytes);
	config.AddExtensionOption("dbx_statement_timeout_ms",
	                          "Cancel a statement still running after this many milliseconds. 0 waits",
	                          LogicalType::UBIGINT, Value::UBIGINT(0));
	config.AddExtensionOption("dbx_http_timeout_ms", "Per-request HTTP timeout in milliseconds", LogicalType::UBIGINT,
	                          Value::UBIGINT(60000));
	config.AddExtensionOption("dbx_http_retries", "Retries for HTTP 429, 503, and connection errors",
	                          LogicalType::UBIGINT, Value::UBIGINT(5));
	config.AddExtensionOption("dbx_ca_cert", "PEM bundle to trust, or 'system'", LogicalType::VARCHAR, Value("system"));
	config.AddExtensionOption("dbx_debug_show_queries", "Print each statement text and id", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(false));

	OptimizerExtension optimizer;
	optimizer.optimize_function = DatabricksOptimizer::Optimize;
	OptimizerExtension::Register(config, std::move(optimizer));
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
