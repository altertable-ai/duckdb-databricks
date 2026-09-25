#pragma once

#include "databricks_config.hpp"
#include "duckdb/main/secret/secret.hpp"

namespace duckdb {

class DatabricksSecrets {
public:
	static constexpr const char *TYPE_NAME = "databricks";
	static constexpr const char *DEFAULT_SECRET_NAME = "__default_databricks";

	static SecretType CreateType();
	static unique_ptr<BaseSecret> CreateFunction(ClientContext &context, CreateSecretInput &input);
	static void SetSecretParameters(CreateSecretFunction &function);
	static unique_ptr<SecretEntry> GetSecretEntry(ClientContext &context, const string &secret_name);
	static void ApplySecret(const SecretEntry &entry, DatabricksConfig &config);
};

} // namespace duckdb
