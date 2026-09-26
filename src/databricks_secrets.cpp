#include "databricks_secrets.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {

namespace {

const vector<string> SECRET_KEYS = {"host", "warehouse_id", "token", "client_id", "client_secret", "catalog"};

void SetOption(DatabricksConfig &config, const string &key, const string &value) {
	if (key == "host") {
		config.SetHost(value);
	} else if (key == "warehouse_id") {
		config.warehouse_id = value;
	} else if (key == "token") {
		config.token = value;
	} else if (key == "client_id") {
		config.client_id = value;
	} else if (key == "client_secret") {
		config.client_secret = value;
	} else if (key == "catalog") {
		config.catalog = value;
	} else {
		throw InvalidInputException("Unrecognized Databricks secret option \"%s\"", key);
	}
}

} // namespace

SecretType DatabricksSecrets::CreateType() {
	SecretType secret_type;
	secret_type.name = TYPE_NAME;
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";
	return secret_type;
}

unique_ptr<BaseSecret> DatabricksSecrets::CreateFunction(ClientContext &context, CreateSecretInput &input) {
	(void)context;
	vector<string> prefix_paths;
	auto result = make_uniq<KeyValueSecret>(prefix_paths, TYPE_NAME, "config", input.name);
	DatabricksConfig validation;
	for (auto &named_param : input.options) {
		auto key = StringUtil::Lower(named_param.first);
		auto value = named_param.second.ToString();
		SetOption(validation, key, value);
		result->secret_map[key] = Value(value);
	}
	validation.ValidateAuth();
	result->redact_keys = {"token", "client_secret"};
	return std::move(result);
}

void DatabricksSecrets::SetSecretParameters(CreateSecretFunction &function) {
	for (auto &name : SECRET_KEYS) {
		function.named_parameters[name] = LogicalType::VARCHAR;
	}
}

unique_ptr<SecretEntry> DatabricksSecrets::GetSecretEntry(ClientContext &context, const string &secret_name) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	auto name = secret_name.empty() ? string(DEFAULT_SECRET_NAME) : secret_name;
	auto entry = secret_manager.GetSecretByName(transaction, name);
	if (!entry) {
		if (!secret_name.empty()) {
			throw BinderException("Secret with name \"%s\" not found", secret_name);
		}
		return nullptr;
	}
	if (entry->secret->GetType() != TYPE_NAME) {
		throw BinderException("Secret \"%s\" is not a Databricks secret (it has type \"%s\")", name,
		                      entry->secret->GetType());
	}
	return entry;
}

void DatabricksSecrets::ApplySecret(const SecretEntry &entry, DatabricksConfig &config) {
	auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*entry.secret);
	for (auto &item : kv_secret.secret_map) {
		SetOption(config, item.first, item.second.ToString());
	}
}

} // namespace duckdb
