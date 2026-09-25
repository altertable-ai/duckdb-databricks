#include "storage/databricks_storage_extension.hpp"

#include "databricks_secrets.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "storage/databricks_catalog.hpp"
#include "storage/databricks_transaction.hpp"

namespace duckdb {

static unique_ptr<Catalog> DatabricksAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                            AttachedDatabase &db, const string &name, AttachInfo &info,
                                            AttachOptions &attach_options) {
	(void)storage_info;
	(void)name;
	if (!Settings::Get<EnableExternalAccessSetting>(context)) {
		throw PermissionException("Attaching Databricks databases is disabled through configuration");
	}
	string secret_name;
	string warehouse_id;
	DatabricksAttachOptions options;
	for (auto &entry : attach_options.options) {
		auto key = StringUtil::Lower(entry.first);
		if (key == "secret") {
			secret_name = entry.second.ToString();
		} else if (key == "warehouse_id") {
			warehouse_id = entry.second.ToString();
		} else if (key == "schema") {
			options.schema = entry.second.ToString();
			if (options.schema.empty()) {
				throw BinderException("SCHEMA must name a Databricks schema");
			}
		} else {
			throw BinderException("Unrecognized option for Databricks attach: %s", entry.first);
		}
	}
	DatabricksConfig config;
	auto secret_entry = DatabricksSecrets::GetSecretEntry(context, secret_name);
	if (secret_entry) {
		DatabricksSecrets::ApplySecret(*secret_entry, config);
	}
	if (!warehouse_id.empty()) {
		config.warehouse_id = warehouse_id;
	}
	if (!info.path.empty()) {
		config.catalog = info.path;
	}
	return make_uniq<DatabricksCatalog>(db, std::move(config), options, context);
}

static unique_ptr<TransactionManager> DatabricksCreateTransactionManager(optional_ptr<StorageExtensionInfo>,
                                                                         AttachedDatabase &db, Catalog &) {
	return make_uniq<DatabricksTransactionManager>(db);
}

DatabricksStorageExtension::DatabricksStorageExtension() {
	attach = DatabricksAttach;
	create_transaction_manager = DatabricksCreateTransactionManager;
}

} // namespace duckdb
