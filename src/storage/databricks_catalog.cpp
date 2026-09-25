#include "storage/databricks_catalog.hpp"

#include "databricks_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/storage/database_size.hpp"
#include "storage/databricks_schema_entry.hpp"
#include "storage/databricks_transaction.hpp"

namespace duckdb {

DatabricksCatalog::DatabricksCatalog(AttachedDatabase &db, DatabricksConfig config_p, DatabricksAttachOptions options_p,
                                     ClientContext &context)
    : Catalog(db), config(std::move(config_p)), options(std::move(options_p)),
      session(make_shared_ptr<DatabricksSession>(config)), schemas(*this) {
	config.ValidateAttach();
	auto catalog_sql = "SELECT catalog_name FROM system.information_schema.catalogs WHERE catalog_name = :catalog";
	auto catalogs = session->Execute(context, DatabricksStatementMode::SMALL, catalog_sql, config.catalog,
	                                 GetDefaultSchema(), {{"catalog", config.catalog}});
	if (catalogs.rows.empty()) {
		throw BinderException("Databricks catalog \"%s\" was not found", config.catalog);
	}
	if (options.schema.empty()) {
		return;
	}
	auto schema_sql =
	    "SELECT schema_name FROM " + DatabricksQuoteIdentifier(config.catalog) + ".information_schema.schemata";
	auto schemata =
	    session->Execute(context, DatabricksStatementMode::SMALL, schema_sql, config.catalog, GetDefaultSchema(), {});
	string ci_match;
	for (idx_t row = 0; row < schemata.rows.size(); row++) {
		auto name = session->Cell(schemata, row, "schema_name");
		if (!name || StringUtil::CIEquals(*name, "information_schema")) {
			continue;
		}
		if (*name == options.schema) {
			return;
		}
		if (ci_match.empty() && StringUtil::CIEquals(*name, options.schema)) {
			ci_match = *name;
		}
	}
	if (!ci_match.empty()) {
		options.schema = ci_match;
		return;
	}
	throw BinderException("Databricks schema \"%s\" was not found in catalog \"%s\"", options.schema, config.catalog);
}

DatabricksCatalog::~DatabricksCatalog() = default;

DatabricksCatalog &DatabricksCatalog::GetAttachedDatabase(ClientContext &context, const string &database_name,
                                                          const string &function_name) {
	auto database = DatabaseManager::Get(context).GetDatabase(context, database_name);
	if (!database) {
		throw BinderException("Failed to find attached database \"%s\" referenced in %s", database_name, function_name);
	}
	auto &catalog = database->GetCatalog();
	if (catalog.GetCatalogType() != CATALOG_TYPE) {
		throw BinderException("Attached database \"%s\" is not a Databricks database", database_name);
	}
	return catalog.Cast<DatabricksCatalog>();
}

void DatabricksCatalog::Initialize(bool load_builtin) {
	(void)load_builtin;
}

optional_ptr<CatalogEntry> DatabricksCatalog::CreateSchema(CatalogTransaction, CreateSchemaInfo &) {
	throw NotImplementedException("CREATE SCHEMA is not supported for Databricks databases; use databricks_execute()");
}

void DatabricksCatalog::DropSchema(ClientContext &, DropInfo &) {
	throw NotImplementedException("DROP SCHEMA is not supported for Databricks databases; use databricks_execute()");
}

void DatabricksCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	schemas.Scan(context, [&](CatalogEntry &entry) { callback(entry.Cast<SchemaCatalogEntry>()); });
}

optional_ptr<SchemaCatalogEntry> DatabricksCatalog::LookupSchema(CatalogTransaction transaction,
                                                                 const EntryLookupInfo &schema_lookup,
                                                                 OnEntryNotFound if_not_found) {
	auto entry = schemas.GetEntry(transaction.GetContext(), schema_lookup.GetEntryName());
	if (!entry) {
		if (if_not_found == OnEntryNotFound::RETURN_NULL) {
			return nullptr;
		}
		throw BinderException("Databricks schema \"%s\" was not found in catalog \"%s\"", schema_lookup.GetEntryName(),
		                      config.catalog);
	}
	return &entry->Cast<SchemaCatalogEntry>();
}

void DatabricksCatalog::ThrowIfReadOnly() const {
	if (GetAttached().IsReadOnly()) {
		throw InvalidInputException("Cannot write to a read-only Databricks database");
	}
}

void DatabricksCatalog::ClearCache() {
	schemas.ClearEntries();
}

void DatabricksCatalog::RetireEntries(vector<shared_ptr<CatalogEntry>> entries) {
	GetAttached().GetTransactionManager().Cast<DatabricksTransactionManager>().RetireEntries(std::move(entries));
}

shared_ptr<CatalogEntry> DatabricksCatalog::GetSchemaEntryOwner(const string &name) {
	return schemas.GetEntryOwner(name);
}

[[noreturn]] static void ThrowWriteNotImplemented(const string &statement) {
	throw NotImplementedException("%s is not supported for Databricks tables; use databricks_execute()", statement);
}

PhysicalOperator &DatabricksCatalog::PlanCreateTableAs(ClientContext &, PhysicalPlanGenerator &, LogicalCreateTable &,
                                                       PhysicalOperator &) {
	ThrowWriteNotImplemented("CREATE TABLE AS");
}
PhysicalOperator &DatabricksCatalog::PlanInsert(ClientContext &, PhysicalPlanGenerator &, LogicalInsert &,
                                                optional_ptr<PhysicalOperator>) {
	ThrowWriteNotImplemented("INSERT");
}
PhysicalOperator &DatabricksCatalog::PlanDelete(ClientContext &, PhysicalPlanGenerator &, LogicalDelete &) {
	ThrowWriteNotImplemented("DELETE");
}
PhysicalOperator &DatabricksCatalog::PlanDelete(ClientContext &, PhysicalPlanGenerator &, LogicalDelete &,
                                                PhysicalOperator &) {
	ThrowWriteNotImplemented("DELETE");
}
PhysicalOperator &DatabricksCatalog::PlanUpdate(ClientContext &, PhysicalPlanGenerator &, LogicalUpdate &) {
	ThrowWriteNotImplemented("UPDATE");
}
PhysicalOperator &DatabricksCatalog::PlanUpdate(ClientContext &, PhysicalPlanGenerator &, LogicalUpdate &,
                                                PhysicalOperator &) {
	ThrowWriteNotImplemented("UPDATE");
}
PhysicalOperator &DatabricksCatalog::PlanMergeInto(ClientContext &, PhysicalPlanGenerator &, LogicalMergeInto &,
                                                   PhysicalOperator &) {
	ThrowWriteNotImplemented("MERGE");
}

unique_ptr<LogicalOperator> DatabricksCatalog::BindCreateIndex(Binder &, CreateStatement &, TableCatalogEntry &,
                                                               unique_ptr<LogicalOperator>) {
	ThrowWriteNotImplemented("Indexes");
}
unique_ptr<LogicalOperator> DatabricksCatalog::BindAlterAddIndex(Binder &, TableCatalogEntry &,
                                                                 unique_ptr<LogicalOperator>,
                                                                 unique_ptr<CreateIndexInfo>,
                                                                 unique_ptr<AlterTableInfo>) {
	ThrowWriteNotImplemented("Indexes");
}

DatabaseSize DatabricksCatalog::GetDatabaseSize(ClientContext &context) {
	(void)context;
	return DatabaseSize();
}

} // namespace duckdb
