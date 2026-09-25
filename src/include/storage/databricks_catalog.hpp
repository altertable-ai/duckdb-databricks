#pragma once

#include "databricks_config.hpp"
#include "databricks_statement.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "storage/databricks_schema_set.hpp"

namespace duckdb {

struct DatabricksAttachOptions {
	//! Empty means every schema except information_schema, and the default schema name is "default"
	string schema;
};

class DatabricksCatalog : public Catalog {
public:
	DatabricksCatalog(AttachedDatabase &db, DatabricksConfig config, DatabricksAttachOptions options,
	                  ClientContext &context);
	~DatabricksCatalog() override;

	static constexpr const char *CATALOG_TYPE = "databricks";

	static DatabricksCatalog &GetAttachedDatabase(ClientContext &context, const string &database_name,
	                                              const string &function_name);

	const DatabricksConfig &GetConfig() const {
		return config;
	}
	const DatabricksAttachOptions &GetAttachOptions() const {
		return options;
	}
	shared_ptr<DatabricksSession> GetSession() {
		return session;
	}
	void ThrowIfReadOnly() const;
	void ClearCache();
	void RetireEntries(vector<shared_ptr<CatalogEntry>> entries);
	shared_ptr<CatalogEntry> GetSchemaEntryOwner(const string &name);

	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return CATALOG_TYPE;
	}
	string GetDefaultSchema() const override {
		return options.schema.empty() ? "default" : options.schema;
	}
	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;
	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner, LogicalMergeInto &op,
	                                PhysicalOperator &plan) override;
	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;
	unique_ptr<LogicalOperator> BindAlterAddIndex(Binder &binder, TableCatalogEntry &table_entry,
	                                              unique_ptr<LogicalOperator> plan,
	                                              unique_ptr<CreateIndexInfo> create_info,
	                                              unique_ptr<AlterTableInfo> alter_info) override;
	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override {
		return false;
	}
	string GetDBPath() override {
		return config.BaseUrl() + "/" + config.catalog;
	}

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

	DatabricksConfig config;
	DatabricksAttachOptions options;
	shared_ptr<DatabricksSession> session;
	DatabricksSchemaSet schemas;
};

} // namespace duckdb
