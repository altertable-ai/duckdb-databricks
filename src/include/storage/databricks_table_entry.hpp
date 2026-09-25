#pragma once

#include "databricks_types.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

class DatabricksTableEntry : public TableCatalogEntry {
public:
	DatabricksTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                     vector<DatabricksColumn> columns, string table_type, string column_collision);

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	const vector<DatabricksColumn> &GetColumns() const {
		return columns;
	}
	const string &GetTableType() const {
		return table_type;
	}
	void ThrowIfColumnsCollide() const;

private:
	vector<DatabricksColumn> columns;
	string table_type;
	string column_collision;
};

} // namespace duckdb
