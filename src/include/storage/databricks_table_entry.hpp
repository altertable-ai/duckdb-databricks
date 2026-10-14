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
	unique_ptr<Expression> GetDefaultExpressionForColumn(ClientContext &context, const LogicalType &input_type,
	                                                     const LogicalType &result_type, ColumnBinding binding,
	                                                     const Expression &constant_value) const override;
	const ColumnList &GetColumns() const override {
		return column_list;
	}

	//! Alias on UPDATE SET values that the user wrote, as opposed to SET col = DEFAULT
	static const Identifier USER_ASSIGNMENT_ALIAS;

	const vector<DatabricksColumn> &GetDatabricksColumns() const {
		return columns;
	}
	const string &GetTableType() const {
		return table_type;
	}
	void ThrowIfColumnsCollide() const;

private:
	ColumnList column_list;
	vector<DatabricksColumn> columns;
	string table_type;
	string column_collision;
};

} // namespace duckdb
