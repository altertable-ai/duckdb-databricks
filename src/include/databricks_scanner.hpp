#pragma once

#include "databricks_statement.hpp"
#include "databricks_types.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {
class TableCatalogEntry;

struct DatabricksScanBindData : public TableFunctionData {
	shared_ptr<DatabricksSession> session;
	//! Databricks catalog, not the DuckDB database name
	string catalog;
	string schema;
	string table;
	//! Set for databricks_query(); the statement has already run
	string query;
	bool executed = false;
	DatabricksStatementResult result;
	vector<DatabricksColumn> columns;
	TableCatalogEntry *table_entry = nullptr;
	//! Keeps the table entry and its schema alive for prepared statements.
	shared_ptr<CatalogEntry> table_lifetime;
	shared_ptr<CatalogEntry> schema_lifetime;
	bool filter_pushdown = false;
	//! Predicate captured by pushdown_complex_filter, without a WHERE keyword.
	string extra_filter;
	//! Set by the optimizer. Includes the leading space.
	string order_by_clause;
	string limit_clause;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other_p) const override;
};

class DatabricksScanFunction : public TableFunction {
public:
	DatabricksScanFunction();

	static string BuildQuery(const DatabricksScanBindData &bind_data, const vector<column_t> &column_ids,
	                         optional_ptr<TableFilterSet> filters);
	static void SetScanCallbacks(TableFunction &function);
};

} // namespace duckdb
