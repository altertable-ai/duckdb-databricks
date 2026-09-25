#pragma once

#include "databricks_types.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {
class LogicalGet;

class DatabricksExpressions {
public:
	//! True when a filter on this column can be sent to Databricks and not re-checked locally.
	static bool SupportsFilterPushdown(const DatabricksColumn &column);
	//! WHERE body. Empty when no pushed filter restricts rows.
	//! filters is keyed by position in column_ids. Use the static filter set, not dynamic join hints.
	static string TransformFilters(const vector<column_t> &column_ids, optional_ptr<TableFilterSet> filters,
	                               const vector<DatabricksColumn> &columns);
	//! Empty for optional, dynamic, and bloom filters. Throws when a required filter cannot be translated.
	static string TransformFilter(const string &column, const TableFilter &filter);
	static string TransformConstant(const Value &value);
	//! Translates a filter expression DuckDB would otherwise keep locally (IN, OR, NOT, BETWEEN, prefix LIKE).
	//! Returns false when the expression must stay in DuckDB.
	static bool TryTranslateExpression(const vector<DatabricksColumn> &columns, const LogicalGet &get, Expression &expr,
	                                   string &sql);
};

} // namespace duckdb
