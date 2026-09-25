#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {

struct DatabricksLiteral {
	//! A Databricks SQL literal for value. NULL is CAST(NULL AS <type>).
	static string Render(const Value &value);
	//! Databricks type name for a DuckDB type, used by DDL and typed NULLs.
	static string TypeName(const LogicalType &type);
};

} // namespace duckdb
