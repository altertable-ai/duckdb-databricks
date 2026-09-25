#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {

struct DatabricksColumn {
	string name;
	string type_text;
	LogicalType type;
	bool nullable = true;
	string default_sql;
	string comment;
};

struct DatabricksTypes {
	//! Databricks type text (full_data_type or a manifest type_text) to a DuckDB type.
	//! GEOMETRY, GEOGRAPHY, OBJECT, and anything unrecognised become VARCHAR. VOID becomes INTEGER.
	static LogicalType Parse(const string &type_text);
	static string ToDuckDBName(const string &type_text) {
		return Parse(type_text).ToString();
	}
};

} // namespace duckdb
