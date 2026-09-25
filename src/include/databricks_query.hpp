#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

class DatabricksQueryFunction : public TableFunction {
public:
	DatabricksQueryFunction();
};

} // namespace duckdb
