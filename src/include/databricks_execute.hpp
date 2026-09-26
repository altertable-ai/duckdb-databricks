#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

class DatabricksExecuteFunction : public TableFunction {
public:
	DatabricksExecuteFunction();
};

} // namespace duckdb
