#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

class DatabricksClearCacheFunction : public TableFunction {
public:
	DatabricksClearCacheFunction();
	static void ClearDatabricksCaches(ClientContext &context);
};

} // namespace duckdb
