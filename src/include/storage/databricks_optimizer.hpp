#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

//! Moves LIMIT / OFFSET and ORDER BY ... LIMIT that sit directly on a Databricks scan into the statement.
class DatabricksOptimizer {
public:
	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb
