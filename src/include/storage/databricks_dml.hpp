#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {
class LogicalDelete;
class LogicalUpdate;

class DatabricksDml : public PhysicalOperator {
public:
	DatabricksDml(PhysicalPlan &physical_plan, LogicalOperator &op, string catalog_name, string schema, string sql);

	static string UpdateSql(LogicalUpdate &op);
	static string DeleteSql(LogicalDelete &op);

	string catalog_name;
	string schema_name;
	string sql;

public:
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
