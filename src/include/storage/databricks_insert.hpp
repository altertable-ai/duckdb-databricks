#pragma once

#include "duckdb/common/index_vector.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {
class DatabricksCatalog;
class DatabricksTableEntry;
struct CreateTableInfo;

struct DatabricksInsertColumn {
	string name;
	idx_t source_index;
};

class DatabricksInsert : public PhysicalOperator {
public:
	DatabricksInsert(PhysicalPlan &physical_plan, LogicalOperator &op, DatabricksTableEntry &table,
	                 vector<DatabricksInsertColumn> columns);
	DatabricksInsert(PhysicalPlan &physical_plan, LogicalOperator &op, DatabricksCatalog &catalog, const string &schema,
	                 unique_ptr<CreateTableInfo> create_info);

	static vector<DatabricksInsertColumn> GetInsertColumns(DatabricksTableEntry &table,
	                                                       const physical_index_vector_t<idx_t> &column_index_map);

	string catalog_name;
	string schema_name;
	string table_name;
	vector<DatabricksInsertColumn> columns;
	string insert_prefix;
	unique_ptr<CreateTableInfo> create_info;

public:
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
