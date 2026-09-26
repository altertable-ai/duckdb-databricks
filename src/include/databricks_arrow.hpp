#pragma once

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct DatabricksArrowBatch {
	unique_ptr<ArrowArrayWrapper> array;
	ArrowTableSchema schema;
};

//! Decode one Arrow IPC stream into record batches and DuckDB arrow type metadata.
vector<DatabricksArrowBatch> DatabricksDecodeArrow(ClientContext &context, const string &bytes);

} // namespace duckdb
