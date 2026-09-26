#include "databricks_arrow.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/table/arrow.hpp"

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

namespace duckdb {

vector<DatabricksArrowBatch> DatabricksDecodeArrow(ClientContext &context, const string &bytes) {
	if (bytes.empty()) {
		return {};
	}
	auto buffer = arrow::Buffer::FromString(bytes);
	auto input = std::make_shared<arrow::io::BufferReader>(buffer);
	auto opened = arrow::ipc::RecordBatchStreamReader::Open(input);
	if (!opened.ok()) {
		throw IOException("Databricks transport error during chunk download: Arrow stream could not be opened: %s",
		                  opened.status().ToString());
	}
	auto reader = opened.ValueOrDie();
	vector<DatabricksArrowBatch> batches;
	while (true) {
		std::shared_ptr<arrow::RecordBatch> batch;
		auto status = reader->ReadNext(&batch);
		if (!status.ok()) {
			throw IOException("Databricks transport error during chunk download: Arrow stream could not be read: %s",
			                  status.ToString());
		}
		if (!batch) {
			break;
		}
		if (batch->num_rows() == 0) {
			continue;
		}
		DatabricksArrowBatch decoded;
		decoded.array = make_uniq<ArrowArrayWrapper>();
		ArrowSchemaWrapper schema;
		auto exported = arrow::ExportRecordBatch(*batch, &decoded.array->arrow_array, &schema.arrow_schema);
		if (!exported.ok()) {
			throw IOException("Databricks transport error during chunk download: Arrow export failed: %s",
			                  exported.ToString());
		}
		ArrowTableFunction::PopulateArrowTableSchema(context, decoded.schema, schema.arrow_schema);
		batches.push_back(std::move(decoded));
	}
	return batches;
}

} // namespace duckdb
