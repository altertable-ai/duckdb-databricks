#include "databricks_type_mapping_function.hpp"

#include "databricks_types.hpp"

namespace duckdb {

static void TypeMappingFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	(void)state;
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t type_text) {
		auto mapped = DatabricksTypes::ToDuckDBName(type_text.GetString());
		return StringVector::AddString(result, mapped);
	});
}

ScalarFunction DatabricksTypeMappingFunction() {
	return ScalarFunction("databricks_type_mapping", {LogicalType::VARCHAR}, LogicalType::VARCHAR, TypeMappingFunction);
}

} // namespace duckdb
