#pragma once

#include "storage/databricks_catalog_set.hpp"

namespace duckdb {

class DatabricksSchemaSet : public DatabricksCatalogSet {
public:
	explicit DatabricksSchemaSet(Catalog &catalog);

protected:
	void LoadEntries(ClientContext &context) override;
};

} // namespace duckdb
