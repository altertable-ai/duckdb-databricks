#pragma once

#include "storage/databricks_catalog_set.hpp"

namespace duckdb {

class DatabricksSchemaSet : public DatabricksCatalogSet {
public:
	explicit DatabricksSchemaSet(Catalog &catalog);
	void Seed(const string &name, const string &comment);

protected:
	void LoadEntries(ClientContext &context) override;
};

} // namespace duckdb
