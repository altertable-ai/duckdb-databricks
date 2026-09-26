#pragma once

#include "storage/databricks_catalog_set.hpp"

namespace duckdb {
class SchemaCatalogEntry;

class DatabricksTableSet : public DatabricksCatalogSet {
public:
	DatabricksTableSet(SchemaCatalogEntry &schema, Catalog &catalog);

protected:
	void LoadEntries(ClientContext &context) override;

private:
	SchemaCatalogEntry &schema;
};

} // namespace duckdb
