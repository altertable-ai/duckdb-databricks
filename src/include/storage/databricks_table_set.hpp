#pragma once

#include "storage/databricks_catalog_set.hpp"

namespace duckdb {
class SchemaCatalogEntry;
struct CreateTableInfo;

class DatabricksTableSet : public DatabricksCatalogSet {
public:
	DatabricksTableSet(SchemaCatalogEntry &schema, Catalog &catalog);
	//! Cache a table this connection just created. No-op when the set is not loaded yet.
	bool Remember(CreateTableInfo &info);
	void Forget(const string &name);

protected:
	void LoadEntries(ClientContext &context) override;

private:
	SchemaCatalogEntry &schema;
};

} // namespace duckdb
