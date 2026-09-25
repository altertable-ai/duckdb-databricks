#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"

#include <functional>

namespace duckdb {
class Catalog;
class ClientContext;

class DatabricksCatalogSet {
public:
	explicit DatabricksCatalogSet(Catalog &catalog);
	virtual ~DatabricksCatalogSet() = default;

	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const string &name);
	shared_ptr<CatalogEntry> GetEntryOwner(const string &name);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	void ClearEntries();

protected:
	virtual void LoadEntries(ClientContext &context) = 0;
	void CreateEntry(unique_ptr<CatalogEntry> entry);

	Catalog &catalog;

private:
	void TryLoadEntries(ClientContext &context);

	mutex load_lock;
	mutex entry_lock;
	bool is_loaded = false;
	vector<shared_ptr<CatalogEntry>> ordered_entries;
	unordered_map<string, shared_ptr<CatalogEntry>> entries;
};

} // namespace duckdb
