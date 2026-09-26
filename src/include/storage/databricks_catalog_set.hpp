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
	bool IsLoaded();
	//! Drop one cached entry. A set that was never loaded stays unloaded.
	void Erase(const string &name);

protected:
	virtual void LoadEntries(ClientContext &context) = 0;
	void CreateEntry(unique_ptr<CatalogEntry> entry);
	//! Insert one entry and mark the set loaded, without a warehouse round trip.
	void SeedEntry(unique_ptr<CatalogEntry> entry);

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
