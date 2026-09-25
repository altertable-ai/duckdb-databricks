#include "storage/databricks_catalog_set.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "storage/databricks_catalog.hpp"

namespace duckdb {

DatabricksCatalogSet::DatabricksCatalogSet(Catalog &catalog) : catalog(catalog) {
}

void DatabricksCatalogSet::TryLoadEntries(ClientContext &context) {
	if (context.transaction.HasActiveTransaction()) {
		Transaction::Get(context, catalog.GetAttached());
	}
	lock_guard<mutex> load_guard(load_lock);
	if (is_loaded) {
		return;
	}
	try {
		LoadEntries(context);
	} catch (...) {
		lock_guard<mutex> guard(entry_lock);
		entries.clear();
		ordered_entries.clear();
		throw;
	}
	is_loaded = true;
}

optional_ptr<CatalogEntry> DatabricksCatalogSet::GetEntry(ClientContext &context, const string &name) {
	TryLoadEntries(context);
	lock_guard<mutex> guard(entry_lock);
	auto exact = entries.find(name);
	if (exact != entries.end()) {
		return exact->second.get();
	}
	for (auto &entry : ordered_entries) {
		if (StringUtil::CIEquals(entry->name, name)) {
			return entry.get();
		}
	}
	return nullptr;
}

shared_ptr<CatalogEntry> DatabricksCatalogSet::GetEntryOwner(const string &name) {
	lock_guard<mutex> guard(entry_lock);
	auto entry = entries.find(name);
	if (entry == entries.end()) {
		return nullptr;
	}
	return entry->second;
}

void DatabricksCatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	TryLoadEntries(context);
	vector<shared_ptr<CatalogEntry>> snapshot;
	{
		lock_guard<mutex> guard(entry_lock);
		snapshot = ordered_entries;
	}
	for (auto &entry : snapshot) {
		callback(*entry);
	}
}

void DatabricksCatalogSet::ClearEntries() {
	vector<shared_ptr<CatalogEntry>> cleared;
	{
		lock_guard<mutex> load_guard(load_lock);
		lock_guard<mutex> guard(entry_lock);
		cleared = std::move(ordered_entries);
		ordered_entries.clear();
		entries.clear();
		is_loaded = false;
	}
	catalog.Cast<DatabricksCatalog>().RetireEntries(std::move(cleared));
}

void DatabricksCatalogSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	shared_ptr<CatalogEntry> shared_entry(std::move(entry));
	lock_guard<mutex> guard(entry_lock);
	entries[shared_entry->name] = shared_entry;
	ordered_entries.push_back(std::move(shared_entry));
}

} // namespace duckdb
