#include "storage/databricks_transaction.hpp"

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

DatabricksTransaction::DatabricksTransaction(TransactionManager &manager, ClientContext &context,
                                             idx_t transaction_id_p)
    : Transaction(manager, context), transaction_id(transaction_id_p) {
}

DatabricksTransaction::~DatabricksTransaction() = default;

DatabricksTransaction &DatabricksTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<DatabricksTransaction>();
}

DatabricksTransactionManager::DatabricksTransactionManager(AttachedDatabase &db) : TransactionManager(db) {
}

DatabricksTransactionManager::~DatabricksTransactionManager() = default;

Transaction &DatabricksTransactionManager::StartTransaction(ClientContext &context) {
	lock_guard<mutex> guard(transaction_lock);
	auto transaction = make_uniq<DatabricksTransaction>(*this, context, next_transaction_id++);
	auto &result = *transaction;
	transactions[result] = std::move(transaction);
	return result;
}

void DatabricksTransactionManager::RetireEntries(vector<shared_ptr<CatalogEntry>> entries) {
	if (entries.empty()) {
		return;
	}
	lock_guard<mutex> guard(transaction_lock);
	retired.push_back(RetiredBatch {next_transaction_id, std::move(entries)});
}

void DatabricksTransactionManager::EndTransaction(Transaction &transaction) {
	unique_ptr<DatabricksTransaction> ended;
	vector<RetiredBatch> released;
	lock_guard<mutex> guard(transaction_lock);
	auto entry = transactions.find(transaction);
	if (entry != transactions.end()) {
		ended = std::move(entry->second);
		transactions.erase(entry);
	}
	auto oldest_active = next_transaction_id;
	for (auto &active : transactions) {
		oldest_active = MinValue(oldest_active, active.second->GetTransactionId());
	}
	idx_t release_count = 0;
	while (release_count < retired.size() && retired[release_count].stamp <= oldest_active) {
		release_count++;
	}
	if (release_count == 0) {
		return;
	}
	for (idx_t i = 0; i < release_count; i++) {
		released.push_back(std::move(retired[i]));
	}
	retired.erase(retired.begin(), retired.begin() + static_cast<std::ptrdiff_t>(release_count));
}

ErrorData DatabricksTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	(void)context;
	EndTransaction(transaction);
	return ErrorData();
}

void DatabricksTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &databricks_transaction = transaction.Cast<DatabricksTransaction>();
	if (databricks_transaction.HasWritten()) {
		auto context = databricks_transaction.context.lock();
		if (context) {
			DUCKDB_LOG_WARNING(*context, "Databricks writes are committed immediately and were not rolled back");
		}
	}
	EndTransaction(transaction);
}

void DatabricksTransactionManager::Checkpoint(ClientContext &context, bool force) {
	(void)context;
	(void)force;
}

} // namespace duckdb
