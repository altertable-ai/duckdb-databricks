#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {
class CatalogEntry;

class DatabricksTransaction : public Transaction {
public:
	DatabricksTransaction(TransactionManager &manager, ClientContext &context, idx_t transaction_id);
	~DatabricksTransaction() override;

	static DatabricksTransaction &Get(ClientContext &context, Catalog &catalog);

	void MarkWritten() {
		written = true;
	}
	bool HasWritten() const {
		return written;
	}
	idx_t GetTransactionId() const {
		return transaction_id;
	}

private:
	atomic<bool> written {false};
	const idx_t transaction_id;
};

class DatabricksTransactionManager : public TransactionManager {
public:
	explicit DatabricksTransactionManager(AttachedDatabase &db);
	~DatabricksTransactionManager() override;

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;
	void Checkpoint(ClientContext &context, bool force = false) override;
	void RetireEntries(vector<shared_ptr<CatalogEntry>> entries);

private:
	void EndTransaction(Transaction &transaction);

	struct RetiredBatch {
		idx_t stamp;
		vector<shared_ptr<CatalogEntry>> entries;
	};

	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<DatabricksTransaction>> transactions;
	idx_t next_transaction_id = 1;
	vector<RetiredBatch> retired;
};

} // namespace duckdb
