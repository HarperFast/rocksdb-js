#include "transaction_handle.h"

namespace rocksdb_js {

/**
 * Creates a new RocksDB transaction, enables snapshots, and sets the
 * transaction id.
 */
TransactionHandle::TransactionHandle(std::shared_ptr<DBHandle> dbHandle) :
	dbHandle(dbHandle),
	txn(nullptr)
{
	if (dbHandle->descriptor->mode == DBMode::Pessimistic) {
		auto* tdb = static_cast<rocksdb::TransactionDB*>(dbHandle->descriptor->db.get());
		rocksdb::TransactionOptions txnOptions;
		this->txn = tdb->BeginTransaction(rocksdb::WriteOptions(), txnOptions);
	} else if (dbHandle->descriptor->mode == DBMode::Optimistic) {
		auto* odb = static_cast<rocksdb::OptimisticTransactionDB*>(dbHandle->descriptor->db.get());
		rocksdb::OptimisticTransactionOptions txnOptions;
		this->txn = odb->BeginTransaction(rocksdb::WriteOptions(), txnOptions);
	} else {
		throw std::runtime_error("Invalid database");
	}
	this->txn->SetSnapshot();
	this->id = this->txn->GetId() & 0xffffffff;
}

/**
 * Destroys the handle's RocksDB transaction.
 */
TransactionHandle::~TransactionHandle() {
	this->release();
}

/**
 * Get a value using the specified database handle.
 */
rocksdb::Status TransactionHandle::get(
	rocksdb::Slice& key,
	std::string& result,
	std::shared_ptr<DBHandle> dbHandleOverride
) {
	auto readOptions = rocksdb::ReadOptions();
	readOptions.snapshot = this->txn->GetSnapshot();

	std::shared_ptr<DBHandle> dbHandle = dbHandleOverride ? dbHandleOverride : this->dbHandle;
	auto column = dbHandle->column.get();

	// TODO: should this be GetForUpdate?
	return this->txn->Get(readOptions, column, key, &result);
}

/**
 * Get a value using the specified database handle.
 */
rocksdb::Status TransactionHandle::getSync(
	rocksdb::Slice& key,
	std::string& result,
	std::shared_ptr<DBHandle> dbHandleOverride
) {
	auto readOptions = rocksdb::ReadOptions();
	readOptions.snapshot = this->txn->GetSnapshot();

	std::shared_ptr<DBHandle> dbHandle = dbHandleOverride ? dbHandleOverride : this->dbHandle;
	auto column = dbHandle->column.get();

	// TODO: should this be GetForUpdate?
	return this->txn->Get(readOptions, column, key, &result);
}

/**
 * Put a value using the specified database handle.
 */
rocksdb::Status TransactionHandle::putSync(
	rocksdb::Slice& key,
	rocksdb::Slice& value,
	std::shared_ptr<DBHandle> dbHandleOverride
) {
	std::shared_ptr<DBHandle> dbHandle = dbHandleOverride ? dbHandleOverride : this->dbHandle;
	auto column = dbHandle->column.get();
	return this->txn->Put(column, key, value);
}

/**
 * Remove a value using the specified database handle.
 */
rocksdb::Status TransactionHandle::removeSync(
	rocksdb::Slice& key,
	std::shared_ptr<DBHandle> dbHandleOverride
) {
	std::shared_ptr<DBHandle> dbHandle = dbHandleOverride ? dbHandleOverride : this->dbHandle;
	auto column = dbHandle->column.get();
	return this->txn->Delete(column, key);
}

/**
 * Release the transaction. This is called after successful commit, after
 * the transaction has been aborted, or when the transaction is destroyed.
 */
void TransactionHandle::release() {
	if (this->txn) {
		this->txn->ClearSnapshot();
		delete this->txn;
		this->txn = nullptr;
	}
}

} // namespace rocksdb_js
