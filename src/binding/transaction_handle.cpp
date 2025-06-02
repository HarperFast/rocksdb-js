#include <sstream>
#include "database.h"
#include "db_iterator_handle.h"
#include "transaction_handle.h"
#include "macros.h"

namespace rocksdb_js {

/**
 * Creates a new RocksDB transaction, enables snapshots, and sets the
 * transaction id.
 */
TransactionHandle::TransactionHandle(std::shared_ptr<DBHandle> dbHandle, bool disableSnapshot) :
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

	if (!disableSnapshot) {
		this->txn->SetSnapshot();
	}

	this->id = this->txn->GetId() & 0xffffffff;
}

/**
 * Destroys the handle's RocksDB transaction.
 */
TransactionHandle::~TransactionHandle() {
	this->close();
}

/**
 * Release the transaction. This is called after successful commit, after
 * the transaction has been aborted, or when the transaction is destroyed.
 */
void TransactionHandle::close() {
	if (!this->txn) {
		return;
	}

	// destroy the RocksDB transaction
	this->txn->ClearSnapshot();
	delete this->txn;
	this->txn = nullptr;

	// unregister this transaction handle from the descriptor
	if (this->dbHandle && this->dbHandle->descriptor) {
		this->dbHandle->descriptor->transactionRemove(this->id);
	}
	
	this->dbHandle.reset();
}

/**
 * Get a value using the specified database handle.
 */
napi_value TransactionHandle::get(
	napi_env env,
	rocksdb::Slice& key,
	napi_value resolve,
	napi_value reject,
	std::shared_ptr<DBHandle> dbHandleOverride
) {
	if (!this->txn) {
		::napi_throw_error(env, nullptr, "Transaction is closed");
		return nullptr;
	}

	napi_value returnStatus;
	std::string value;
	std::shared_ptr<DBHandle> dbHandle = dbHandleOverride ? dbHandleOverride : this->dbHandle;

	rocksdb::ReadOptions readOptions;
	readOptions.snapshot = this->txn->GetSnapshot();
	readOptions.read_tier = rocksdb::kBlockCacheTier;

	rocksdb::Status status = this->txn->Get(
		readOptions,
		dbHandle->column.get(),
		key,
		&value
	);

	if (!status.IsIncomplete()) {
		// found it in the block cache!
		return resolveGetSyncResult(env, "Transaction get failed", status, value, resolve, reject);
	}

	napi_value name;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(
		env,
		"transaction.get",
		NAPI_AUTO_LENGTH,
		&name
	))

	readOptions.read_tier = rocksdb::kReadAllTier;
	auto state = new GetState<TransactionHandle*>(env, this, readOptions, key);
	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef))
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef))

	NAPI_STATUS_THROWS(::napi_create_async_work(
		env,       // node_env
		nullptr,   // async_resource
		name,      // async_resource_name
		[](napi_env env, void* data) { // execute
			auto state = reinterpret_cast<GetState<TransactionHandle*>*>(data);
			state->status = state->handle->txn->Get(
				state->readOptions,
				state->handle->dbHandle->column.get(),
				state->keySlice,
				&state->value
			);
		},
		[](napi_env env, napi_status status, void* data) { // complete
			auto state = reinterpret_cast<GetState<TransactionHandle*>*>(data);
			resolveGetResult(env, "Transaction get failed", state->status, state->value, state->resolveRef, state->rejectRef);
			delete state;
		},
		state,     // data
		&state->asyncWork // -> result
	));

	NAPI_STATUS_THROWS(::napi_queue_async_work(env, state->asyncWork))

	NAPI_STATUS_THROWS(::napi_create_uint32(env, 1, &returnStatus))
	return returnStatus;
}

void TransactionHandle::getCount(
	DBIteratorOptions& itOptions,
	uint64_t& count,
	std::shared_ptr<DBHandle> dbHandleOverride
) {
	std::shared_ptr<DBHandle> dbHandle = dbHandleOverride ? dbHandleOverride : this->dbHandle;

	// if we don't have a start or end key, we can just get the estimated number of keys
	if (itOptions.startKeyStr == nullptr && itOptions.endKeyStr == nullptr) {
		dbHandle->descriptor->db->GetIntProperty(
			dbHandle->column.get(),
			"rocksdb.estimate-num-keys",
			&count
		);
	} else {
		std::unique_ptr<DBIteratorHandle> itHandle = std::make_unique<DBIteratorHandle>(this, itOptions);
		for (count = 0; itHandle->iterator->Valid(); ++count) {
			itHandle->iterator->Next();
		}
	}
}

/**
 * Get a value using the specified database handle.
 */
rocksdb::Status TransactionHandle::getSync(
	rocksdb::Slice& key,
	std::string& result,
	std::shared_ptr<DBHandle> dbHandleOverride
) {
	if (!this->txn) {
		return rocksdb::Status::Aborted("Transaction is closed");
	}

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
	if (!this->txn) {
		return rocksdb::Status::Aborted("Transaction is closed");
	}

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
	if (!this->txn) {
		return rocksdb::Status::Aborted("Transaction is closed");
	}

	std::shared_ptr<DBHandle> dbHandle = dbHandleOverride ? dbHandleOverride : this->dbHandle;
	auto column = dbHandle->column.get();
	return this->txn->Delete(column, key);
}

} // namespace rocksdb_js
