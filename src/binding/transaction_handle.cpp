#include <sstream>
#include "database.h"
#include "db_descriptor.h"
#include "db_iterator_handle.h"
#include "transaction_handle.h"
#include "macros.h"

namespace rocksdb_js {

/**
 * Creates a new RocksDB transaction, enables snapshots, and sets the
 * transaction id.
 */
TransactionHandle::TransactionHandle(
	std::shared_ptr<DBHandle> dbHandle,
	napi_env env,
	napi_ref jsDatabaseRef,
	bool disableSnapshot
) :
	dbHandle(dbHandle),
	env(env),
	jsDatabaseRef(jsDatabaseRef),
	disableSnapshot(disableSnapshot),
	snapshotSet(false),
	state(TransactionState::Pending),
	txn(nullptr)
{
	rocksdb::WriteOptions writeOptions;
	writeOptions.disableWAL = dbHandle->disableWAL;

	if (dbHandle->descriptor->mode == DBMode::Pessimistic) {
		auto* tdb = static_cast<rocksdb::TransactionDB*>(dbHandle->descriptor->db.get());
		rocksdb::TransactionOptions txnOptions;
		this->txn = tdb->BeginTransaction(writeOptions, txnOptions);
	} else if (dbHandle->descriptor->mode == DBMode::Optimistic) {
		auto* odb = static_cast<rocksdb::OptimisticTransactionDB*>(dbHandle->descriptor->db.get());
		rocksdb::OptimisticTransactionOptions txnOptions;
		this->txn = odb->BeginTransaction(writeOptions, txnOptions);
	} else {
		throw std::runtime_error("Invalid database");
	}

	this->id = this->dbHandle->descriptor->transactionGetNextId();

	this->startTimestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::system_clock::now().time_since_epoch()
	).count();
}

/**
 * Destroys the handle's RocksDB transaction.
 */
TransactionHandle::~TransactionHandle() {
	this->close();
}

/**
 * Adds a log entry to the specified transaction log store's batch.
 */
void TransactionHandle::addLogEntry(std::unique_ptr<TransactionLogEntry> entry) {
	DEBUG_LOG("%p TransactionHandle::addLogEntry Adding log entry to store \"%s\" for transaction %u (size=%zu)\n",
		this, entry->store->name.c_str(), this->id, entry->size);

	// check if this transaction is already bound to a different log store
	auto currentBoundStore = this->boundLogStore.lock();
	if (currentBoundStore) {
		// transaction is already bound to a log store
		if (currentBoundStore.get() != entry->store.get()) {
			throw std::runtime_error("Log already bound to a transaction");
		}
	} else {
		// bind this transaction to the log store
		this->boundLogStore = entry->store;
		DEBUG_LOG("%p TransactionHandle::addLogEntry Binding transaction %u to log store \"%s\"\n",
			this, this->id, entry->store->name.c_str());
	}

	if (!this->logEntryBatch) {
		this->logEntryBatch = std::make_unique<TransactionLogEntryBatch>(this->startTimestamp);
	}

	this->logEntryBatch->addEntry(std::move(entry));
}

/**
 * Release the transaction. This is called after successful commit, after
 * the transaction has been aborted, or when the transaction is destroyed.
 */
void TransactionHandle::close() {
	if (!this->txn) {
		return;
	}

	// update state to aborted if not already committed
	if (this->state == TransactionState::Pending || this->state == TransactionState::Committing) {
		this->state = TransactionState::Aborted;
	}

	// cancel all active async work before closing
	this->cancelAllAsyncWork();

	// wait for all async work to complete before closing
	this->waitForAsyncWorkCompletion();

	// destroy the RocksDB transaction
	this->txn->ClearSnapshot();
	delete this->txn;
	this->txn = nullptr;

	if (this->jsDatabaseRef != nullptr) {
		DEBUG_LOG("%p TransactionHandle::close cleaning up reference to database\n", this)
		NAPI_STATUS_THROWS_ERROR_VOID(::napi_delete_reference(this->env, this->jsDatabaseRef), "Failed to delete reference to database")
		DEBUG_LOG("%p TransactionHandle::close reference to database deleted successfully\n", this)
		this->jsDatabaseRef = nullptr;
	} else {
		DEBUG_LOG("%p TransactionHandle::close jsDatabaseRef is already null\n", this)
	}

	// the transaction should already be removed from the registry when
	// committing/aborting  so we don't need to call transactionRemove here to
	// avoid race conditions and bad_weak_ptr errors
	DEBUG_LOG("%p TransactionHandle::close transaction should already be removed from registry\n", this)

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

	if (this->state != TransactionState::Pending) {
		DEBUG_LOG("%p TransactionHandle::get Transaction is not in pending state (state=%d)\n", this, this->state)
		::napi_throw_error(env, nullptr, "Transaction is not in pending state");
		return nullptr;
	}

	if (!this->disableSnapshot && !this->snapshotSet) {
		this->snapshotSet = true;
		this->txn->SetSnapshot();
	}

	napi_value returnStatus;
	std::string value;
	std::shared_ptr<DBHandle> dbHandle = dbHandleOverride ? dbHandleOverride : this->dbHandle;

	rocksdb::ReadOptions readOptions;
	if (this->snapshotSet) {
		readOptions.snapshot = this->txn->GetSnapshot();
	}
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
	auto state = new AsyncGetState<TransactionHandle*>(env, this, readOptions, key);
	// Use refcount 1 to prevent GC during async operation, but don't manually delete
	// to avoid crashes during environment teardown. The small memory leak is acceptable.
	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef))
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef))

	NAPI_STATUS_THROWS(::napi_create_async_work(
		env,       // node_env
		nullptr,   // async_resource
		name,      // async_resource_name
		[](napi_env doNotUse, void* data) { // execute
			auto state = reinterpret_cast<AsyncGetState<TransactionHandle*>*>(data);
			// check if database is still open before proceeding
			if (!state->handle || !state->handle->dbHandle || !state->handle->dbHandle->opened() || state->handle->dbHandle->isCancelled()) {
				state->status = rocksdb::Status::Aborted("Database closed during transaction get operation");
			} else {
				state->status = state->handle->txn->Get(
					state->readOptions,
					state->handle->dbHandle->column.get(),
					state->keySlice,
					&state->value
				);
			}
			// signal that execute handler is complete
			state->signalExecuteCompleted();
		},
		[](napi_env env, napi_status status, void* data) { // complete
			auto state = reinterpret_cast<AsyncGetState<TransactionHandle*>*>(data);

			if (status != napi_cancelled) {
				resolveGetResult(env, "Transaction get failed", state);
			}

			state->deleteAsyncWork();
			delete state;
		},
		state,     // data
		&state->asyncWork // -> result
	));

	// register the async work with the transaction handle
	this->registerAsyncWork();

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

	if (this->state != TransactionState::Pending) {
		DEBUG_LOG("%p TransactionHandle::getSync Transaction is not in pending state (state=%d)\n", this, this->state)
		return rocksdb::Status::Aborted("Transaction is not in pending state");
	}

	if (!this->disableSnapshot && !this->snapshotSet) {
		this->snapshotSet = true;
		this->txn->SetSnapshot();
	}

	auto readOptions = rocksdb::ReadOptions();
	if (this->snapshotSet) {
		readOptions.snapshot = this->txn->GetSnapshot();
	}

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

	if (this->state != TransactionState::Pending) {
		DEBUG_LOG("%p TransactionHandle::putSync Transaction is not in pending state (state=%d)\n", this, this->state)
		return rocksdb::Status::Aborted("Transaction is not in pending state");
	}

	if (!this->disableSnapshot && !this->snapshotSet && this->dbHandle->descriptor->mode == DBMode::Pessimistic) {
		this->snapshotSet = true;
		this->txn->SetSnapshot();
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

	if (this->state != TransactionState::Pending) {
		DEBUG_LOG("%p TransactionHandle::removeSync Transaction is not in pending state (state=%d)\n", this, this->state)
		return rocksdb::Status::Aborted("Transaction is not in pending state");
	}

	if (!this->disableSnapshot && !this->snapshotSet && this->dbHandle->descriptor->mode == DBMode::Pessimistic) {
		this->snapshotSet = true;
		this->txn->SetSnapshot();
	}

	std::shared_ptr<DBHandle> dbHandle = dbHandleOverride ? dbHandleOverride : this->dbHandle;
	auto column = dbHandle->column.get();
	return this->txn->Delete(column, key);
}

} // namespace rocksdb_js
