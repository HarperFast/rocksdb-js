#include <sstream>
#include "database.h"
#include "db_descriptor.h"
#include "db_settings.h"
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
	coordinatedRetry(false),
	state(TransactionState::Pending),
	txn(nullptr),
	committedPosition(0, 0) {
	this->resetTransaction();
	this->id = this->dbHandle->descriptor->transactionGetNextId();

	this->startTimestamp = rocksdb_js::getMonotonicTimestamp();
}

void TransactionHandle::resetTransaction(){
	// clear/delete the previous transaction and create a new transaction so that it can be retried
	if (this->txn) {
		this->txn->ClearSnapshot();
		delete this->txn;
	}

	this->logEntryBatch.reset();
	this->snapshotSet = false; // snapshot flag so it will be reapplied

	rocksdb::WriteOptions writeOptions;
	writeOptions.disableWAL = dbHandle->disableWAL;

	auto dbHandle = this->dbHandle;
	if (dbHandle->descriptor->mode == DBMode::Pessimistic) {
		auto* tdb = static_cast<rocksdb::TransactionDB*>(dbHandle->descriptor->db.get());
		rocksdb::TransactionOptions txnOptions;
		this->txn = tdb->BeginTransaction(writeOptions, txnOptions);
	} else if (dbHandle->descriptor->mode == DBMode::Optimistic) {
		auto* odb = static_cast<rocksdb::OptimisticTransactionDB*>(dbHandle->descriptor->db.get());
		rocksdb::OptimisticTransactionOptions txnOptions;
		this->txn = odb->BeginTransaction(writeOptions, txnOptions);
	} else {
		throw rocksdb_js::DBException("Invalid database");
	}
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
			throw rocksdb_js::DBException("Log already bound to a transaction");
		}
	} else {
		// Bind under transactionBindMutex so the bind+increment is atomic with
		// respect to tryClose()'s phase-3 check-and-mark-closing sequence.
		// transactionBindMutex is never held during I/O, so this cannot stall the
		// event loop the way holding writeMutex here would.
		std::lock_guard<std::mutex> lock(entry->store->transactionBindMutex);
		if (entry->store->isClosing.load(std::memory_order_relaxed)) {
			throw rocksdb_js::DBException("Transaction log store is closed");
		}
		this->boundLogStore = entry->store;
		entry->store->pendingTransactionCount++;
		DEBUG_LOG("%p TransactionHandle::addLogEntry Binding transaction %u to log store \"%s\"\n",
			this, this->id, entry->store->name.c_str());
	}

	if (!this->logEntryBatch) {
		this->logEntryBatch = std::make_unique<TransactionLogEntryBatch>(this->startTimestamp);
	}

	this->logEntryBatch->addEntry(std::move(entry));
}

void TransactionHandle::lockVTSlot(
	const std::shared_ptr<DBHandle>& dbHandle,
	const rocksdb::Slice& key
) {
	auto* vt = DBSettings::getInstance().getVerificationTableRaw();
	if (!vt) return;

	// Use the transaction's base descriptor as the database identity key.
	// All column families of the same physical DB share the same descriptor.
	uintptr_t dbPtr = reinterpret_cast<uintptr_t>(this->dbHandle->descriptor.get());
	uint32_t cfId = dbHandle->getColumnFamilyHandle()->GetID();
	auto* slot = vt->slotFor(dbPtr, cfId, key);
	if (!slot) return;

	uint64_t v = slot->load(std::memory_order_acquire);
	if (vtIsLock(v)) {
		// Slot is already locked — either by us (same key written earlier in
		// this transaction) or by a concurrent transaction. Either way, readers
		// already see the lock and fall through to RocksDB. We don't need to
		// install a second lock for this slot.
		return;
	}

	// Slot is 0 or a version. Install our own LockTracker via a single CAS.
	// We do not retry: if the CAS loses to another transaction's lock, that
	// lock already invalidates the cache for this slot.
	uint16_t gen = vtNextGen();
	LockTracker* t = new LockTracker(vt->slotIndexOf(slot), gen, dbPtr);
	t->holders.store(1, std::memory_order_relaxed);

	if (slot->compare_exchange_strong(v, vtEncodeLock(t, gen),
	        std::memory_order_release, std::memory_order_acquire)) {
		lockedVTSlots.push_back(slot);
		heldTrackers.push_back(t);
	} else {
		delete t;
	}
}

void TransactionHandle::releaseIntent() {
	for (size_t i = 0; i < lockedVTSlots.size(); i++) {
		auto* slot = lockedVTSlots[i];
		LockTracker* t = heldTrackers[i];

		// CAS the slot from our lock-encoded value back to 0.
		// If the CAS fails some other code already cleared the slot (e.g.
		// DB close or a concurrent populateVersion); either is fine.
		uint64_t expected = vtEncodeLock(t, t->generation);
		slot->compare_exchange_strong(expected, 0ULL,
		    std::memory_order_release, std::memory_order_acquire);

		// Notify any waiters that were parked on this slot.
		t->wake();

		if (t->refcount.fetch_sub(1, std::memory_order_release) == 1) delete t;
	}

	lockedVTSlots.clear();
	heldTrackers.clear();
}

/**
 * Release the transaction. This is called after successful commit, after
 * the transaction has been aborted, or when the transaction is destroyed.
 */
void TransactionHandle::close() {
	if (this->dbHandle && this->dbHandle->descriptor) {
		this->dbHandle->descriptor->transactionRemove(shared_from_this());
	}

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

	// if the transaction was aborted (either via an error, explicit abort, or was pending), we need
	// to remove the committed position from the log store
	if (this->state != TransactionState::Committed && this->committedPosition.logSequenceNumber > 0) {
		auto store = this->boundLogStore.lock();
		if (store) {
			store->commitAborted(this->committedPosition);
		}
	}

	// If the transaction was bound to a log store but writeBatch() was never called (committedPosition
	// is still zero), the pendingTransactionCount was incremented at bind time but never decremented
	// by writeBatch(). Decrement it now so the store can be safely destroyed.
	//
	// Guard under transactionBindMutex and verify isClosing first: if tryClose() already closed
	// the store and reset the count to zero we must not decrement again (count would go negative).
	if (this->committedPosition.logSequenceNumber == 0) {
		auto store = this->boundLogStore.lock();
		if (store) {
			std::lock_guard<std::mutex> bindLock(store->transactionBindMutex);
			if (!store->isClosing.load(std::memory_order_relaxed)) {
				store->pendingTransactionCount--;
			}
		}
	}

	// Release any VT locks that were installed at putSync/removeSync time
	// but not yet released (e.g. transaction aborted or DB closed mid-commit).
	if (!this->lockedVTSlots.empty()) {
		this->releaseIntent();
	}

	// destroy the RocksDB transaction
	this->txn->ClearSnapshot();
	delete this->txn;
	this->txn = nullptr;

	if (this->jsDatabaseRef != nullptr) {
		DEBUG_LOG("%p TransactionHandle::close Cleaning up reference to database\n", this);
		NAPI_STATUS_THROWS_ERROR_VOID(::napi_delete_reference(this->env, this->jsDatabaseRef), "Failed to delete reference to database");
		DEBUG_LOG("%p TransactionHandle::close Reference to database deleted successfully\n", this);
		this->jsDatabaseRef = nullptr;
	} else {
		DEBUG_LOG("%p TransactionHandle::close jsDatabaseRef is already null\n", this);
	}

	// the transaction should already be removed from the registry when
	// committing/aborting  so we don't need to call transactionRemove here to
	// avoid race conditions and bad_weak_ptr errors
	DEBUG_LOG("%p TransactionHandle::close Transaction should already be removed from registry\n", this);

	this->dbHandle.reset();
}

/**
 * Get a value using the specified database handle.
 */
napi_value TransactionHandle::get(
	napi_env env,
	std::string &key,
	napi_value resolve,
	napi_value reject,
	std::shared_ptr<DBHandle> dbHandleOverride,
	std::atomic<uint64_t>* vtSlot,
	bool hasExpectedVersion,
	uint64_t expectedVersion,
	bool wantsPopulate
) {
	if (!this->txn) {
		::napi_throw_error(env, nullptr, "Transaction is closed");
		return nullptr;
	}

	if (this->state != TransactionState::Pending) {
		DEBUG_LOG("%p TransactionHandle::get Transaction is not in pending state (state=%d)\n", this, this->state);
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
		dbHandle->getColumnFamilyHandle(),
		key,
		&value
	);

	if (!status.IsIncomplete()) {
		// Block-cache hit. Apply VT check/populate before resolving.
		if (vtSlot && status.ok()) {
			rocksdb::Slice valueSlice(value.data(), value.size());
			uint64_t extracted = VerificationTable::extractVersionFromValue(valueSlice);
			if (hasExpectedVersion && extracted != 0 && extracted == expectedVersion) {
				VerificationTable::populateVersion(vtSlot, expectedVersion);
				napi_value global, freshResult;
				::napi_get_global(env, &global);
				::napi_create_int32(env, FRESH_VERSION_FLAG, &freshResult);
				::napi_call_function(env, global, resolve, 1, &freshResult, nullptr);
				NAPI_STATUS_THROWS(::napi_create_uint32(env, 0, &returnStatus));
				return returnStatus;
			}
			if ((hasExpectedVersion || wantsPopulate) && extracted != 0) {
				VerificationTable::populateVersion(vtSlot, extracted);
			}
		}
		return resolveGetSyncResult(env, "Transaction get failed", status, value, resolve, reject);
	}

	napi_value name;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(
		env,
		"transaction.get",
		NAPI_AUTO_LENGTH,
		&name
	));

	readOptions.read_tier = rocksdb::kReadAllTier;
	auto state = new AsyncGetState<TransactionHandle*>(env, this, readOptions, std::move(key));
	state->vtSlot = vtSlot;
	state->hasExpectedVersion = hasExpectedVersion;
	state->expectedVersion = expectedVersion;
	state->wantsPopulate = wantsPopulate;
	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef));
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef));

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
					state->handle->dbHandle->getColumnFamilyHandle(),
					state->key,
					&state->value
				);
			}
			// signal that execute handler is complete
			state->signalExecuteCompleted();
		},
		[](napi_env env, napi_status status, void* data) { // complete
			auto state = reinterpret_cast<AsyncGetState<TransactionHandle*>*>(data);
			state->deleteAsyncWork();

			if (status != napi_cancelled) {
				resolveGetResult(env, "Transaction get failed", state);
			}

			delete state;
		},
		state,     // data
		&state->asyncWork // -> result
	));

	// register the async work with the transaction handle
	this->registerAsyncWork();

	NAPI_STATUS_THROWS(::napi_queue_async_work(env, state->asyncWork));

	NAPI_STATUS_THROWS(::napi_create_uint32(env, 1, &returnStatus));
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
			dbHandle->getColumnFamilyHandle(),
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
	rocksdb::PinnableSlice& result,
	rocksdb::ReadOptions& readOptions,
	std::shared_ptr<DBHandle> dbHandleOverride
) {
	if (!this->txn) {
		return rocksdb::Status::Aborted("Transaction is closed");
	}

	if (this->state != TransactionState::Pending) {
		DEBUG_LOG("%p TransactionHandle::getSync Transaction is not in pending state (state=%d)\n", this, this->state);
		return rocksdb::Status::Aborted("Transaction is not in pending state");
	}

	if (!this->disableSnapshot && !this->snapshotSet) {
		this->snapshotSet = true;
		this->txn->SetSnapshot();
	}

	if (this->snapshotSet) {
		readOptions.snapshot = this->txn->GetSnapshot();
	}

	std::shared_ptr<DBHandle> dbHandle = dbHandleOverride ? dbHandleOverride : this->dbHandle;
	auto column = dbHandle->getColumnFamilyHandle();

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
		DEBUG_LOG("%p TransactionHandle::putSync Transaction is not in pending state (state=%d)\n", this, this->state);
		return rocksdb::Status::Aborted("Transaction is not in pending state");
	}

	if (!this->disableSnapshot && !this->snapshotSet && this->dbHandle->descriptor->mode == DBMode::Pessimistic) {
		this->snapshotSet = true;
		this->txn->SetSnapshot();
	}

	std::shared_ptr<DBHandle> dbHandle = dbHandleOverride ? dbHandleOverride : this->dbHandle;
	auto column = dbHandle->getColumnFamilyHandle();
	rocksdb::Status status = this->txn->Put(column, key, value);

	// Lock the VT slot for this key immediately on write. This ensures that
	// any cached version of the key is invalidated as soon as it enters the
	// transaction's write buffer — not deferred to commit time. This upholds
	// the invariant that a cached version is only trusted when there is a
	// single visible version of the record across all transactions.
	if (status.ok() && dbHandle->enableVerificationTable) {
		this->lockVTSlot(dbHandle, key);
	}

	return status;
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
		DEBUG_LOG("%p TransactionHandle::removeSync Transaction is not in pending state (state=%d)\n", this, this->state);
		return rocksdb::Status::Aborted("Transaction is not in pending state");
	}

	if (!this->disableSnapshot && !this->snapshotSet && this->dbHandle->descriptor->mode == DBMode::Pessimistic) {
		this->snapshotSet = true;
		this->txn->SetSnapshot();
	}

	std::shared_ptr<DBHandle> dbHandle = dbHandleOverride ? dbHandleOverride : this->dbHandle;
	auto column = dbHandle->getColumnFamilyHandle();
	rocksdb::Status status = this->txn->Delete(column, key);

	if (status.ok() && dbHandle->enableVerificationTable) {
		this->lockVTSlot(dbHandle, key);
	}

	return status;
}

} // namespace rocksdb_js
