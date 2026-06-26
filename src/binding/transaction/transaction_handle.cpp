#include <chrono>
#include <sstream>
#include <thread>
#include "database/database.h"
#include "database/db_descriptor.h"
#include "database/db_settings.h"
#include "iterator/db_iterator_handle.h"
#include "transaction/transaction_handle.h"
#include "core/test_seam.h"
#include "napi/macros.h"

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
	envThreadId(std::this_thread::get_id()),
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

	auto dbHandle = this->dbHandle;
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
 *
 * @example
 * ```typescript
 * await db.transaction(async (txn) => {
 *   const log = txn.useLog('foo'); // transaction log store will be bound to this transaction
 *   log.addEntry(Buffer.from('hello'));
 *   log.addEntry(Buffer.from('world'));
 * });
 * ```
 */
void TransactionHandle::addLogEntry(std::unique_ptr<TransactionLogEntry> entry) {
	DEBUG_LOG("%p TransactionHandle::addLogEntry Adding log entry to store \"%s\" for transaction %u (size=%zu)\n",
		this, entry->store->name.c_str(), this->id, entry->size);

	// #668 (defense in depth): the write-ahead log is write-once per transaction. If
	// committedPosition is already set, this transaction's batch was durably written by a
	// prior commit attempt (committedPosition survives resetTransaction). A commit that
	// returned IsBusy is retried by re-running the transaction body to re-drive the RocksDB
	// commit; re-staging the log here would write the records a second time at a new
	// position, orphaning the original (commitFinished is gated on !IsBusy, so the original
	// is never finalized) and pinning the committed-read watermark at it forever — silent
	// committed-read truncation (HarperFast/harper-pro#426). Higher layers are expected to
	// suppress the re-log on retry (e.g. harper's DatabaseTransaction.isRetry), but enforce
	// write-once here too so a stray re-stage from any caller cannot corrupt the watermark.
	if (this->committedPosition.logSequenceNumber > 0) {
		DEBUG_LOG("%p TransactionHandle::addLogEntry Skipping re-stage on retry for transaction %u "
			"(WAL already written at seq %u)\n",
			this, this->id, this->committedPosition.logSequenceNumber);
		return;
	}

	// check if this transaction is already bound to a different log store
	auto currentBoundStore = this->boundLogStore.lock();
	if (currentBoundStore) {
		// transaction is already bound to a log store
		if (currentBoundStore->name != entry->store->name) {
			std::string errorMessage = "Transaction " + std::to_string(this->id) + " is already bound to the log store \"" + currentBoundStore->name + "\"";
			throw rocksdb_js::DBException(errorMessage);
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

	// Register a write intent on the slot. lockSlotForWrite installs a new
	// LockTracker or joins an existing one as an additional holder (when another
	// transaction — or this one, via an earlier write to a colliding key —
	// already locked it), all under the VT's writer mutex. Joining is essential:
	// if a second concurrent writer skipped registering an intent, a reader
	// could repopulate the slot with a now-stale version after the first writer
	// released but before the second committed.
	LockTracker* t = vt->lockSlotForWrite(slot, dbPtr);
	if (t) {
		lockedVTSlots.push_back(slot);
		heldTrackers.push_back(t);
	}
}

void TransactionHandle::releaseIntent() {
	if (!lockedVTSlots.empty()) {
		// The trackers were created via the VT, so it is materialized and
		// getVerificationTableRaw() returns it. releaseWriteIntent drops this
		// transaction's holder reference under the writer mutex; the slot is
		// only cleared (and waiters woken) when the last holder releases.
		auto* vt = DBSettings::getInstance().getVerificationTableRaw();
		if (vt) {
			for (size_t i = 0; i < lockedVTSlots.size(); i++) {
				vt->releaseWriteIntent(lockedVTSlots[i], heldTrackers[i]);
			}
		}
	}

	lockedVTSlots.clear();
	heldTrackers.clear();
}

/**
 * Release the transaction. This is called after successful commit, after
 * the transaction has been aborted, or when the transaction is destroyed.
 *
 * The `closed` atomic gate ensures this runs at most once even when called
 * from multiple threads concurrently (e.g. DBDescriptor::close() on env M's
 * JS thread racing the async commit's complete callback on env W's JS thread).
 */
void TransactionHandle::close() {
	if (this->closed.exchange(true)) {
		return;
	}

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

	// Test seam: widen the PATH A vs PATH B race window (see txnCloseTestDelayMs).
	// This window is real in production (PATH B fires after waitForAsyncWorkCompletion
	// unblocks); the seam makes it wide enough to reproduce deterministically.
	// Noop in production.
	const int closeDelayMs = testDelayMs("ROCKSDB_JS_TXN_CLOSE_DELAY_MS");
	if (closeDelayMs > 0) {
		std::this_thread::sleep_for(std::chrono::milliseconds(closeDelayMs));
	}

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
		if (std::this_thread::get_id() == this->envThreadId) {
			// On the owning JS thread — safe to call napi_delete_reference.
			DEBUG_LOG("%p TransactionHandle::close Cleaning up reference to database\n", this);
			NAPI_STATUS_THROWS_ERROR_VOID(::napi_delete_reference(this->env, this->jsDatabaseRef), "Failed to delete reference to database");
			DEBUG_LOG("%p TransactionHandle::close Reference to database deleted successfully\n", this);
		} else {
			// Wrong thread (close() called from a different env's JS thread, e.g.
			// DBDescriptor::close() PATH A). napi_delete_reference is not thread-safe
			// across envs; skip and let Node clean up the weak ref on env teardown.
			DEBUG_LOG("%p TransactionHandle::close Skipping napi_delete_reference (wrong thread)\n", this);
		}
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
	uint64_t observedSlot,
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
		// Block-cache hit. Apply the VT freshness check and seed before resolving.
		// vtPopulateIfSettled reads the key's LATEST committed version (not this
		// transaction's snapshot value) and gates on the single-version invariant,
		// so a transactional read seeds the cache only when settled and can never
		// publish a stale snapshot value.
		if (vtSlot && status.ok()) {
			rocksdb::Slice valueSlice(value.data(), value.size());
			uint64_t extracted = VerificationTable::extractVersionFromValue(valueSlice);
			const rocksdb::Snapshot* readSnapshot = this->readSnapshot();
			if (hasExpectedVersion && extracted != 0 && extracted == expectedVersion) {
				vtPopulateIfSettled(dbHandle, vtSlot, rocksdb::Slice(key.data(), key.size()), extracted, readSnapshot, observedSlot);
				napi_value global, freshResult;
				::napi_get_global(env, &global);
				::napi_create_int32(env, FRESH_VERSION_FLAG, &freshResult);
				::napi_call_function(env, global, resolve, 1, &freshResult, nullptr);
				NAPI_STATUS_THROWS(::napi_create_uint32(env, 0, &returnStatus));
				return returnStatus;
			}
			if ((hasExpectedVersion || wantsPopulate) && extracted != 0) {
				vtPopulateIfSettled(dbHandle, vtSlot, rocksdb::Slice(key.data(), key.size()), extracted, readSnapshot, observedSlot);
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
	state->vtObserved = observedSlot;
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
	std::unique_ptr<DBIteratorHandle> itHandle = std::make_unique<DBIteratorHandle>(this, itOptions);
	for (count = 0; itHandle->iterator->Valid(); ++count) {
		itHandle->iterator->Next();
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

	this->ensureSnapshot();

	if (this->snapshotSet) {
		readOptions.snapshot = this->txn->GetSnapshot();
	}

	std::shared_ptr<DBHandle> dbHandle = dbHandleOverride ? dbHandleOverride : this->dbHandle;
	auto column = dbHandle->getColumnFamilyHandle();

	// TODO: should this be GetForUpdate?
	return this->txn->Get(readOptions, column, key, &result);
}

void TransactionHandle::ensureSnapshot() {
	if (this->txn && !this->disableSnapshot && !this->snapshotSet) {
		this->snapshotSet = true;
		this->txn->SetSnapshot();
	}
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
