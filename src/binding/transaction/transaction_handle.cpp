#include <cassert>
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

namespace {

/**
 * Keeps a target DBHandle open while a transaction resolves and pins that
 * handle's column descriptor. This bridges the gap between the caller's open
 * check and the transaction's async-work registration without making the
 * worker access a concurrently closing DBHandle.
 */
struct ScopedAsyncWorkRegistration {
	AsyncWorkHandle* handle;

	explicit ScopedAsyncWorkRegistration(AsyncWorkHandle* handle)
		: handle(handle) {
		if (this->handle) {
			this->handle->registerAsyncWork();
		}
	}

	~ScopedAsyncWorkRegistration() {
		if (this->handle) {
			this->handle->unregisterAsyncWork();
		}
	}

	ScopedAsyncWorkRegistration(const ScopedAsyncWorkRegistration&) = delete;
	ScopedAsyncWorkRegistration& operator=(const ScopedAsyncWorkRegistration&) = delete;

	void release() {
		this->handle = nullptr;
	}
};

template<typename State>
struct PendingAsyncState {
	napi_env env;
	State* state;

	PendingAsyncState(napi_env env, State* state)
		: env(env), state(state) {}

	~PendingAsyncState() {
		if (!this->state) return;
		if (this->state->resolveRef) {
			::napi_delete_reference(this->env, this->state->resolveRef);
			this->state->resolveRef = nullptr;
		}
		if (this->state->rejectRef) {
			::napi_delete_reference(this->env, this->state->rejectRef);
			this->state->rejectRef = nullptr;
		}
		this->state->deleteAsyncWork();
		delete this->state;
	}

	void release() {
		this->state = nullptr;
	}

	PendingAsyncState(const PendingAsyncState&) = delete;
	PendingAsyncState& operator=(const PendingAsyncState&) = delete;
};

} // namespace

/**
 * Creates a new RocksDB transaction, enables snapshots, and sets the
 * transaction id.
 */
TransactionHandle::TransactionHandle(std::shared_ptr<DBHandle> dbHandle, bool disableSnapshot) :
	dbHandle(dbHandle),
	disableSnapshot(disableSnapshot),
	coordinatedRetry(false),
	state(TransactionState::Pending),
	txn(nullptr),
	committedPosition(0, 0) {
	this->resetTransaction();
	this->id = this->dbHandle->descriptor->transactionGetNextId();

	this->startTimestamp = rocksdb_js::getMonotonicTimestamp();
	this->createdAt = std::chrono::steady_clock::now();
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

	// Use the descriptor's per-open epoch as the database identity key (not the
	// descriptor pointer, which is reused across a close/reopen of the same path).
	// All column families of the same physical DB share the same epoch, separated
	// by cfId.
	uint64_t dbId = dbHandle->descriptor->vtEpoch;
	uint32_t cfId = dbHandle->getColumnFamilyHandle()->GetID();
	auto* slot = vt->slotFor(dbId, cfId, key);
	if (!slot) return;

	// Register a write intent on the slot. lockSlotForWrite installs a new
	// LockTracker or joins an existing one as an additional holder (when another
	// transaction — or this one, via an earlier write to a colliding key —
	// already locked it), all under the VT's writer mutex. Joining is essential:
	// if a second concurrent writer skipped registering an intent, a reader
	// could repopulate the slot with a now-stale version after the first writer
	// released but before the second committed.
	LockTracker* t = vt->lockSlotForWrite(slot, dbId);
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
 * The JS wrapper was garbage collected, so nothing can commit, abort, or read through this handle
 * again — request its release. A commit in flight is the exception: TransactionCommitState holds
 * its own shared_ptr and closing here would cancel it mid-flight, so completeCommitWork closes it
 * instead when it settles. Other published dependents retry the close when they release.
 */
void TransactionHandle::onWrapperCollected() {
	this->wrapperCollected.store(true);
	this->closeOrphanIfUnused();
}

void TransactionHandle::registerIterator() {
	this->activeIteratorCount.fetch_add(1, std::memory_order_relaxed);
}

void TransactionHandle::unregisterIterator() {
	const uint32_t previous = this->activeIteratorCount.fetch_sub(1, std::memory_order_relaxed);
	assert(previous > 0 && "Transaction iterator count underflow");
	if (previous == 1) {
		this->closeOrphanIfUnused();
	}
}

void TransactionHandle::closeOrphanIfUnused() {
	if (!this->wrapperCollected.load() || this->closed.load()) {
		return;
	}

	if (this->state == TransactionState::Committing) {
		DEBUG_LOG("%p TransactionHandle::closeOrphanIfUnused Commit in flight, deferring close (txnId=%u)\n", this, this->id);
		return;
	}

	this->cancelAllAsyncWork();
	const int32_t activeAsyncWork = this->activeAsyncWorkCount.load();
	const uint32_t activeIterators = this->activeIteratorCount.load(std::memory_order_relaxed);
	if (activeAsyncWork > 0 || activeIterators > 0) {
		DEBUG_LOG("%p TransactionHandle::closeOrphanIfUnused Deferring close (txnId=%u, async=%d, iterators=%u)\n",
			this, this->id, activeAsyncWork, activeIterators);
		return;
	}

	DEBUG_LOG("%p TransactionHandle::closeOrphanIfUnused Closing orphaned transaction (txnId=%u, state=%d)\n",
		this, this->id, static_cast<int>(this->state));
	// transactionRemove() can drop the registry's last reference. Keep this
	// object alive until close() returns even when the final dependent releases
	// on a worker thread.
	auto keepAlive = this->shared_from_this();
	this->close();
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

	// cancel all active async work before closing
	this->cancelAllAsyncWork();

	// Drain BEFORE touching anything the in-flight work owns. Nothing below is
	// safe while a commit is still executing: `state` feeds the commitAborted()
	// decision, releaseIntent() mutates VT state the commit is using, and
	// `delete txn` hands RocksDB a dangling transaction.
	const bool drained = this->waitForAsyncWorkCompletion();

	// Test seam: widen the PATH A vs PATH B race window (see txnCloseTestDelayMs).
	// This window is real in production (PATH B fires after waitForAsyncWorkCompletion
	// unblocks); the seam makes it wide enough to reproduce deterministically.
	// Noop in production.
	const int closeDelayMs = testDelayMs("ROCKSDB_JS_TXN_CLOSE_DELAY_MS");
	if (closeDelayMs > 0) {
		std::this_thread::sleep_for(std::chrono::milliseconds(closeDelayMs));
	}

	if (!drained) {
		// The drain timed out with work still executing against `txn` (e.g. a
		// worker env torn down during a slow commit). Destroying now would free
		// a transaction RocksDB is still using and could mark the log aborted
		// while the data commit goes on to succeed — so deliberately leak
		// instead. The in-flight commit owns its own cleanup (it releases VT
		// intents and resolves the log position on completion); this close must
		// not steal it. A leaked transaction is recoverable; a use-after-free
		// and a log/data disagreement are not. The complete admission-and-drain
		// contract that removes this window is HarperFast/rocksdb-js#784.
		DEBUG_LOG("%p TransactionHandle::close async work still in flight after drain timeout; leaking txn rather than freeing it\n", this);
		return;
	}

	// Only now that no native work can be running: settle the final state.
	if (this->state == TransactionState::Pending || this->state == TransactionState::Committing) {
		this->state = TransactionState::Aborted;
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

	// Note: close() is deliberately napi-free. The transaction holds no napi
	// refs (the JS database is passed to UseLog by the TS layer per-call), so
	// close is safe from any thread and any teardown phase — a weak napi_ref
	// held here and still alive at worker-env teardown crashes Node's
	// second-pass finalizer drain (HarperFast/rocksdb-js#741).

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
	// Register before inspecting txn/state. Descriptor-wide close can select the
	// transaction before the target DBHandle, and close() must not reset txn in
	// the middle of this setup. Async fallback transfers this registration to its
	// state; synchronous and failed setup paths release it on return.
	ScopedAsyncWorkRegistration transactionRegistration(this);
	if (this->isCancelled() || !this->txn) {
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
	// Cross-column-family reads enter through another DBHandle. Register against
	// that handle while copying its descriptor so a concurrent close cannot reset
	// columnDescriptor in the gap between the caller's open check and this read.
	ScopedAsyncWorkRegistration targetHandleRegistration(dbHandleOverride.get());
	if (dbHandleOverride && dbHandle->isCancelled()) {
		::napi_throw_error(env, nullptr, "Database closed during transaction get operation");
		return nullptr;
	}
	auto readColumnDescriptor = dbHandle->columnDescriptor;

	rocksdb::ReadOptions readOptions;
	if (this->snapshotSet) {
		readOptions.snapshot = this->txn->GetSnapshot();
	}
	readOptions.read_tier = rocksdb::kBlockCacheTier;

	rocksdb::Status status = this->txn->Get(
		readOptions,
		readColumnDescriptor->column.get(),
		key,
		&value
	);

	if (!status.IsIncomplete()) {
		// Block-cache hit. Apply the VT freshness check and seed before resolving.
		// vtPopulateIfSettled reads the key's LATEST committed version (not this
		// transaction's snapshot value) and gates on the single-version invariant,
		// so a transactional read seeds the cache only when settled and can never
		// publish a stale snapshot value.
		rocksdb::Slice valueSlice(value.data(), value.size());
		if (vtSlot && status.ok() && !VerificationTable::valueVersionIsNotUnique(valueSlice)) {
			uint64_t extracted = VerificationTable::extractVersionFromValue(valueSlice);
			const rocksdb::Snapshot* readSnapshot = this->readSnapshot();
			const rocksdb::Slice keySlice(key.data(), key.size());
			// The caller's column family, not the handle's: this read may be routed to another one.
			const VtLatestCheck latest = vtCheckLatest(
				dbHandle->descriptor->db.get(),
				readColumnDescriptor->column.get(),
				keySlice,
				readSnapshot
			);
			if (!latest.notUnique) {
				const uint64_t populateVersion = latest.read ? latest.latestVersion : extracted;
				const rocksdb::Snapshot* populateSnapshot = latest.read ? nullptr : readSnapshot;
				if (hasExpectedVersion && extracted != 0 && extracted == expectedVersion) {
					vtPopulateIfSettled(dbHandle, vtSlot, keySlice, populateVersion, populateSnapshot, observedSlot);
					napi_value global, freshResult;
					::napi_get_global(env, &global);
					::napi_create_int32(env, FRESH_VERSION_FLAG, &freshResult);
					::napi_call_function(env, global, resolve, 1, &freshResult, nullptr);
					NAPI_STATUS_THROWS(::napi_create_uint32(env, 0, &returnStatus));
					return returnStatus;
				}
				if ((hasExpectedVersion || wantsPopulate) && extracted != 0) {
					vtPopulateIfSettled(dbHandle, vtSlot, keySlice, populateVersion, populateSnapshot, observedSlot);
				}
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
	auto state = new AsyncGetState<std::shared_ptr<TransactionHandle>>(
		env,
		this->shared_from_this(),
		readOptions,
		std::move(key)
	);
	// Until the transaction registration is transferred below, setup failures
	// must delete this state without unregistering work it does not own.
	state->completed.store(true);
	PendingAsyncState pendingState(env, state);
	// Resolve and pin the caller's column family on the JS thread. The worker
	// releases this descriptor before signaling completion so the native column
	// family handle cannot outlive its RocksDB database during teardown.
	state->readColumnDescriptor = std::move(readColumnDescriptor);
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
			auto state = reinterpret_cast<AsyncGetState<std::shared_ptr<TransactionHandle>>*>(data);
			const int getDelayMs = testDelayMs("ROCKSDB_JS_TXN_GET_DELAY_MS");
			if (getDelayMs > 0) {
				std::this_thread::sleep_for(std::chrono::milliseconds(getDelayMs));
			}
			if (!state->handle || state->handle->isCancelled()) {
				state->status = rocksdb::Status::Aborted("Database closed during transaction get operation");
			} else {
				state->status = state->handle->txn->Get(
					state->readOptions,
					state->readColumnDescriptor->column.get(),
					state->key,
					&state->value
				);
				// While the database and the caller's column family are still pinned — the completion
				// runs after teardown may have released both.
				if (state->status.ok() && state->vtSlot) {
					vtCheckAsyncGet(
						state,
						state->handle->dbHandle->descriptor->db.get(),
						state->readColumnDescriptor->column.get()
					);
				}
			}
			state->readColumnDescriptor.reset();
			// signal that execute handler is complete
			state->signalExecuteCompleted();
			state->handle->closeOrphanIfUnused();
		},
		[](napi_env env, napi_status status, void* data) { // complete
			auto state = reinterpret_cast<AsyncGetState<std::shared_ptr<TransactionHandle>>*>(data);
			state->deleteAsyncWork();

			if (status != napi_cancelled) {
				resolveGetResult(env, "Transaction get failed", state);
			}

			delete state;
		},
		state,     // data
		&state->asyncWork // -> result
	));

	// Transfer the registration claimed at function entry to the queued state.
	// If queueing fails, PendingAsyncState deletes the state, whose base
	// destructor releases the registration and whose derived members release the
	// column descriptor first.
	state->completed.store(false);
	transactionRegistration.release();
	NAPI_STATUS_THROWS(::napi_queue_async_work(env, state->asyncWork));
	pendingState.release();

	NAPI_STATUS_THROWS(::napi_create_uint32(env, 1, &returnStatus));
	return returnStatus;
}

bool TransactionHandle::getCount(
	DBIteratorOptions& itOptions,
	uint64_t& count,
	std::shared_ptr<DBHandle> dbHandleOverride
) {
	this->ensureSnapshot();
	if (this->snapshotSet) {
		itOptions.readOptions.snapshot = this->txn->GetSnapshot();
	}

	std::unique_ptr<DBIteratorHandle> itHandle =
		std::make_unique<DBIteratorHandle>(this->shared_from_this(), itOptions, dbHandleOverride);
	return itHandle->countRemaining(count);
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
