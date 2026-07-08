#include <chrono>
#include <cstdlib>
#include <cstring>
#include <thread>
#include "database/database.h"
#include "database/db_descriptor.h"
#include "database/db_handle.h"
#include "iterator/db_iterator.h"
#include "database/db_settings.h"
#include "napi/macros.h"
#include "transaction/transaction.h"
#include "transaction/transaction_handle.h"
#include "core/platform.h"
#include "core/test_seam.h"
#include "napi/helpers.h"
#include "napi/async.h"
#include "core/verification_table.h"

#define UNWRAP_TRANSACTION_HANDLE(fnName) \
	std::shared_ptr<TransactionHandle>* txnHandle = nullptr; \
	do { \
		NAPI_STATUS_THROWS(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&txnHandle))); \
		if (!txnHandle || !(*txnHandle)) { \
			::napi_throw_error(env, nullptr, fnName " failed: Transaction has already been closed"); \
			return nullptr; \
		} \
	} while (0)

#define NAPI_THROW_JS_ERROR(code, message) \
	napi_value error; \
	rocksdb_js::createJSError(env, code, message, error); \
	::napi_throw(env, error); \
	return nullptr

namespace rocksdb_js {

/**
 * Creates a new `NativeTransaction` object.
 *
 * @param env - The NAPI environment.
 * @param info - The callback info.
 * @returns The new `NativeTransaction` object.
 *
 * @example
 * ```typescript
 * const db = RocksDatabase.open('/path/to/database');
 * const txn = new NativeTransaction(db);
 * txn.putSync('key', 'value');
 * await txn.commit();
 * ```
 */
napi_value Transaction::Constructor(napi_env env, napi_callback_info info) {
	NAPI_CONSTRUCTOR_ARGV_WITH_DATA("Transaction", 2);

	napi_ref exportsRef = reinterpret_cast<napi_ref>(data);
	napi_value exports;
	NAPI_STATUS_THROWS(::napi_get_reference_value(env, exportsRef, &exports));

	napi_value databaseCtor;
	bool isDatabase = false;
	NAPI_STATUS_THROWS(::napi_get_named_property(env, exports, "Database", &databaseCtor));
	NAPI_STATUS_THROWS(::napi_instanceof(env, argv[0], databaseCtor, &isDatabase));
	if (!isDatabase) {
		::napi_throw_error(env, nullptr, "Invalid argument, expected Database instance");
		return nullptr;
	}

	std::shared_ptr<DBHandle>* dbHandle = nullptr;
	NAPI_STATUS_THROWS(::napi_unwrap(env, argv[0], reinterpret_cast<void**>(&dbHandle)));

	if (dbHandle == nullptr || !(*dbHandle)->opened()) {
		::napi_throw_error(env, nullptr, "Database not open");
		return nullptr;
	}

	if ((*dbHandle)->descriptor->closing.load()) {
		::napi_throw_error(env, nullptr, "Database is closing!");
		return nullptr;
	}

	// THROW_IF_READONLY((*dbHandle)->descriptor, "");

	bool disableSnapshot = false;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, argv[1], "disableSnapshot", disableSnapshot));

	bool coordinatedRetry = false;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, argv[1], "coordinatedRetry", coordinatedRetry));

	napi_ref jsDatabaseRef;
	NAPI_STATUS_THROWS(::napi_create_reference(env, argv[0], 0, &jsDatabaseRef));

	// create shared_ptr on heap so it persists after function returns
	std::shared_ptr<TransactionHandle>* txnHandle = new std::shared_ptr<TransactionHandle>(
		std::make_shared<TransactionHandle>(*dbHandle, env, jsDatabaseRef, disableSnapshot)
	);
	(*txnHandle)->coordinatedRetry = coordinatedRetry;

	(*dbHandle)->descriptor->transactionAdd(*txnHandle);

	DEBUG_LOG(
		"%p Transaction::Constructor Initializing transaction %u (dbHandle=%p, dbDescriptor=%p, use_count=%ld)\n",
		(*txnHandle).get(),
		(*txnHandle)->id,
		(*txnHandle)->dbHandle.get(),
		(*txnHandle)->dbHandle->descriptor.get(),
		(*txnHandle)->dbHandle.use_count()
	);

	try {
		NAPI_STATUS_THROWS(::napi_wrap(
			env,
			jsThis,
			reinterpret_cast<void*>(txnHandle),
			[](napi_env env, void* data, void* hint) {
				auto* txnHandle = static_cast<std::shared_ptr<TransactionHandle>*>(data);
				DEBUG_LOG("Transaction::Constructor NativeTransaction GC'd (txnHandle=%p, ref count=%ld)\n",
					data, txnHandle->use_count());
				[[maybe_unused]] auto id = (*txnHandle)->id;
				if (*txnHandle) {
					(*txnHandle).reset();
				}
				delete txnHandle;
			},
			nullptr, // finalize_hint
			nullptr  // result
		));

		return jsThis;
	} catch (const std::exception& e) {
		delete txnHandle;
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}
}

/**
 * Aborts the transaction.
 */
napi_value Transaction::Abort(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_TRANSACTION_HANDLE("Abort");

	TransactionState txnState = (*txnHandle)->state;
	if (txnState == TransactionState::Aborted) {
		// already aborted
		return nullptr;
	}
	if (txnState == TransactionState::Committing || txnState == TransactionState::Committed) {
		NAPI_THROW_JS_ERROR("ERR_ALREADY_COMMITTED", "Transaction has already been committed");
	}
	bool hadLogWrites = (*txnHandle)->committedPosition.logSequenceNumber > 0;

	(*txnHandle)->state = TransactionState::Aborted;

	ROCKSDB_STATUS_THROWS_ERROR_LIKE((*txnHandle)->txn->Rollback(), "Transaction rollback failed");
	DEBUG_LOG("Transaction::Abort closing txnHandle=%p txnId=%u\n", (*txnHandle).get(), (*txnHandle)->id);
	(*txnHandle)->close();

	if (hadLogWrites) {
		NAPI_THROW_JS_ERROR("ERR_TRANSACTION_ABANDONED", "Transaction was abandoned after writing to the transaction log");
	}

	NAPI_RETURN_UNDEFINED();
}

/**
 * Context passed through the RETRY_NOW TSFN; owns the resolve/reject refs.
 * Freed by retryNowFinalize after the TSFN fires.
 */
struct RetryNowContext {
	napi_ref resolveRef;
	napi_ref rejectRef;
};

static void retryNowCallJs(napi_env env, napi_value /*func*/, void* context, void* /*data*/) {
	auto* ctx = reinterpret_cast<RetryNowContext*>(context);
	napi_value global, resolveFn, retryVal;
	::napi_get_global(env, &global);
	::napi_get_reference_value(env, ctx->resolveRef, &resolveFn);
	::napi_create_int32(env, RETRY_NOW_VALUE, &retryVal);
	::napi_call_function(env, global, resolveFn, 1, &retryVal, nullptr);
}

static void retryNowFinalize(napi_env env, void* finalizeData, void* /*hint*/) {
	auto* ctx = reinterpret_cast<RetryNowContext*>(finalizeData);
	if (ctx->resolveRef) ::napi_delete_reference(env, ctx->resolveRef);
	if (ctx->rejectRef) ::napi_delete_reference(env, ctx->rejectRef);
	delete ctx;
}

/**
 * State for the `Commit` async work.
 */
struct TransactionCommitState final : BaseAsyncState<std::shared_ptr<TransactionHandle>> {
	bool hasLog;
	// Slot pointers captured before releaseIntent() for coordinated-retry parking.
	std::vector<std::atomic<uint64_t>*> savedSlots;

	TransactionCommitState(
		napi_env env,
		std::shared_ptr<TransactionHandle> handle
	) :
		BaseAsyncState<std::shared_ptr<TransactionHandle>>(env, handle),
		hasLog(false) {}
};

/**
 * Log-lane stage of the commit: validates the handle and writes the
 * transaction-log batch (recording the committed position). Runs off the JS
 * thread — on the database's log lane, or on a libuv threadpool thread in the
 * legacy path (which runs both stages back to back). A pass-through no-op for
 * transactions with no log entries so total dispatch order is preserved into
 * the commit lane.
 */
static void executeLogWork(TransactionCommitState* state) {
	auto txnHandle = state->handle;
	if (!txnHandle) {
		DEBUG_LOG("%p Transaction::Commit ERROR: Called with nullptr txnHandle\n", txnHandle.get());
		state->status = rocksdb::Status::Aborted("Database closed during transaction commit operation");
	} else if (txnHandle->isCancelled()) {
		DEBUG_LOG("%p Transaction::Commit ERROR: Called with txnHandle cancelled\n", txnHandle.get());
		state->status = rocksdb::Status::Aborted("Database closed during transaction commit operation");
	} else if (!txnHandle->dbHandle) {
		DEBUG_LOG("%p Transaction::Commit ERROR: Called with nullptr dbHandle\n", txnHandle.get());
		state->status = rocksdb::Status::Aborted("Database closed during transaction commit operation");
	} else if (!txnHandle->dbHandle->opened()) {
		DEBUG_LOG("%p Transaction::Commit ERROR: Called with dbHandle not opened\n", txnHandle.get());
		state->status = rocksdb::Status::Aborted("Database closed during transaction commit operation");
	} else if (txnHandle->logEntryBatch) {
		DEBUG_LOG("%p Transaction::Commit Committing log entries for transaction %u\n",
			txnHandle.get(), txnHandle->id);
		auto store = txnHandle->boundLogStore.lock();
		if (store) {
			try {
				// write the batch to the store
				store->writeBatch(*txnHandle->logEntryBatch, txnHandle->committedPosition);
				// free the batch after writing to avoid memory leak
				txnHandle->logEntryBatch.reset();
				state->hasLog = true;
			} catch (const std::exception& e) {
				DEBUG_LOG("%p Transaction::Commit ERROR: writeBatch failed for transaction %u: %s\n", txnHandle.get(), txnHandle->id, e.what());
				state->status = rocksdb::Status::Aborted(e.what());
			}
		} else {
			DEBUG_LOG("%p Transaction::Commit ERROR: Log store not found for transaction %u\n", txnHandle.get(), txnHandle->id);
			state->status = rocksdb::Status::Aborted("Log store not found for transaction");
		}
	}
}

/**
 * Commit-lane stage of the commit: commits the RocksDB transaction and records
 * the committed position against the RocksDB sequence number. Runs off the JS
 * thread — on the database's commit lane, or on a libuv threadpool thread in
 * the legacy path. Always runs after executeLogWork on the same state; skips
 * the RocksDB commit if the log stage failed. This stage's commitFinished can
 * run concurrently with the log lane's writeBatch for a later transaction —
 * the store synchronizes the shared position state on dataSetsMutex (the same
 * interleaving the legacy multi-threaded libuv path exercised).
 */
static void executeCommitWork(TransactionCommitState* state) {
	auto txnHandle = state->handle;
	// The log stage already failed the commit on any invalid-handle condition;
	// only proceed past here with a usable handle.
	if (txnHandle && txnHandle->dbHandle && txnHandle->dbHandle->descriptor) {
		auto descriptor = txnHandle->dbHandle->descriptor;

		// ensure the log stage (or handle validation) hasn't errored
		if (state->status.ok()) {
			if (testForceTryAgain()) {
				// Test seam: strand this commit. Roll back so no data lands, then report TryAgain
				// so the retry path (reset onto a fresh snapshot + re-run) is exercised. A failed
				// rollback (e.g. underlying DB corruption) is a real error, not a simulated one —
				// surface it rather than masking it behind the forced TryAgain.
				rocksdb::Status rollbackStatus = txnHandle->txn->Rollback();
				state->status = rollbackStatus.ok()
					? rocksdb::Status::TryAgain("forced stranded snapshot (test seam)")
					: rollbackStatus;
			} else {
				state->status = txnHandle->txn->Commit();
			}

			// For coordinated retry: save slot pointers before
			// releaseIntent() clears them so the complete callback
			// can park on any new lock installed on those slots.
			if (state->status.IsBusy() && txnHandle->coordinatedRetry) {
				state->savedSlots = txnHandle->lockedVTSlots;
			}

			// Release VT locks that were installed at putSync/removeSync
			// time, regardless of commit outcome (success or IsBusy).
			if (!txnHandle->lockedVTSlots.empty()) {
				txnHandle->releaseIntent();
			}
		}

		// Publish the log entries (advance the committed-read watermark) only when the data
		// transaction actually committed. IsBusy and TryAgain are both retried on a reset
		// transaction (committedPosition survives, so the WAL batch is not rewritten — #668),
		// and the eventual successful commit is what publishes. A hard error is abandoned:
		// close()'s commitAborted drops the position from the uncommitted set so it stops
		// pinning the watermark (the bytes themselves cannot be unwritten — abandoning a
		// logged transaction remains the loudly-flagged ERR_TRANSACTION_ABANDONED case).
		// Gating on !IsBusy (rather than ok()) published on a failed TryAgain commit, making
		// the entry visible while the record was rolled back and only re-committed later on a
		// retry — a change-feed entry ahead of its data for every reader in that window.
		if (txnHandle->committedPosition.logSequenceNumber > 0 && state->status.ok()) {
			auto store = txnHandle->boundLogStore.lock();
			if (store) {
				store->commitFinished(txnHandle->committedPosition, descriptor->db->GetLatestSequenceNumber());
			} else {
				DEBUG_LOG("%p Transaction::Commit ERROR: Log store not found for transaction, log number: %u id: %u\n", txnHandle.get(), txnHandle->committedPosition.logSequenceNumber, txnHandle->id);
				state->status = rocksdb::Status::Aborted("Log store not found for transaction");
			}
		}

		if (state->status.ok()) {
			DEBUG_LOG("%p Transaction::Commit Emitted committed event (txnId=%u)\n", txnHandle.get(), txnHandle->id);
			txnHandle->state = TransactionState::Committed;
			descriptor->notify("committed", nullptr);
		} else if (state->status.IsBusy() || state->status.IsTryAgain()) {
			DEBUG_LOG("%p Transaction::Commit ERROR: Commit failed with %s, resetting transaction\n",
				txnHandle.get(), state->status.IsBusy() ? "IsBusy" : "TryAgain");
			// Clear/delete the previous transaction and create a new one so the retry re-drives the
			// commit. resetTransaction preserves committedPosition (the WAL batch stays write-once,
			// #668) but takes a fresh RocksDB snapshot: IsBusy converges by re-tracking keys at the
			// current sequence, and TryAgain — whose snapshot was stranded outside the memtable
			// window after a flush, so recommitting the same transaction re-checks the same lost
			// history forever (harper#1695) — converges because the re-run reads and validates
			// against current state instead. The caller must re-run the transaction body so the
			// reads are re-taken on the new snapshot (db.transaction()'s retry loop does this).
			txnHandle->resetTransaction();
		}
	}
	// signal that execute handler is complete
	state->signalExecuteCompleted();
}

// forward declaration; defined after completeCommitWork
static void commitCompletionCallJs(napi_env env, napi_value jsCallback, void* context, void* data);

/**
 * Completes the commit on the JS thread: resolves/rejects the commit promise,
 * handles the coordinated-retry parking, and closes the transaction handle.
 * Shared by the legacy async-work complete callback and the commit-thread
 * tsfn callback. Does NOT free the state.
 */
static void completeCommitWork(napi_env env, TransactionCommitState* state) {
	if (state->status.ok()) {
		if (state->handle) {
			DEBUG_LOG("%p Transaction::Commit Complete closing (txnId=%u)\n", state->handle.get(), state->handle->id);
			state->handle->close();
			DEBUG_LOG("%p Transaction::Commit Complete closed (txnId=%u)\n", state->handle.get(), state->handle->id);
		} else {
			DEBUG_LOG("%p Transaction::Commit Complete, but handle is null!\n", state->handle.get());
		}

		state->callResolve();
	} else if (
		state->status.IsBusy() &&
		state->handle &&
		state->handle->coordinatedRetry
	) {
		// Coordinated-retry path: signal RETRY_NOW to JS instead of
		// rejecting. The native layer may park on an active VT lock
		// before firing the resolve, so JS retries only after the
		// conflicting transaction has released its write intent.
		// Intentionally IsBusy-only: RETRY_NOW parks on the conflicting
		// holder's VT-slot lock, and TryAgain (stranded snapshot after a
		// flush) has no lock holder to park on — it takes the normal
		// reject→reset→retry path below instead.
		if (state->handle->state == TransactionState::Committing) {
			state->handle->state = TransactionState::Pending;
		}

		// Transfer resolve/reject refs from state to a RetryNowContext
		// so the TSFN finalize can clean them up.
		auto* ctx = new RetryNowContext{state->resolveRef, state->rejectRef};
		state->resolveRef = nullptr;
		state->rejectRef = nullptr;

		bool parked = false;
		VerificationTable* vt = DBSettings::getInstance().getVerificationTableRaw();
		for (auto* slot : state->savedSlots) {
			// refTrackerIfLocked takes a temporary reference under the VT
			// writer mutex, so the tracker cannot be freed by a concurrent
			// releaseWriteIntent between loading the slot and referencing it
			// (the old load-then-incref use-after-free).
			LockTracker* t = vt ? vt->refTrackerIfLocked(slot) : nullptr;
			if (!t) continue;

			// Create a TSFN that calls resolve(RETRY_NOW) when fired.
			napi_value resource_name;
			::napi_create_string_latin1(env, "transaction.retry", NAPI_AUTO_LENGTH, &resource_name);
			napi_threadsafe_function tsfn;
			::napi_create_threadsafe_function(
				env, nullptr, nullptr, resource_name,
				0, 1,
				ctx, retryNowFinalize,
				ctx, retryNowCallJs,
				&tsfn
			);
			::napi_unref_threadsafe_function(env, tsfn);

			// Register wake callback; if the tracker already fired wake()
			// before we got here, addWakeCallback returns false and we
			// call+release the TSFN immediately (async on the JS thread).
			bool registered = t->addWakeCallback([tsfn]() {
				::napi_call_threadsafe_function(tsfn, nullptr, napi_tsfn_nonblocking);
				::napi_release_threadsafe_function(tsfn, napi_tsfn_release);
			});
			if (!registered) {
				::napi_call_threadsafe_function(tsfn, nullptr, napi_tsfn_nonblocking);
				::napi_release_threadsafe_function(tsfn, napi_tsfn_release);
			}

			vt->unrefTracker(t);
			parked = true;
			break;
		}

		if (!parked) {
			// No active lock found; resolve RETRY_NOW directly (we are on
			// the JS thread in this complete callback).
			napi_value global, resolveFn, retryVal;
			::napi_get_global(env, &global);
			::napi_get_reference_value(env, ctx->resolveRef, &resolveFn);
			::napi_create_int32(env, RETRY_NOW_VALUE, &retryVal);
			::napi_call_function(env, global, resolveFn, 1, &retryVal, nullptr);
			::napi_delete_reference(env, ctx->resolveRef);
			::napi_delete_reference(env, ctx->rejectRef);
			delete ctx;
		}
		// If parked, ctx is owned by the TSFN finalize; do not free here.
	} else {
		// Normal error path: reset to Pending so JS can retry.
		// Guard: keep Aborted if close() already set it (DB closing
		// during commit) — don't let Transaction::Abort call Rollback()
		// on a null txn.
		if (state->handle && state->handle->state == TransactionState::Committing) {
			state->handle->state = TransactionState::Pending;
		}
		napi_value error;
		ROCKSDB_CREATE_ERROR_LIKE_VOID(error, state->status, "Transaction commit failed");
		napi_value hasLogValue;
		// #668: writeBatch is skipped on an IsBusy/TryAgain retry once the WAL batch is durable
		// (committedPosition set), so state->hasLog is false this attempt. Fall back to
		// committedPosition so the retry heuristic still treats this as logged.
		bool hasLog = state->hasLog ||
			(state->handle && state->handle->committedPosition.logSequenceNumber > 0);
		napi_status status = ::napi_get_boolean(env, hasLog, &hasLogValue);
		if (status == napi_ok) {
			::napi_set_named_property(env, error, "hasLog", hasLogValue);
		}
		state->callReject(error);
	}
}

/**
 * TSFN callback: completes a commit dispatched to the commit thread on the
 * originating JS thread, accounts the completion (unref-ing the tsfn when the
 * env goes idle), and frees the state.
 */
static void commitCompletionCallJs(napi_env env, napi_value jsCallback, void* context, void* data) {
	TransactionCommitState* state = reinterpret_cast<TransactionCommitState*>(data);

	// env is nullptr when the env is tearing down; nothing left to resolve and
	// the descriptor's per-env tsfn is being released — do not touch napi.
	if (env != nullptr) {
		// Pin the descriptor across completion: completeCommitWork may close the
		// txn handle (dropping the state's references), but we still need the
		// descriptor for the pending accounting below. The state pins it here
		// (state -> txnHandle -> dbHandle -> descriptor).
		std::shared_ptr<DBDescriptor> descriptor =
			(state->handle && state->handle->dbHandle) ? state->handle->dbHandle->descriptor : nullptr;
		completeCommitWork(env, state);
		if (descriptor) {
			descriptor->finishCommitCompletion(env);
		}
	}

	delete state;
}

/**
 * How async commits are executed, selected by ROCKSDB_JS_COMMIT_THREAD:
 * - `0` / `false`: legacy path — one libuv async-work item per commit.
 * - unset / anything else: single dedicated commit thread per database
 *   (default) — both the log write and the RocksDB commit run on the commit
 *   lane. Best measured throughput (no inter-lane handoff, full cache
 *   locality).
 * - `2`: two-lane pipeline — the log lane writes the transaction-log batch,
 *   then forwards to the commit lane for the RocksDB commit, letting the
 *   stages overlap across transactions. Measured slower than single-lane on
 *   synthetic loads (the per-txn handoff outweighs the overlap for small
 *   commits); selectable for evaluation on real workloads.
 */
enum class CommitThreadMode { Legacy, SingleLane, TwoLane };

static CommitThreadMode commitThreadMode() {
	static const CommitThreadMode mode = []() {
		const char* v = ::getenv("ROCKSDB_JS_COMMIT_THREAD");
		if (v != nullptr && (::strcmp(v, "0") == 0 || ::strcmp(v, "false") == 0)) {
			return CommitThreadMode::Legacy;
		}
		if (v != nullptr && ::strcmp(v, "2") == 0) {
			return CommitThreadMode::TwoLane;
		}
		return CommitThreadMode::SingleLane;
	}();
	return mode;
}

/**
 * Test-only seam: milliseconds the commit thread sleeps after executing the
 * commit and before calling back into JS. Widens the window in which env
 * teardown can race an in-flight commit completion so the worker-teardown
 * test reproduces deterministically. Zero (unset) in production.
 */
static unsigned commitDelayMs() {
	static const unsigned ms = []() -> unsigned {
		const char* v = ::getenv("ROCKSDB_JS_COMMIT_DELAY_MS");
		return v != nullptr ? static_cast<unsigned>(::atoi(v)) : 0;
	}();
	return ms;
}

/**
 * Commits the transaction.
 */
napi_value Transaction::Commit(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2);
	napi_value resolve = argv[0];
	napi_value reject = argv[1];
	UNWRAP_TRANSACTION_HANDLE("Commit");

	TransactionCommitState* state = new TransactionCommitState(env, *txnHandle);
	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef));
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef));

	TransactionState txnState = (*txnHandle)->state;
	if (txnState == TransactionState::Aborted) {
		NAPI_THROW_JS_ERROR("ERR_ALREADY_ABORTED", "Transaction has already been aborted");
	}
	if (txnState == TransactionState::Committing || txnState == TransactionState::Committed) {
		// already committed
		napi_value global;
		NAPI_STATUS_THROWS(::napi_get_global(env, &global));
		NAPI_STATUS_THROWS(::napi_call_function(env, global, resolve, 0, nullptr, nullptr));
		delete state;
		return nullptr;
	}
	DEBUG_LOG("%p Transaction::Commit Setting state to committing\n", (*txnHandle).get(), (*txnHandle)->id);
	(*txnHandle)->state = TransactionState::Committing;

	auto dbHandle = (*txnHandle)->dbHandle;
	CommitThreadMode mode = commitThreadMode();
	if (mode != CommitThreadMode::Legacy && dbHandle && dbHandle->descriptor) {
		// The commit lanes are owned by the descriptor and drained/joined before
		// the descriptor is destroyed; and an in-flight commit's state pins the
		// descriptor alive (state -> txnHandle -> dbHandle -> descriptor). So a
		// raw pointer captured into the worker task is valid for the task's
		// lifetime and cannot form a reference cycle with the worker thread.
		DBDescriptor* descriptor = dbHandle->descriptor.get();

		// Ensure this env has a completion tsfn and account the dispatch (refs
		// the tsfn as the env goes idle->busy so the event loop stays alive).
		NAPI_STATUS_THROWS(descriptor->registerCommitCompletion(env, commitCompletionCallJs));

		// register the commit with the transaction handle so close() can wait
		(*txnHandle)->registerAsyncWork();

		// Commit-lane stage: RocksDB commit, then marshal the completion back
		// to the originating env. dispatchCommitCompletion returns false if
		// that env was torn down (e.g. worker terminate) — the completion has
		// nowhere to run, so drop the state.
		auto commitStage = [descriptor, state]() {
			executeCommitWork(state);
			if (unsigned delay = commitDelayMs()) {
				std::this_thread::sleep_for(std::chrono::milliseconds(delay));
			}
			if (!descriptor->dispatchCommitCompletion(state->env, state)) {
				DEBUG_LOG("%p Transaction::Commit commit thread: env gone, dropping completion\n", state);
				delete state;
			}
		};

		if (mode == CommitThreadMode::TwoLane) {
			// Two-lane pipeline: the log lane writes the transaction-log batch,
			// then forwards to the commit lane. Every commit passes through
			// both lanes so total order is preserved.
			descriptor->logWorker.enqueue([descriptor, state, commitStage]() {
				executeLogWork(state);
				descriptor->commitWorker.enqueue(commitStage);
			});
		} else {
			// Single lane (default): both stages run back to back on the
			// commit lane.
			descriptor->commitWorker.enqueue([state, commitStage]() {
				executeLogWork(state);
				commitStage();
			});
		}

		NAPI_RETURN_UNDEFINED();
	}

	// Legacy path: dispatch the commit to the libuv threadpool.
	napi_value name;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(
		env,
		"transaction.commit",
		NAPI_AUTO_LENGTH,
		&name
	));

	NAPI_STATUS_THROWS(::napi_create_async_work(
		env,       // node_env
		nullptr,   // async_resource
		name,      // async_resource_name
		[](napi_env doNotUse, void* data) { // execute
			TransactionCommitState* state = reinterpret_cast<TransactionCommitState*>(data);
			executeLogWork(state);
			executeCommitWork(state);
		},
		[](napi_env env, napi_status status, void* data) { // complete
			TransactionCommitState* state = reinterpret_cast<TransactionCommitState*>(data);

			DEBUG_LOG("%p Transaction::Commit Complete callback entered (status=%d, txnId=%d)\n",
				state->handle.get(), status, state->handle ? state->handle->id : 0);

			state->deleteAsyncWork();

			// only process result if the work wasn't cancelled
			if (status != napi_cancelled) {
				completeCommitWork(env, state);
			}

			delete state;
		},
		state,     // data
		&state->asyncWork // -> result
	));

	// register the async work with the transaction handle
	(*txnHandle)->registerAsyncWork();

	NAPI_STATUS_THROWS(::napi_queue_async_work(env, state->asyncWork));

	NAPI_RETURN_UNDEFINED();
}

/**
 * Commits the transaction synchronously.
 */
napi_value Transaction::CommitSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_TRANSACTION_HANDLE("CommitSync");

	TransactionState txnState = (*txnHandle)->state;
	if (txnState == TransactionState::Aborted) {
		NAPI_THROW_JS_ERROR("ERR_ALREADY_ABORTED", "Transaction has already been aborted");
	}
	if (txnState == TransactionState::Committing || txnState == TransactionState::Committed) {
		NAPI_RETURN_UNDEFINED();
	}
	(*txnHandle)->state = TransactionState::Committing;

	std::shared_ptr<TransactionLogStore> store = nullptr;
	bool hasLog = false;

	if ((*txnHandle)->logEntryBatch) {
		DEBUG_LOG("%p Transaction::CommitSync Committing log entries for transaction %u\n",
			(*txnHandle).get(), (*txnHandle)->id);
		store = (*txnHandle)->boundLogStore.lock();
		if (store) {
			hasLog = true;
			store->writeBatch(*(*txnHandle)->logEntryBatch, (*txnHandle)->committedPosition);
			// free the batch after writing to avoid memory leak
			(*txnHandle)->logEntryBatch.reset();
		} else {
			DEBUG_LOG("%p Transaction::CommitSync ERROR: Log store not found for transaction %u\n", (*txnHandle).get(), (*txnHandle)->id);
			NAPI_THROW_JS_ERROR("ERR_LOG_STORE_NOT_FOUND", "Log store not found for transaction");
		}
	}

	rocksdb::Status status = (*txnHandle)->txn->Commit();

	if (!(*txnHandle)->lockedVTSlots.empty()) {
		(*txnHandle)->releaseIntent();
	}

	// Publish only on a real commit; IsBusy/TryAgain defer to the retry's eventual success, and on
	// a hard error close()'s commitAborted stops the position pinning the watermark. See the async
	// Commit path for detail.
	if ((*txnHandle)->committedPosition.logSequenceNumber > 0 && status.ok()) {
		if (!store) {
			store = (*txnHandle)->boundLogStore.lock();
		}
		if (store) {
			store->commitFinished((*txnHandle)->committedPosition, (*txnHandle)->dbHandle->descriptor->db->GetLatestSequenceNumber());
		} else {
			DEBUG_LOG("%p Transaction::Commit ERROR: Log store not found for transaction, log number: %u id: %u\n", (*txnHandle).get(), (*txnHandle)->committedPosition.logSequenceNumber, (*txnHandle)->id);
			status = rocksdb::Status::Aborted("Log store not found for transaction");
		}
	}

	if (status.ok()) {
		DEBUG_LOG("%p Transaction::CommitSync Emitted committed event (txnId=%u)\n", (*txnHandle).get(), (*txnHandle)->id);
		(*txnHandle)->state = TransactionState::Committed;
		(*txnHandle)->dbHandle->descriptor->notify("committed", nullptr);

		DEBUG_LOG("%p Transaction::CommitSync Closing transaction (txnId=%u)\n", (*txnHandle).get(), (*txnHandle)->id);
		(*txnHandle)->close();
	} else {
		if (status.IsBusy() || status.IsTryAgain()) {
			DEBUG_LOG("%p Transaction::CommitSync ERROR: Commit failed with %s, resetting transaction\n",
				(*txnHandle).get(), status.IsBusy() ? "IsBusy" : "TryAgain");
			// Reset onto a fresh snapshot so the retry re-drives the commit against current state
			// (committedPosition survives, keeping the WAL write-once, #668). See async Commit.
			(*txnHandle)->resetTransaction();
		}
		if ((*txnHandle)->state == TransactionState::Committing) {
			(*txnHandle)->state = TransactionState::Pending;
		}
		napi_value error;
		ROCKSDB_CREATE_ERROR_LIKE_VOID(error, status, "Transaction commit failed");
		napi_value hasLogValue;
		// #668: writeBatch is skipped on an IsBusy/TryAgain retry (WAL already durable), so fall
		// back to committedPosition so the caller keeps treating this as a logged txn.
		NAPI_STATUS_THROWS(::napi_get_boolean(env,
			hasLog || (*txnHandle)->committedPosition.logSequenceNumber > 0, &hasLogValue));
		NAPI_STATUS_THROWS(::napi_set_named_property(env, error, "hasLog", hasLogValue));
		NAPI_STATUS_THROWS(::napi_throw(env, error));
	}

	NAPI_RETURN_UNDEFINED();
}

/**
 * Asynchronously gets a value through the transaction. The first argument, that specifies the key, can be a buffer or a number
 * indicating the length of the key that was written to the shared buffer.
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * const txn = new NativeTransaction(db);
 * const value = await txn.get('foo');
 * ```
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * const txn = new NativeTransaction(db);
 * const b = Buffer.alloc(1024);
 * db.setDefaultKeyBuffer(b);
 * b.utf8Write('foo');
 * const value = await txn.get(3);
 * ```
 */
napi_value Transaction::Get(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(5);
	napi_value resolve = argv[1];
	napi_value reject = argv[2];
	UNWRAP_TRANSACTION_HANDLE("Get");
	UNWRAP_DB_HANDLE_AND_OPEN();
	rocksdb::Slice keySlice;
	if (!rocksdb_js::getSliceFromArg(env, argv[0], keySlice, (*txnHandle)->dbHandle->defaultKeyBufferPtr, "Key must be a buffer")) {
		return nullptr;
	}
	// storing in std::string so it can live through the async process
	std::string key(keySlice.data(), keySlice.size());

	// argv[3]: txnId (ignored — the transaction is self)
	// argv[4]: optional expectedVersion for VT check and populate
	bool hasExpectedVersion = false;
	uint64_t expectedVersion = 0;
	if (argc >= 5) {
		hasExpectedVersion = parseExpectedVersion(env, argv[4], expectedVersion);
	}

	std::atomic<uint64_t>* vtSlot = nullptr;
	uint64_t vtObserved = 0;
	if (hasExpectedVersion) {
		vtSlot = vtSlotFor((*txnHandle)->dbHandle, DBSettings::getInstance().getVerificationTableRaw(), keySlice);
		if (vtSlot) vtObserved = vtSlot->load(std::memory_order_acquire);
	}

	return (*txnHandle)->get(env, key, resolve, reject, nullptr, vtSlot, vtObserved, hasExpectedVersion, expectedVersion);
}

/**
 * Gets the number of keys within a range or in the entire RocksDB database.
 *
 * @example
 * ```typescript
 * const txn = new NativeTransaction(db);
 * const total = txn.getCount();
 * const range = txn.getCount({ start: 'a', end: 'z' });
 * ```
 */
napi_value Transaction::GetCount(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);
	UNWRAP_TRANSACTION_HANDLE("GetCount");

	DBIteratorOptions itOptions;
	itOptions.initFromNapiObject(env, argv[0]);
	itOptions.values = false;

	uint64_t count = 0;
	(*txnHandle)->getCount(itOptions, count);

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_int64(env, count, &result));
	return result;
}

/**
 * Synchronously gets a value through the transaction. The first argument, that specifies the key, can be a buffer or a number
 * indicating the length of the key that was written to the shared buffer.
 *
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * const txn = new NativeTransaction(db);
 * const value = txn.getSync('foo');
 * ```
 * @example
 * ```typescript
 * const db = new NativeDatabase();
 * const txn = new NativeTransaction(db);
 * const b = Buffer.alloc(1024);
 * db.setDefaultKeyBuffer(b);
 * b.utf8Write('foo');
 * const value = txn.getSync(3);
 * ```
 */
napi_value Transaction::GetSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(4);
	UNWRAP_TRANSACTION_HANDLE("GetSync");
	rocksdb::Slice keySlice;
	if (!rocksdb_js::getSliceFromArg(env, argv[0], keySlice, (*txnHandle)->dbHandle->defaultKeyBufferPtr, "Key must be a buffer")) {
		return nullptr;
	}
	int32_t flags;
	NAPI_STATUS_THROWS(::napi_get_value_int32(env, argv[1], &flags));
	// argv[2]: txnId (ignored — the transaction is self)
	// argv[3]: optional expectedVersion for VT check and populate
	bool hasExpectedVersion = false;
	uint64_t expectedVersion = 0;
	if (argc >= 4) {
		hasExpectedVersion = parseExpectedVersion(env, argv[3], expectedVersion);
	}

	bool wantsPopulate = (flags & POPULATE_VERSION_FLAG) != 0;

	// Establish the transaction snapshot BEFORE loading the VT slot.
	// If we loaded the slot first, a complete write cycle (lock → commit →
	// settle) landing in the window between the slot load and the snapshot-set
	// could let us observe V_old in the slot, pass the fast-path check, and
	// then pin a snapshot that already sees V_new — a torn view where the VT
	// says FRESH for V_old but the transaction's snapshot is post-V_new.
	// Pinning the snapshot first ensures that the write cycle's lock clears
	// the slot before our load, so a FRESH hit is consistent with the snapshot.
	(*txnHandle)->ensureSnapshot();

	std::atomic<uint64_t>* vtSlot = nullptr;
	// Observe the slot after the snapshot is established; reused for the
	// fast-path check and the post-read conditional CAS.
	uint64_t vtObserved = 0;
	if (hasExpectedVersion || wantsPopulate) {
		vtSlot = vtSlotFor((*txnHandle)->dbHandle, DBSettings::getInstance().getVerificationTableRaw(), keySlice);
		if (vtSlot) vtObserved = vtSlot->load(std::memory_order_acquire);
	}

	// VT fast-path: caller-supplied version matches the table → return FRESH
	if (vtSlot && hasExpectedVersion && vtObserved == expectedVersion) {
		// Snapshot already established above; no further action needed.
		napi_value result;
		NAPI_STATUS_THROWS(::napi_create_int32(env, FRESH_VERSION_FLAG, &result));
		return result;
	}

	rocksdb::PinnableSlice value;
	rocksdb::ReadOptions readOptions;
	if (flags & ONLY_IF_IN_MEMORY_CACHE_FLAG) {
		readOptions.read_tier = rocksdb::kBlockCacheTier;
	}
	rocksdb::Status status = (*txnHandle)->getSync(keySlice, value, readOptions);

	if (status.IsNotFound()) {
		NAPI_RETURN_UNDEFINED();
	}

	if (!status.ok()) {
		::napi_throw_error(env, nullptr, status.ToString().c_str());
		return nullptr;
	}

	napi_value result;
	if (status.IsIncomplete()) {
		NAPI_STATUS_THROWS(::napi_create_int32(env, NOT_IN_MEMORY_CACHE_FLAG, &result));
		return result;
	}

	// Seed the slot with the key's LATEST committed version, gated so it only
	// becomes cacheable when that latest version is the single accessible value —
	// so a transactional read at a stale snapshot can't publish a stale version,
	// while a settled transactional read does seed the cache. Passing this
	// transaction's read snapshot lets vtPopulateIfSettled skip a redundant
	// latest-read when the snapshot is current (the common no-concurrent-write
	// case) and re-read only when the snapshot is behind a newer write.
	if (vtSlot && (wantsPopulate || hasExpectedVersion)) {
		uint64_t extracted = VerificationTable::extractVersionFromValue(value);
		const rocksdb::Snapshot* readSnapshot = (*txnHandle)->readSnapshot();
		if (hasExpectedVersion && extracted == expectedVersion) {
			// Soft VT miss confirmed fresh: value carries the caller's expected version.
			vtPopulateIfSettled((*txnHandle)->dbHandle, vtSlot, keySlice, extracted, readSnapshot, vtObserved);
			NAPI_STATUS_THROWS(::napi_create_int32(env, FRESH_VERSION_FLAG, &result));
			return result;
		}
		vtPopulateIfSettled((*txnHandle)->dbHandle, vtSlot, keySlice, extracted, readSnapshot, vtObserved);
	}

	if (!(flags & ALWAYS_CREATE_NEW_BUFFER_FLAG) &&
			(*txnHandle)->dbHandle->defaultValueBufferPtr != nullptr &&
			value.size() <= (*txnHandle)->dbHandle->defaultValueBufferLength) {
		// if it fits in the default value buffer, copy the data and just return the length
		::memcpy((*txnHandle)->dbHandle->defaultValueBufferPtr, value.data(), value.size());
		NAPI_STATUS_THROWS(::napi_create_int32(env, value.size(), &result));
		return result;
	}

	NAPI_STATUS_THROWS(::napi_create_buffer_copy(
		env,
		value.size(),
		value.data(),
		nullptr,
		&result
	));

	return result;
}

/**
 * Retrieves the timestamp of the transaction in milliseconds.
 */
napi_value Transaction::GetTimestamp(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_TRANSACTION_HANDLE("GetTimestamp");

	napi_value result;
	NAPI_STATUS_THROWS_ERROR(::napi_create_double(env, (*txnHandle)->startTimestamp, &result), "Failed to get timestamp");
	return result;
}

/**
 * Retrieves the ID of the transaction.
 */
napi_value Transaction::Id(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_TRANSACTION_HANDLE("Id");

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_uint32(
		env,
		(*txnHandle)->id,
		&result
	));
	return result;
}

/**
 * Puts a value for the given key.
 */
napi_value Transaction::PutSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2);
	NAPI_GET_BUFFER(argv[0], key, "Key is required");
	NAPI_GET_BUFFER(argv[1], value, nullptr);
	UNWRAP_TRANSACTION_HANDLE("Put");
	// THROW_IF_READONLY((*txnHandle)->dbHandle->descriptor, "Put failed: ");

	rocksdb::Slice keySlice(key + keyStart, keyEnd - keyStart);
	rocksdb::Slice valueSlice(value + valueStart, valueEnd - valueStart);

	DEBUG_LOG("%p Transaction::PutSync key:", txnHandle->get());
	DEBUG_LOG_KEY_LN(keySlice);

	DEBUG_LOG("%p Transaction::PutSync value:", txnHandle->get());
	DEBUG_LOG_KEY_LN(valueSlice);

	ROCKSDB_STATUS_THROWS_ERROR_LIKE((*txnHandle)->putSync(keySlice, valueSlice), "Transaction put failed");

	NAPI_RETURN_UNDEFINED();
}

/**
 * Removes a value for the given key.
 */
napi_value Transaction::RemoveSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);
	NAPI_GET_BUFFER(argv[0], key, "Key is required");
	UNWRAP_TRANSACTION_HANDLE("Remove");
	// THROW_IF_READONLY((*txnHandle)->dbHandle->descriptor, "Remove failed: ");

	rocksdb::Slice keySlice(key + keyStart, keyEnd - keyStart);

	ROCKSDB_STATUS_THROWS_ERROR_LIKE((*txnHandle)->removeSync(keySlice), "Transaction remove failed");

	NAPI_RETURN_UNDEFINED();
}

/**
 * Sets the timestamp of the transaction.
 */
napi_value Transaction::SetTimestamp(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);
	UNWRAP_TRANSACTION_HANDLE("SetTimestamp");

	napi_valuetype type;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[0], &type));

	if (type == napi_undefined) {
		// use current timestamp
		(*txnHandle)->startTimestamp = rocksdb_js::getMonotonicTimestamp();
	} else if (type == napi_number) {
		double timestampMs = 0.0;
		NAPI_STATUS_THROWS_ERROR(::napi_get_value_double(env, argv[0], &timestampMs),
			"Invalid timestamp, expected positive number");
		if (timestampMs <= 0) {
			::napi_throw_error(env, nullptr, "Invalid timestamp, expected positive number");
			return nullptr;
		}
		(*txnHandle)->startTimestamp = timestampMs;
	} else {
		::napi_throw_error(env, nullptr, "Invalid timestamp, expected positive number");
		return nullptr;
	}

	NAPI_RETURN_UNDEFINED();
}

/**
 * Creates a new transaction log instance bound to this transaction.
 */
napi_value Transaction::UseLog(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);
	NAPI_GET_STRING(argv[0], name, "Name is required");
	UNWRAP_TRANSACTION_HANDLE("UseLog");

	// check if transaction is already bound to a different log store
	auto boundStore = (*txnHandle)->boundLogStore.lock();
	if (boundStore && boundStore->name != name) {
		std::string errorMessage = "Transaction " + std::to_string((*txnHandle)->id) + " is already bound to the log store \"" + boundStore->name + "\"";
		::napi_throw_error(env, nullptr, errorMessage.c_str());
		return nullptr;
	}

	// resolve the store and bind if not already bound
	std::shared_ptr<TransactionLogStore> store;
	try {
		store = (*txnHandle)->dbHandle->descriptor->resolveTransactionLogStore(name);
	} catch (const std::runtime_error& e) {
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}
	if (!boundStore) {
		// Bind under transactionBindMutex so the bind+increment is atomic with
		// respect to tryClose()'s phase-3 check-and-mark-closing sequence.
		// transactionBindMutex is never held during I/O, so this cannot stall
		// the event loop the way holding writeMutex here would.
		std::lock_guard<std::mutex> lock(store->transactionBindMutex);
		if (store->isClosing.load(std::memory_order_relaxed)) {
			::napi_throw_error(env, nullptr, "Transaction log store is closed");
			return nullptr;
		}
		(*txnHandle)->boundLogStore = store;
		store->pendingTransactionCount++;
		DEBUG_LOG("%p Transaction::UseLog Binding transaction %u to log store \"%s\"\n",
			(*txnHandle).get(), (*txnHandle)->id, name.c_str());
	}

	// this needs to create a new TransactionLog instance that is not tracked by
	// the DBHandle and is bound to this transaction
	napi_value exports;
	NAPI_STATUS_THROWS_ERROR(::napi_get_reference_value(env, (*txnHandle)->dbHandle->exportsRef, &exports), "Failed to get 'exports' reference");

	napi_value transactionLogCtor;
	NAPI_STATUS_THROWS_ERROR(::napi_get_named_property(env, exports, "TransactionLog", &transactionLogCtor), "Failed to get 'TransactionLog' constructor");

	napi_value jsDatabase;
	NAPI_STATUS_THROWS_ERROR(::napi_get_reference_value(env, (*txnHandle)->jsDatabaseRef, &jsDatabase), "Failed to get 'jsDatabase' reference");

	napi_value args[3];
	args[0] = jsDatabase;

	NAPI_STATUS_THROWS_ERROR(::napi_create_string_utf8(env, name.c_str(), name.size(), &args[1]), "Invalid log name");
	NAPI_STATUS_THROWS_ERROR(::napi_create_uint32(env, (*txnHandle)->id, &args[2]), "Failed to create transaction id argument");

	napi_value instance;
	NAPI_STATUS_THROWS_ERROR(::napi_new_instance(env, transactionLogCtor, 3, args, &instance), "Failed to create new TransactionLog instance");

	return instance;
}

/**
 * Initializes the `NativeTransaction` JavaScript class.
 */
void Transaction::Init(napi_env env, napi_value exports) {
	napi_property_descriptor properties[] = {
		{ "abort", nullptr, Abort, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "commit", nullptr, Commit, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "commitSync", nullptr, CommitSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "get", nullptr, Get, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getCount", nullptr, GetCount, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getSync", nullptr, GetSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getTimestamp", nullptr, GetTimestamp, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "id", nullptr, nullptr, Id, nullptr, nullptr, napi_default, nullptr },
		{ "putSync", nullptr, PutSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "removeSync", nullptr, RemoveSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "setTimestamp", nullptr, SetTimestamp, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "useLog", nullptr, UseLog, nullptr, nullptr, nullptr, napi_default, nullptr }
	};

	auto className = "Transaction";
	constexpr size_t len = sizeof("Transaction") - 1;

	napi_ref exportsRef;
	NAPI_STATUS_THROWS_VOID(::napi_create_reference(env, exports, 1, &exportsRef));

	napi_value ctor;
	NAPI_STATUS_THROWS_VOID(::napi_define_class(
		env,
		className,                // className
		len,                      // length of class name
		Transaction::Constructor, // constructor
		(void*)exportsRef,        // constructor arg
		sizeof(properties) / sizeof(napi_property_descriptor), // number of properties
		properties,               // properties array
		&ctor                     // [out] constructor
	));

	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, className, ctor));
}

} // namespace rocksdb_js

