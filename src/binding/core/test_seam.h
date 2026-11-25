#ifndef __CORE_TEST_SEAM_H__
#define __CORE_TEST_SEAM_H__

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <thread>

// Deterministic test seams that widen a race window are gated on a millisecond
// delay read from an environment variable (0 = disabled). They are inert in
// production where the env var is unset.
//
// Read a test delay while the module is initialized. Native call sites use the
// snapshotted atomics below so they never race a JavaScript process.env update.
inline int testDelayMs(const char* envName) {
	const char* value = ::getenv(envName);
	return value ? ::atoi(value) : 0;
}

// Snapshot native fault flags once; mutating process.env while native workers
// can read it is unsafe, and process.env deletion does not update MSVC's CRT.
inline std::atomic<bool>& closeFailureFlag() {
	static std::atomic<bool> pending{ false };
	return pending;
}

// Snapshotted for the same reason as closeFailureFlag(): a fault flag must not
// be re-read from the environment on a native thread.
inline std::atomic<bool>& destroyFailureFlag() {
	static std::atomic<bool> pending{ false };
	return pending;
}

// A count, not a flag: a quarantined descriptor is only reached at process exit
// when the shutdown() retry fails too, so reproducing that state needs the
// close-time flush to fail more than once. `=1` behaves exactly as the previous
// boolean did.
inline std::atomic<int>& closeFlushFailureFlag() {
	static std::atomic<int> pending{ 0 };
	return pending;
}

// DBIterator::Next() returns one row per call, so its seam is snapshotted here
// rather than read per call: a getenv() scan per row is a measurable share of
// the per-row cost for a seam that is unset in production.
inline std::atomic<int>& iteratorNextDelayMsFlag() {
	static std::atomic<int> delayMs{ 0 };
	return delayMs;
}

inline std::atomic<bool>& transactionCommitExecuteDelayActive() {
	static std::atomic<bool> active{ false };
	return active;
}

inline std::atomic<int>& transactionCommitAdmissionDelayMs() {
	static std::atomic<int> delayMs{ 0 };
	return delayMs;
}

// Per-row delay for DBIteratorHandle::countRemaining(), snapshotted for the
// same reason.
inline std::atomic<int>& countScanDelayMsFlag() {
	static std::atomic<int> delayMs{ 0 };
	return delayMs;
}

// Iterator construction can be frequent, so its test-only delay must not
// scan the environment on every constructor call.
inline std::atomic<int>& iteratorSetupDelayMsFlag() {
	static std::atomic<int> delayMs{ 0 };
	return delayMs;
}

// Transaction close can run on any teardown thread; snapshot this seam before
// native work begins rather than reading process environment there.
inline std::atomic<int>& txnCloseDelayMsFlag() {
	static std::atomic<int> delayMs{ 0 };
	return delayMs;
}

inline std::atomic<bool>& txnCloseDelayActive() {
	static std::atomic<bool> active{ false };
	return active;
}

inline std::atomic<int>& txnCloseDrainWaiting() {
	static std::atomic<int> waiting{ 0 };
	return waiting;
}

// A child-process-only latch: the first transaction close to reach it after its drain
// parks until a foreign close records how it was protected. No timer can release it.
struct TransactionCloseDrainLatch {
	enum Outcome : int {
		Parked = 0,
		GuardedAtDrain,
		AwaitedByClose,
		// finishClose() passed its closables sweep with the close parked or unfinished.
		Missed,
	};
	std::atomic<bool> enabled{ false };
	std::atomic<void*> handle{ nullptr };
	std::atomic<void*> descriptor{ nullptr };
	std::atomic<int> outcome{ Parked };
	std::atomic<bool> finished{ false };

	bool release(const std::atomic<void*>& field, void* owner, Outcome result) {
		int expected = Parked;
		if (owner == nullptr || field.load(std::memory_order_acquire) != owner ||
			!this->outcome.compare_exchange_strong(expected, result, std::memory_order_acq_rel)) {
			return false;
		}
		this->outcome.notify_all();
		return true;
	}
};

inline TransactionCloseDrainLatch& txnCloseDrainLatch() {
	static TransactionCloseDrainLatch latch;
	return latch;
}

inline bool isTransactionCloseAwaited() {
	const int outcome = txnCloseDrainLatch().outcome.load(std::memory_order_acquire);
	return outcome == TransactionCloseDrainLatch::GuardedAtDrain || outcome == TransactionCloseDrainLatch::AwaitedByClose;
}

// Transactional reads execute on a libuv worker, so their test delay cannot
// read the process environment concurrently with JavaScript.
inline std::atomic<int>& txnGetDelayMsFlag() {
	static std::atomic<int> delayMs{ 0 };
	return delayMs;
}

// Commit execution can run on a worker or the dedicated commit thread.
inline std::atomic<int>& commitExecuteDelayMsFlag() {
	static std::atomic<int> delayMs{ 0 };
	return delayMs;
}

inline std::atomic<bool>& commitThreadStartFailureFlag() {
	static std::atomic<bool> failing{ false };
	return failing;
}

inline std::atomic<int>& commitDispatchFailureFlag() {
	static std::atomic<int> pending{ 0 };
	return pending;
}

inline bool testConsumeCount(std::atomic<int>& counter) {
	int cur = counter.load(std::memory_order_relaxed);
	while (cur > 0) {
		if (counter.compare_exchange_weak(cur, cur - 1, std::memory_order_relaxed)) {
			return true;
		}
	}
	return false;
}

inline bool testConsumeCommitDispatchFailure() {
	return testConsumeCount(commitDispatchFailureFlag());
}

inline std::atomic<int>& commitDispatchCallFailureFlag() {
	static std::atomic<int> pending{ 0 };
	return pending;
}

// Completion tsfn calls that succeed before the injected failures start.
inline std::atomic<int>& commitDispatchCallFailureSkipFlag() {
	static std::atomic<int> pending{ 0 };
	return pending;
}

inline bool testConsumeCommitDispatchCallFailure() {
	if (commitDispatchCallFailureFlag().load(std::memory_order_relaxed) <= 0 ||
		testConsumeCount(commitDispatchCallFailureSkipFlag())) {
		return false;
	}
	return testConsumeCount(commitDispatchCallFailureFlag());
}

// Upper bound, in milliseconds, that a cancellable manual compactRange() parks
// before handing the range to RocksDB. It returns as soon as the descriptor's
// cancel token is armed, so a fixture can hold a compaction across a foreign
// close claim without depending on how long a real compaction happens to take.
// Snapshotted here rather than read per call so the production path (unset)
// costs one relaxed load.
inline std::atomic<int>& compactCancelDelayMsFlag() {
	static std::atomic<int> delayMs{ 0 };
	return delayMs;
}

// Per-key delay inside a compaction filter installed on every column family
// only while this is non-zero, so a fixture gets a long automatic compaction
// that RocksDB can still abort per key.
inline std::atomic<int>& compactionKeyDelayMsFlag() {
	static std::atomic<int> delayMs{ 0 };
	return delayMs;
}

// Delays TransactionLogEventListener::OnFlushCompleted, which runs on a RocksDB
// flush thread after Flush() may already have returned.
inline std::atomic<int>& flushCompletedDelayMsFlag() {
	static std::atomic<int> delayMs{ 0 };
	return delayMs;
}

// A count because different flush jobs can be delayed concurrently.
inline std::atomic<int>& flushCompletedDelayActive() {
	static std::atomic<int> active{ 0 };
	return active;
}

// One-shot close failure injected after finishClose() has cancelled background
// work, so a retry has to complete without flushing again.
inline std::atomic<bool>& closeFailureAfterCancelFlag() {
	static std::atomic<bool> pending{ false };
	return pending;
}

// Delays DBRegistry::OpenDB() after it releases databasesMutex. A handle must
// already be adopted and attached, and OpenDB() must hold no reference it took
// under the lock, while this delay is observable.
inline std::atomic<int>& openAttachDelayMsFlag() {
	static std::atomic<int> delayMs{ 0 };
	return delayMs;
}

// The three teardown/backup delays below run on a libuv worker or on whichever
// thread drives teardown, so they are snapshotted for the same reason as the
// fault flags above rather than read through ::getenv there.
inline std::atomic<int>& backupDelayMsFlag() {
	static std::atomic<int> delayMs{ 0 };
	return delayMs;
}

inline std::atomic<int>& destroyDelayMsFlag() {
	static std::atomic<int> delayMs{ 0 };
	return delayMs;
}

inline std::atomic<int>& closeRetryDelayMsFlag() {
	static std::atomic<int> delayMs{ 0 };
	return delayMs;
}

// Holds an env-cleanup hook's registry snapshot, so a last-handle close on another thread lands
// while it pins the descriptor.
inline std::atomic<int>& envCleanupPinDelayMsFlag() {
	static std::atomic<int> delayMs{ 0 };
	return delayMs;
}

// Holds a withLock() completion's descriptor reference, so a last-handle close on another
// thread lands while it is pinned.
inline std::atomic<int>& lockCompleteDelayMsFlag() {
	static std::atomic<int> delayMs{ 0 };
	return delayMs;
}

// Per-column-family delay inside registryStatus()'s column walk, so a foreign
// destroy()/shutdown() clearing `DBDescriptor::columns` lands in the middle of
// it. Without the columnsMutex snapshot that walk is a use-after-free on the
// freed map node's key.
inline std::atomic<int>& registryStatusColumnsDelayMsFlag() {
	static std::atomic<int> delayMs{ 0 };
	return delayMs;
}

// Delays DBHandle::clear() after it deletes the family's files and before it
// returns. clear() runs on a libuv worker for the async API.
inline std::atomic<int>& clearDeleteDelayMsFlag() {
	static std::atomic<int> delayMs{ 0 };
	return delayMs;
}

// Delays TransactionLog::GetMemoryMapOfFile() after it takes its mapping reference and before it
// creates the JS buffer, so a worker terminate() lands in between and the create fails.
inline std::atomic<int>& memoryMapBufferDelayMsFlag() {
	static std::atomic<int> delayMs{ 0 };
	return delayMs;
}

// Parked commits a retired completion's finalizer closed without JS because
// their env was gone. Never reset.
inline std::atomic<uint64_t>& commitFinalizerSkippedDeliveries() {
	static std::atomic<uint64_t> skipped{ 0 };
	return skipped;
}

// Failed native mapping-buffer creates. Never reset.
inline std::atomic<uint64_t>& memoryMapBufferCreateFailures() {
	static std::atomic<uint64_t> failures{ 0 };
	return failures;
}

// Number of transaction-log abort-marker syncs to fail after the marker is written (a count). Read
// on the commit and teardown threads that write markers, so snapshotted like the fault flags above.
inline std::atomic<int>& abortMarkFailureFlag() {
	static std::atomic<int> pending{ 0 };
	return pending;
}

inline bool testConsumeAbortMarkFailure() {
	int cur = abortMarkFailureFlag().load(std::memory_order_relaxed);
	while (cur > 0) {
		if (abortMarkFailureFlag().compare_exchange_weak(cur, cur - 1, std::memory_order_relaxed)) {
			return true;
		}
	}
	return false;
}

// A count, like abortMarkFailureFlag(): WAL syncs to fail with an injected I/O error.
inline std::atomic<int>& syncWalFailureFlag() {
	static std::atomic<int> pending{ 0 };
	return pending;
}

inline bool testConsumeSyncWalFailure() {
	int cur = syncWalFailureFlag().load(std::memory_order_relaxed);
	while (cur > 0) {
		if (syncWalFailureFlag().compare_exchange_weak(cur, cur - 1, std::memory_order_relaxed)) {
			return true;
		}
	}
	return false;
}

inline std::atomic<bool>& prelogConflictCheckDisabled() {
	static std::atomic<bool> disabled{ false };
	return disabled;
}

inline std::atomic<bool>& prelogConflictCheckRetriesOnly() {
	static std::atomic<bool> retriesOnly{ true };
	return retriesOnly;
}

// The values ROCKSDB_JS_PRELOG_CHECK selected at module load, which resetting the test switches
// restores instead of the compiled-in defaults.
inline std::atomic<bool>& prelogConflictCheckDisabledDefault() {
	static std::atomic<bool> disabled{ false };
	return disabled;
}

inline std::atomic<bool>& prelogConflictCheckRetriesOnlyDefault() {
	static std::atomic<bool> retriesOnly{ true };
	return retriesOnly;
}

inline void initializeTestSeams() {
	static std::once_flag initialized;
	std::call_once(initialized, []() {
		const char* value = ::getenv("ROCKSDB_JS_CLOSE_FAILURE");
		closeFailureFlag().store(value && ::atoi(value) > 0, std::memory_order_relaxed);
		value = ::getenv("ROCKSDB_JS_DESTROY_FAILURE");
		destroyFailureFlag().store(value && ::atoi(value) > 0, std::memory_order_relaxed);
		value = ::getenv("ROCKSDB_JS_CLOSE_FLUSH_FAILURE");
		closeFlushFailureFlag().store(value ? ::atoi(value) : 0, std::memory_order_relaxed);
		value = ::getenv("ROCKSDB_JS_CLOSE_FAILURE_AFTER_CANCEL");
		closeFailureAfterCancelFlag().store(value && ::atoi(value) > 0, std::memory_order_relaxed);
		value = ::getenv("ROCKSDB_JS_PRELOG_CHECK");
		if (value && ::strcmp(value, "off") == 0) {
			prelogConflictCheckDisabledDefault().store(true, std::memory_order_relaxed);
		} else if (value && ::strcmp(value, "always") == 0) {
			prelogConflictCheckRetriesOnlyDefault().store(false, std::memory_order_relaxed);
		}
		prelogConflictCheckDisabled().store(
			prelogConflictCheckDisabledDefault().load(std::memory_order_relaxed), std::memory_order_relaxed
		);
		prelogConflictCheckRetriesOnly().store(
			prelogConflictCheckRetriesOnlyDefault().load(std::memory_order_relaxed), std::memory_order_relaxed
		);
		value = ::getenv("ROCKSDB_JS_ABORT_MARK_FAILURE");
		abortMarkFailureFlag().store(value ? ::atoi(value) : 0, std::memory_order_relaxed);
		value = ::getenv("ROCKSDB_JS_SYNC_WAL_FAILURE");
		syncWalFailureFlag().store(value ? ::atoi(value) : 0, std::memory_order_relaxed);
		iteratorNextDelayMsFlag().store(
			testDelayMs("ROCKSDB_JS_ITERATOR_NEXT_DELAY_MS"), std::memory_order_relaxed
		);
		countScanDelayMsFlag().store(
			testDelayMs("ROCKSDB_JS_COUNT_DELAY_MS"), std::memory_order_relaxed
		);
		iteratorSetupDelayMsFlag().store(
			testDelayMs("ROCKSDB_JS_ITERATOR_SETUP_DELAY_MS"), std::memory_order_relaxed
		);
		txnCloseDelayMsFlag().store(
			testDelayMs("ROCKSDB_JS_TXN_CLOSE_DELAY_MS"), std::memory_order_relaxed
		);
		txnCloseDrainLatch().enabled.store(
			testDelayMs("ROCKSDB_JS_TXN_CLOSE_DRAIN_LATCH") > 0, std::memory_order_relaxed
		);
		txnGetDelayMsFlag().store(
			testDelayMs("ROCKSDB_JS_TXN_GET_DELAY_MS"), std::memory_order_relaxed
		);
		commitExecuteDelayMsFlag().store(
			testDelayMs("ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS"), std::memory_order_relaxed
		);
		commitThreadStartFailureFlag().store(
			testDelayMs("ROCKSDB_JS_COMMIT_THREAD_START_FAILURE") > 0, std::memory_order_relaxed
		);
		commitDispatchFailureFlag().store(
			testDelayMs("ROCKSDB_JS_COMMIT_DISPATCH_FAILURE"), std::memory_order_relaxed
		);
		commitDispatchCallFailureFlag().store(
			testDelayMs("ROCKSDB_JS_COMMIT_DISPATCH_CALL_FAILURE"), std::memory_order_relaxed
		);
		commitDispatchCallFailureSkipFlag().store(
			testDelayMs("ROCKSDB_JS_COMMIT_DISPATCH_CALL_FAILURE_SKIP"), std::memory_order_relaxed
		);
		compactCancelDelayMsFlag().store(
			testDelayMs("ROCKSDB_JS_COMPACT_DELAY_MS"), std::memory_order_relaxed
		);
		compactionKeyDelayMsFlag().store(
			testDelayMs("ROCKSDB_JS_COMPACTION_KEY_DELAY_MS"), std::memory_order_relaxed
		);
		flushCompletedDelayMsFlag().store(
			testDelayMs("ROCKSDB_JS_FLUSH_COMPLETED_DELAY_MS"), std::memory_order_relaxed
		);
		openAttachDelayMsFlag().store(
			testDelayMs("ROCKSDB_JS_OPEN_ATTACH_DELAY_MS"), std::memory_order_relaxed
		);
		backupDelayMsFlag().store(
			testDelayMs("ROCKSDB_JS_BACKUP_DELAY_MS"), std::memory_order_relaxed
		);
		destroyDelayMsFlag().store(
			testDelayMs("ROCKSDB_JS_DESTROY_DELAY_MS"), std::memory_order_relaxed
		);
		closeRetryDelayMsFlag().store(
			testDelayMs("ROCKSDB_JS_CLOSE_RETRY_DELAY_MS"), std::memory_order_relaxed
		);
		registryStatusColumnsDelayMsFlag().store(
			testDelayMs("ROCKSDB_JS_REGISTRY_STATUS_COLUMNS_DELAY_MS"), std::memory_order_relaxed
		);
		envCleanupPinDelayMsFlag().store(
			testDelayMs("ROCKSDB_JS_ENV_CLEANUP_PIN_DELAY_MS"), std::memory_order_relaxed
		);
		lockCompleteDelayMsFlag().store(
			testDelayMs("ROCKSDB_JS_LOCK_COMPLETE_DELAY_MS"), std::memory_order_relaxed
		);
		clearDeleteDelayMsFlag().store(
			testDelayMs("ROCKSDB_JS_CLEAR_DELAY_MS"), std::memory_order_relaxed
		);
		memoryMapBufferDelayMsFlag().store(
			testDelayMs("ROCKSDB_JS_MEMORY_MAP_BUFFER_DELAY_MS"), std::memory_order_relaxed
		);
	});
}

inline bool testConsumeCloseFailure() {
	return closeFailureFlag().exchange(false, std::memory_order_relaxed);
}

inline bool testConsumeCloseFailureAfterCancel() {
	return closeFailureAfterCancelFlag().exchange(false, std::memory_order_relaxed);
}

inline bool testConsumeCloseFlushFailure() {
	int cur = closeFlushFailureFlag().load(std::memory_order_relaxed);
	while (cur > 0) {
		if (closeFlushFailureFlag().compare_exchange_weak(cur, cur - 1, std::memory_order_relaxed)) {
			return true;
		}
	}
	return false;
}

inline std::atomic<bool>& transactionCommitAdmissionDelayActive() {
	static std::atomic<bool> active{ false };
	return active;
}

inline std::atomic<bool>& transactionCommitAdmissionDelayRegistered() {
	static std::atomic<bool> registered{ false };
	return registered;
}

inline void setTransactionCommitAdmissionDelayForTesting(int delayMs, bool afterRegistration = false) {
	// The phase is set before arming and cleared after disarming, so a delay never meets a stale phase.
	if (delayMs != 0) {
		transactionCommitAdmissionDelayRegistered().store(afterRegistration, std::memory_order_release);
		transactionCommitAdmissionDelayMs().store(delayMs, std::memory_order_release);
		return;
	}
	transactionCommitAdmissionDelayMs().store(0, std::memory_order_release);
	transactionCommitAdmissionDelayRegistered().store(false, std::memory_order_release);
	transactionCommitAdmissionDelayActive().store(false, std::memory_order_release);
}

// Deterministic one-shot(-per-N) seam for the stranded-snapshot retry path: forces the next N
// transaction commits to fail with TryAgain (the caller rolls back so no data is committed),
// reproducing an ERR_TRY_AGAIN that a real memtable flush would cause but that is finicky to
// stage through the public API (it hinges on OCC memtable-history eviction). The count is read
// once from ROCKSDB_JS_FORCE_TRYAGAIN and decremented per commit; inert (returns false) when unset.
// Number of upcoming transaction commits to force-fail with TryAgain. Set from JS via the
// binding's `forceTryAgainForTesting(n)` export (env vars can't be used: the Vitest `threads`
// pool runs tests in worker_threads, whose process.env writes never reach ::getenv). Process-
// global and shared across worker threads that load this .node in the same process; 0 = inert.
inline std::atomic<int>& forceTryAgainCounter() {
	static std::atomic<int> counter{ 0 };
	return counter;
}

// Consumes one forced failure if any remain. Returns true when the caller should treat this
// commit as a stranded-snapshot TryAgain (rolling back so no data is committed).
inline bool testForceTryAgain() {
	int cur = forceTryAgainCounter().load(std::memory_order_relaxed);
	while (cur > 0) {
		if (forceTryAgainCounter().compare_exchange_weak(cur, cur - 1, std::memory_order_relaxed)) {
			return true;
		}
	}
	return false;
}

// Forces DBDescriptor::reclaimColumnFamily to report failure. 0 = inert; 1 = fail without
// calling DropColumnFamily (the family stays live for the retry); 2 = perform the real drop and
// then report failure (the retry must resolve as "already dropped"). Set from JS via
// `forceDropFailureForTesting(mode)`; process-global like forceTryAgainCounter().
inline std::atomic<int>& forceDropFailureMode() {
	static std::atomic<int> mode{ 0 };
	return mode;
}

inline int testForceDropFailureMode() {
	return forceDropFailureMode().load(std::memory_order_relaxed);
}

// Parks one selected transactional Put/Delete after its advisory retirement
// precheck so another worker can physically drop the generation first.
inline std::atomic<int>& transactionStagingDelayCountdown() {
	static std::atomic<int> countdown{ 0 };
	return countdown;
}

inline std::atomic<int>& transactionStagingDelayMs() {
	static std::atomic<int> delayMs{ 0 };
	return delayMs;
}

inline std::atomic<bool>& transactionStagingDelayActive() {
	static std::atomic<bool> active{ false };
	return active;
}

inline void setTransactionStagingDelayForTesting(int countdown, int delayMs) {
	if (countdown == 0) {
		transactionStagingDelayActive().store(false, std::memory_order_release);
	}
	transactionStagingDelayMs().store(delayMs > 60000 ? 60000 : delayMs, std::memory_order_relaxed);
	transactionStagingDelayCountdown().store(countdown, std::memory_order_relaxed);
}

inline int consumeTransactionStagingDelayForTesting() {
	int remaining = transactionStagingDelayCountdown().load(std::memory_order_relaxed);
	while (remaining > 0) {
		if (transactionStagingDelayCountdown().compare_exchange_weak(
				remaining, remaining - 1, std::memory_order_relaxed
			)) {
			return remaining == 1 ? transactionStagingDelayMs().load(std::memory_order_relaxed) : 0;
		}
	}
	return 0;
}

// Parks the next synchronous transaction read (getSync, getCount after its iterator exists, or a
// transaction iterator's setup) after it has read the transaction's database handle, so a foreign
// close can land mid-call.
inline std::atomic<int>& transactionReadDelayMs() {
	static std::atomic<int> delayMs{ 0 };
	return delayMs;
}

inline std::atomic<bool>& transactionReadDelayActive() {
	static std::atomic<bool> active{ false };
	return active;
}

inline void setTransactionReadDelayForTesting(int delayMs) {
	if (delayMs == 0) {
		transactionReadDelayActive().store(false, std::memory_order_release);
	}
	transactionReadDelayMs().store(delayMs > 60000 ? 60000 : delayMs, std::memory_order_release);
}

inline void pauseTransactionReadForTesting() {
	if (transactionReadDelayMs().load(std::memory_order_relaxed) <= 0) {
		return;
	}
	const int delayMs = transactionReadDelayMs().exchange(0, std::memory_order_acq_rel);
	if (delayMs <= 0) {
		return;
	}
	transactionReadDelayActive().store(true, std::memory_order_release);
	const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(delayMs);
	while (transactionReadDelayActive().load(std::memory_order_acquire) &&
		std::chrono::steady_clock::now() < deadline) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	transactionReadDelayActive().store(false, std::memory_order_release);
}

inline std::atomic<int>& writeBufferManagerJoinDelayCountdown() {
	static std::atomic<int> countdown{ 0 };
	return countdown;
}

inline std::atomic<int>& writeBufferManagerJoinDelayMs() {
	static std::atomic<int> delayMs{ 0 };
	return delayMs;
}

inline void setWriteBufferManagerJoinDelayForTesting(int countdown, int delayMs) {
	writeBufferManagerJoinDelayMs().store(delayMs, std::memory_order_relaxed);
	writeBufferManagerJoinDelayCountdown().store(countdown, std::memory_order_release);
}

inline int consumeWriteBufferManagerJoinDelayForTesting() {
	int remaining = writeBufferManagerJoinDelayCountdown().load(std::memory_order_acquire);
	while (remaining > 0) {
		if (writeBufferManagerJoinDelayCountdown().compare_exchange_weak(
				remaining, remaining - 1, std::memory_order_relaxed
			)) {
			return remaining == 1 ? writeBufferManagerJoinDelayMs().load(std::memory_order_relaxed) : 0;
		}
	}
	return 0;
}

enum class AsyncExceptionStage : int {
	None = 0,
	State = 1,
	Execute = 2,
	Complete = 3,
	// DBRegistry::PurgeIfUnreferenced()'s close-claim allocation.
	PurgeClaim = 4,
	// Transaction::Commit() just before dispatch, with the commit fully registered.
	CommitDispatch = 5
};

inline std::atomic<int>& asyncExceptionStage() {
	static std::atomic<int> stage{ 0 };
	return stage;
}

inline std::atomic<int>& asyncExceptionCount() {
	static std::atomic<int> count{ 0 };
	return count;
}

inline void setAsyncExceptionForTesting(int stage, int count) {
	asyncExceptionCount().store(count, std::memory_order_relaxed);
	asyncExceptionStage().store(stage, std::memory_order_relaxed);
}

inline bool consumeAsyncExceptionForTesting(AsyncExceptionStage stage) {
	if (asyncExceptionStage().load(std::memory_order_relaxed) != static_cast<int>(stage)) {
		return false;
	}
	int remaining = asyncExceptionCount().load(std::memory_order_relaxed);
	while (remaining > 0) {
		if (asyncExceptionCount().compare_exchange_weak(remaining, remaining - 1, std::memory_order_relaxed)) {
			return true;
		}
	}
	return false;
}

#endif
