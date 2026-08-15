#ifndef __CORE_TEST_SEAM_H__
#define __CORE_TEST_SEAM_H__

#include <atomic>
#include <cstdlib>
#include <mutex>

// Deterministic test seams that widen a race window are gated on a millisecond
// delay read from an environment variable (0 = disabled). They are inert in
// production where the env var is unset.
//
// Pass the env var name to testDelayMs() at the call site; see
// EventEmitter::notify, TransactionHandle::close, TransactionHandle::get, and
// DBRegistry::DestroyDB for usage.
inline int testDelayMs(const char* envName) {
	const char* value = ::getenv(envName);
	return value ? ::atoi(value) : 0;
}

// Snapshot native fault flags once; mutating process.env while native workers
// can read it is unsafe, and process.env deletion does not update MSVC's CRT.
inline std::atomic<bool>& closeFailureFlag() {
	static std::atomic<bool> pending{false};
	return pending;
}

inline void initializeTestSeams() {
	static std::once_flag initialized;
	std::call_once(initialized, []() {
		const char* value = ::getenv("ROCKSDB_JS_CLOSE_FAILURE");
		closeFailureFlag().store(value && ::atoi(value) > 0, std::memory_order_relaxed);
	});
}

inline bool testConsumeCloseFailure() {
	return closeFailureFlag().exchange(false, std::memory_order_relaxed);
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
	static std::atomic<int> counter{0};
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

#endif
