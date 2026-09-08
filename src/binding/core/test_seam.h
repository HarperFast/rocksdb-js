#ifndef __CORE_TEST_SEAM_H__
#define __CORE_TEST_SEAM_H__

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <thread>

// Deterministic test seams that widen a race window are gated on a millisecond
// delay read from an environment variable (0 = disabled). They are inert in
// production where the env var is unset.
//
// Pass the env var name to testDelayMs() at the call site; see
// EventEmitter::notify, TransactionHandle::close, and TransactionHandle::get
// for usage.
inline int testDelayMs(const char* envName) {
	const char* value = ::getenv(envName);
	return value ? ::atoi(value) : 0;
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

// Ordering seams for the column-family commit gate (test/drop-commit-gate.test.ts). A delay can
// only widen a race window; these let a test *observe* that a commit was admitted or a drop began,
// and park an admitted commit until released, so the commit-wins / drop-wins orderings are asserted
// rather than hoped for. Process-global like forceTryAgainCounter (shared across worker_threads);
// set from JS via the binding's `setCommitHoldForTesting` / `getCommitGateCountersForTesting`.
inline std::atomic<uint64_t>& commitAdmittedCounter() {
	static std::atomic<uint64_t> counter{0};
	return counter;
}

inline std::atomic<uint64_t>& dropBeginCounter() {
	static std::atomic<uint64_t> counter{0};
	return counter;
}

inline std::atomic<bool>& commitHoldFlag() {
	static std::atomic<bool> flag{false};
	return flag;
}

// Parks an admitted commit while the hold flag is set. Bounded so a test that forgets to release
// cannot wedge the commit lane past its own timeout.
inline void testHoldAdmittedCommit() {
	for (int i = 0; i < 30000 && commitHoldFlag().load(std::memory_order_acquire); ++i) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
}

// Number of upcoming successful DropColumnFamily calls to report as failed AFTER RocksDB has
// removed the family — the shape of an OPTIONS-file persistence error, where the family is gone
// but the caller sees an error. Set via the binding's `forceDropFailureForTesting(n)`; 0 = inert.
inline std::atomic<int>& forceDropFailureCounter() {
	static std::atomic<int> counter{0};
	return counter;
}

inline bool testForceDropFailure() {
	int cur = forceDropFailureCounter().load(std::memory_order_relaxed);
	while (cur > 0) {
		if (forceDropFailureCounter().compare_exchange_weak(cur, cur - 1, std::memory_order_relaxed)) {
			return true;
		}
	}
	return false;
}

#endif
