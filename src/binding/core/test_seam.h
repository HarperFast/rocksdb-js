#ifndef __CORE_TEST_SEAM_H__
#define __CORE_TEST_SEAM_H__

#include <atomic>
#include <cstdlib>

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

// Test-only latch for DBRegistry::DropColumnFamily, armed from JS rather than an env var for the
// same reason as forceTryAgainCounter() above. When armed, a successful drop parks between the
// RocksDB drop and the registry cleanup — exactly where a warm DBRegistry::OpenDB would slip in
// if that section ever released databasesMutex early — until an open has reached that mutex, and
// then for this many further milliseconds. Anchoring the hold to the opener's arrival rather than
// to a timer is what makes the interleaving a fact instead of an assumption. 0 = inert, and the
// counters below are then maintained by neither side.
inline std::atomic<int>& dropColumnFamilyDelayMs() {
	static std::atomic<int> ms{0};
	return ms;
}

// Incremented as an armed drop enters the latch, so a test thread knows when the drop is provably
// inside the critical section, and again once that drop has seen an opener reach the registry
// mutex, so the test can assert the interleaving happened rather than hoping it did.
inline std::atomic<uint32_t>& dropColumnFamilyLatchEntered() {
	static std::atomic<uint32_t> count{0};
	return count;
}

inline std::atomic<uint32_t>& dropColumnFamilyLatchObservedOpen() {
	static std::atomic<uint32_t> count{0};
	return count;
}

// Incremented by DBRegistry::OpenDB immediately before it acquires databasesMutex, but only while
// the latch is armed — production pays one relaxed load per open.
inline std::atomic<uint32_t>& openDbMutexAttempts() {
	static std::atomic<uint32_t> count{0};
	return count;
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
