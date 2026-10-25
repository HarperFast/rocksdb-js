#ifndef __CORE_COMMIT_DIAG_H__
#define __CORE_COMMIT_DIAG_H__

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>

namespace rocksdb_js {

/**
 * Prototype measurement switch (abort-marker design, HarperFast/harper#3253):
 * - Full: every failed logged attempt is marked aborted and the retry re-logs.
 * - Hybrid: only a terminal failure of a logged batch is marked; conflict retries stay write-once.
 * - HybridIntent: Hybrid, plus a conflicted attempt (another transaction holds a write intent on
 *   one of its keys, or the pre-log check fails) is refused before the log write.
 */
enum class AbortMarkerMode { Full, Hybrid, HybridIntent };

inline AbortMarkerMode abortMarkerMode() {
	static const AbortMarkerMode mode = [] {
		const char* value = ::getenv("ROCKSDB_JS_ABORT_MODE");
		if (value && std::strcmp(value, "hybrid") == 0) return AbortMarkerMode::Hybrid;
		if (value && std::strcmp(value, "hybrid-vt") == 0) return AbortMarkerMode::HybridIntent;
		return AbortMarkerMode::Full;
	}();
	return mode;
}

/** Process-wide counters for the prototype measurement; relaxed, approximate under races. */
struct CommitDiag {
	std::atomic<uint64_t> refusedByIntent{0};
	std::atomic<uint64_t> refusedByValidation{0};
	std::atomic<uint64_t> conflictAfterLog{0};
	std::atomic<uint64_t> abortsMarked{0};
	std::atomic<uint64_t> abortMarkFailures{0};
	std::atomic<uint64_t> parks{0};
	std::atomic<uint64_t> parkTimeouts{0};
	std::atomic<uint64_t> parkTotalUs{0};
	std::atomic<uint64_t> parkMaxUs{0};

	void recordPark(uint64_t us, bool timedOut) {
		parks.fetch_add(1, std::memory_order_relaxed);
		if (timedOut) parkTimeouts.fetch_add(1, std::memory_order_relaxed);
		parkTotalUs.fetch_add(us, std::memory_order_relaxed);
		uint64_t prev = parkMaxUs.load(std::memory_order_relaxed);
		while (us > prev && !parkMaxUs.compare_exchange_weak(prev, us, std::memory_order_relaxed)) {}
	}
};

inline CommitDiag& commitDiag() {
	static CommitDiag diag;
	return diag;
}

} // namespace rocksdb_js

#endif
