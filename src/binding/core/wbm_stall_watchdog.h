#ifndef __CORE_WBM_STALL_WATCHDOG_H__
#define __CORE_WBM_STALL_WATCHDOG_H__

#include <chrono>
#include <cstdint>
#include <map>
#include <string>

namespace rocksdb_js {

/**
 * Sampling cadence of the WriteBufferManager stall watchdog, and therefore the
 * resolution of every duration it reports.
 */
constexpr uint64_t WBM_STALL_SAMPLE_INTERVAL_MS = 1000;

constexpr uint64_t WBM_STALL_WARN_MS_DEFAULT = 5000;

/**
 * Upper bound for `ROCKSDB_JS_WBM_STALL_WARN_MS`. A value past this is treated as
 * malformed rather than clamped: a threshold longer than a day is never what a
 * caller meant, and accepting one silently disables the alarm.
 */
constexpr uint64_t WBM_STALL_WARN_MS_MAX = 24 * 60 * 60 * 1000;

/**
 * Parses `ROCKSDB_JS_WBM_STALL_WARN_MS`. `0` disables the watchdog; anything
 * below one sample interval clamps up to it (a threshold the sampler cannot
 * resolve is a lie); malformed, negative, or out-of-range falls back to the
 * default. Falling back rather than clamping errs toward alarming earlier than
 * asked, never later — but silently, so `rejected` reports it for a diagnostic.
 */
uint64_t resolveWbmStallWarnMs(const char* raw, bool* rejected = nullptr);

/**
 * Decision state for the WriteBufferManager stall watchdog. Node-free and
 * RocksDB-free so it is unit-testable without threads (see
 * test/native/wbm_stall_watchdog_test.cc).
 *
 * A WriteBufferManager stall exposes no edge callback — `IsStallActive()` is all
 * there is — so this is driven by a poll. It collapses that poll into one report
 * per episode: a stall must be *continuously* active past the threshold, which is
 * what bounds a flapping stall. There is deliberately no second rate-limit window
 * on top; a window would suppress the first report of a genuinely new episode,
 * which is the one report that matters.
 *
 * `markReported()` is separate from `onSample()` so the caller only retires the
 * episode once the line has actually been written — a failed write leaves the
 * next sample free to retry.
 */
class WbmStallWatchdogState final {
public:
	using Clock = std::chrono::steady_clock;

	struct Sample {
		/** The threshold was crossed and this episode has not been reported yet. */
		bool reportNow = false;
		/** How long the current stall has been active; 0 when not stalled. */
		uint64_t stallActiveMs = 0;
	};

	Sample onSample(bool stallActive, Clock::time_point now, uint64_t thresholdMs) {
		Sample sample;
		if (!stallActive) {
			this->active = false;
			this->reported = false;
			return sample;
		}
		if (!this->active) {
			this->active = true;
			this->reported = false;
			this->startedAt = now;
			return sample;
		}
		auto elapsed =
			std::chrono::duration_cast<std::chrono::milliseconds>(now - this->startedAt).count();
		sample.stallActiveMs = elapsed < 0 ? 0 : static_cast<uint64_t>(elapsed);
		sample.reportNow =
			thresholdMs != 0 && !this->reported && sample.stallActiveMs >= thresholdMs;
		return sample;
	}

	void markReported() {
		this->reported = true;
	}

private:
	bool active = false;
	bool reported = false;
	Clock::time_point startedAt{};
};

/**
 * Everything the warn line carries. Aggregate numbers only — no paths, keys,
 * values or column-family names.
 */
struct WriteBufferManagerStallReport final {
	uint64_t stallActiveMs = 0;
	uint64_t bufferSize = 0;
	uint64_t memoryUsage = 0;
	uint64_t mutableMemoryUsage = 0;
	bool allowStall = false;
	bool costToCache = false;
	uint64_t columnFamilies = 0;
	/** Effective `max_write_buffer_size_to_maintain` -> number of column families with it. */
	std::map<int64_t, uint64_t> maxWriteBufferSizeToMaintain;
	/**
	 * False when the registry was locked by a close that is itself wedged on this
	 * stall, so the two fields above are empty rather than measured. The line is
	 * still emitted: an alarm that waits for the inventory would go silent in
	 * exactly the incident it exists for.
	 */
	bool inventoryAvailable = true;
};

std::string formatWriteBufferManagerStallReport(const WriteBufferManagerStallReport& report);

} // namespace rocksdb_js

#endif
