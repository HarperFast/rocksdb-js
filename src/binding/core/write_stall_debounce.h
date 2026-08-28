#ifndef __CORE_WRITE_STALL_DEBOUNCE_H__
#define __CORE_WRITE_STALL_DEBOUNCE_H__

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

namespace rocksdb_js {

/**
 * Per-column-family debounce for the `'writeStall'` event. Node-free so it is
 * unit-testable without a running V8 (see test/native/write_stall_debounce_test.cc).
 *
 * RocksDB reports write-stall transitions per column family and, under the
 * `dbWriteBufferSize`-oversubscription pathology, oscillates a CF between normal
 * and stalled many times per second. This collapses that into an actionable
 * signal WITHOUT a timer:
 *
 *   - Only the rising edge (normal -> delayed/stopped) emits — the alert.
 *   - Repeated risings during one oscillating episode are rate-limited to at most
 *     one per `windowMs`, so a sustained stall yields a bounded heartbeat, never a
 *     flood.
 *   - The falling edge (-> normal) re-arms the CF but emits nothing: a no-timer
 *     design cannot deliver a non-flapping "recovered" (it can't tell a brief dip
 *     from a real recovery without waiting), so recovery is observed via the live
 *     `RocksDatabase.isWriteStalled()` pull instead of pushed here.
 *
 * The re-arm is why the FSM must run even when nobody is listening: gating the
 * whole thing on listener presence would strand a CF `reportedStalled` after a
 * listener detaches mid-stall and suppress the next genuine alert.
 */
class WriteStallDebounce final {
public:
	using Clock = std::chrono::steady_clock;

	/**
	 * Records a stall-condition transition for `columnFamily` and returns whether
	 * it should emit a `'writeStall'` event. `isStalled` is true when the current
	 * condition is delayed or stopped (normal collapses to false). `windowMs` is
	 * the rate-limit window; 0 emits every rising edge.
	 */
	bool onTransition(const std::string& columnFamily, bool isStalled, Clock::time_point now, uint64_t windowMs) {
		std::lock_guard<std::mutex> lock(this->mutex);
		Entry& entry = this->entries[columnFamily];
		if (!isStalled) {
			entry.reportedStalled = false; // falling edge: re-arm, emit nothing
			return false;
		}
		if (entry.reportedStalled) {
			return false; // already reported stalled for this episode
		}
		entry.reportedStalled = true;
		if (windowMs != 0 && entry.everEmitted &&
			now - entry.lastEmit < std::chrono::milliseconds(windowMs)) {
			return false; // rate-limited: an oscillation dip within the window
		}
		entry.lastEmit = now;
		entry.everEmitted = true;
		return true;
	}

	/** Retires a column family's state (call on drop) so the map stays bounded. */
	void forget(const std::string& columnFamily) {
		std::lock_guard<std::mutex> lock(this->mutex);
		this->entries.erase(columnFamily);
	}

private:
	struct Entry {
		bool reportedStalled = false;
		bool everEmitted = false;
		Clock::time_point lastEmit{};
	};
	std::mutex mutex;
	std::unordered_map<std::string, Entry> entries;
};

} // namespace rocksdb_js

#endif
