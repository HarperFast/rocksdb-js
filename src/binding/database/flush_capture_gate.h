#ifndef __FLUSH_CAPTURE_GATE_H__
#define __FLUSH_CAPTURE_GATE_H__

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>

namespace rocksdb_js {

/**
 * Parks flushes at `OnFlushBegin` while a backup captures its table files and
 * transaction-log replay positions together (see `captureLiveFilesWithTransactionLogs`).
 * RocksDB calls `OnFlushBegin` with the DB mutex released and before the flush
 * installs anything, so a parked flush has changed neither the live files nor
 * `txn.state`, and the capturer may take the DB mutex while flushes are parked.
 *
 * A flush parks for at most `maxPark`, then proceeds and marks the hold
 * escaped. The bound is what keeps a parked flush from wedging the process:
 * a parked flush occupies a HIGH-pool thread, and a listing waits for purges,
 * which RocksDB also schedules on that pool (iterator cleanup does so even while
 * file deletions are disabled).
 */
class FlushCaptureGate final {
public:
	explicit FlushCaptureGate(std::chrono::milliseconds maxPark = std::chrono::seconds(1)) : maxPark(maxPark) {}

	void enterFlushBegin() {
		std::unique_lock<std::mutex> lock(this->mutex);
		if (!this->held) {
			return;
		}
		++this->parked;
		if (!this->condition.wait_for(lock, this->maxPark, [this] { return !this->held; })) {
			this->escaped = true;
		}
		--this->parked;
	}

	std::mutex captureMutex;

private:
	friend class FlushCaptureHold;

	const std::chrono::milliseconds maxPark;
	std::mutex mutex;
	std::condition_variable condition;
	bool held = false;
	bool escaped = false;
	uint32_t parked = 0;
};

class FlushCaptureHold final {
public:
	explicit FlushCaptureHold(FlushCaptureGate& gate) : gate(gate) {
		std::lock_guard<std::mutex> lock(gate.mutex);
		gate.held = true;
		gate.escaped = false;
	}

	~FlushCaptureHold() {
		{
			std::lock_guard<std::mutex> lock(this->gate.mutex);
			this->gate.held = false;
		}
		this->gate.condition.notify_all();
	}

	FlushCaptureHold(const FlushCaptureHold&) = delete;
	FlushCaptureHold& operator=(const FlushCaptureHold&) = delete;

	uint32_t parkedFlushes() {
		std::lock_guard<std::mutex> lock(this->gate.mutex);
		return this->gate.parked;
	}

	/** A flush outwaited the hold and may have installed during it. */
	bool flushEscaped() {
		std::lock_guard<std::mutex> lock(this->gate.mutex);
		return this->gate.escaped;
	}

private:
	FlushCaptureGate& gate;
};

} // namespace rocksdb_js

#endif
