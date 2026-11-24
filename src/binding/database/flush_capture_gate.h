#ifndef __FLUSH_CAPTURE_GATE_H__
#define __FLUSH_CAPTURE_GATE_H__

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
 */
class FlushCaptureGate final {
public:
	void enterFlushBegin() {
		std::unique_lock<std::mutex> lock(this->mutex);
		if (!this->held) {
			return;
		}
		++this->parked;
		this->condition.wait(lock, [this] { return !this->held; });
		--this->parked;
	}

	std::mutex captureMutex;

private:
	friend class FlushCaptureHold;

	std::mutex mutex;
	std::condition_variable condition;
	bool held = false;
	uint32_t parked = 0;
};

class FlushCaptureHold final {
public:
	explicit FlushCaptureHold(FlushCaptureGate& gate) : gate(gate) {
		std::lock_guard<std::mutex> lock(gate.mutex);
		gate.held = true;
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

private:
	FlushCaptureGate& gate;
};

} // namespace rocksdb_js

#endif
