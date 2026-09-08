#ifndef __DB_STATS_H__
#define __DB_STATS_H__

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <node_api.h>
#include <string>
#include <thread>
#include "core/wbm_stall_watchdog.h"
#include "rocksdb/write_buffer_manager.h"

namespace rocksdb_js {

struct WriteBufferManagerStats final {
	bool enabled = false;
	uint64_t bufferSize = 0;
	uint64_t memoryUsage = 0;
	uint64_t mutableMemoryUsage = 0;
	bool allowStall = false;
	bool costToCache = false;
	bool stallActive = false;
	uint64_t stallActiveMs = 0;
	bool watchdogRunning = false;
	uint64_t columnFamilies = 0;
	std::map<int64_t, uint64_t> maxWriteBufferSizeToMaintain;
	bool inventoryAvailable = true;
};

class DBStats final {
private:
	DBStats();

	std::atomic<rocksdb::WriteBufferManager*> writeBufferManager{nullptr};
	std::atomic<uint64_t> writeBufferManagerStallActiveMs{0};
	std::atomic<bool> writeBufferManagerWatchdogRunning{false};
	std::atomic<bool> writeBufferManagerWatchdogStopping{false};

	std::thread watchdogThread;
	std::mutex watchdogMutex;
	std::condition_variable watchdogCv;
	bool watchdogStarted = false;
	bool watchdogArmed = false;
	bool watchdogStopRequested = false;
	/** One joiner owns the retiring thread; the rest wait for it. */
	bool watchdogRetiring = false;
	/**
	 * An `ensure` call arrived while a stop was in flight (e.g. `db.open()`
	 * racing `shutdown()`'s `DBRegistry::Shutdown()`, which closes databases
	 * without holding `databasesMutex`). `watchdogStopRequested` is the only
	 * signal that stop is resolved, so without this flag that reset is the
	 * last anyone hears from the bailed call: the database it was arming for
	 * is left with no watchdog for the rest of the process.
	 */
	bool watchdogArmPendingAfterStop = false;
	std::atomic<uint64_t> watchdogGeneration{0};

	void armWatchdogLocked();
	void runWriteBufferManagerWatchdog();
	void sampleWriteBufferManagerStall(
		WbmStallWatchdogState& state,
		uint64_t thresholdMs,
		uint64_t generation
	);
	WriteBufferManagerStats getWriteBufferManagerStats(bool includeColumnFamilies);

	static napi_value GetWriteBufferManagerStats(napi_env env, napi_callback_info info);

public:
	static DBStats& getInstance() {
		static DBStats instance;
		return instance;
	}

	void publishWriteBufferManager(rocksdb::WriteBufferManager* writeBufferManager);
	void ensureWriteBufferManagerWatchdog();
	void disableWriteBufferManagerWatchdog();
	void requestWriteBufferManagerWatchdogStop();
	/**
	 * `allowRearm` distinguishes the two callers: the explicit, JS-callable
	 * `shutdown()` documents that databases may reopen afterward, so an
	 * `ensure` that raced its stop should replay once the join resolves. The
	 * module's last-env cleanup hook and ~DBStats() cannot tell "shutdown,
	 * may reopen" from "process is ending" — a thread spawned there could
	 * outlive the join that is supposed to retire it — so both pass false
	 * and simply drop a queued replay, same as before this ever tracked one.
	 */
	void joinWriteBufferManagerWatchdog(bool allowRearm);

	bool getWriteBufferManagerStat(const std::string& statName, double& value);
	void setWriteBufferManagerStatsOnObject(napi_env env, napi_value result);

	static void Init(napi_env env, napi_value exports);
	~DBStats();
};

} // namespace rocksdb_js

#endif
