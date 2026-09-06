#include "database/db_stats.h"
#include <cstdio>
#include "core/platform.h"
#include "database/db_registry.h"
#include "database/db_settings.h"
#include "napi/global_events.h"
#include "napi/macros.h"

namespace rocksdb_js {

namespace {

constexpr const char* WBM_BUFFER_SIZE_KEY = "writeBufferManager.bufferSize";
constexpr const char* WBM_MEMORY_USAGE_KEY = "writeBufferManager.memoryUsage";
constexpr const char* WBM_MUTABLE_MEMORY_USAGE_KEY = "writeBufferManager.mutableMemoryUsage";
constexpr const char* WBM_STALL_ACTIVE_KEY = "writeBufferManager.stallActive";
constexpr const char* WBM_STALL_ACTIVE_MS_KEY = "writeBufferManager.stallActiveMs";

uint64_t writeBufferManagerStallWarnMs() {
	static const uint64_t value = [] {
		const char* raw = ::getenv("ROCKSDB_JS_WBM_STALL_WARN_MS");
		bool rejected = false;
		uint64_t resolved = resolveWbmStallWarnMs(raw, &rejected);
		if (rejected) {
			::fprintf(stderr,
				"[rocksdb-js] ignoring ROCKSDB_JS_WBM_STALL_WARN_MS=\"%s\" (not an integer in "
				"[0, %llu] ms); using %llu\n",
				raw, static_cast<unsigned long long>(WBM_STALL_WARN_MS_MAX),
				static_cast<unsigned long long>(resolved));
		}
		return resolved;
	}();
	return value;
}

bool lookupWriteBufferManagerStat(
	const std::string& statName,
	const WriteBufferManagerStats& stats,
	double& value
) {
	if (statName == WBM_BUFFER_SIZE_KEY) {
		value = static_cast<double>(stats.bufferSize);
	} else if (statName == WBM_MEMORY_USAGE_KEY) {
		value = static_cast<double>(stats.memoryUsage);
	} else if (statName == WBM_MUTABLE_MEMORY_USAGE_KEY) {
		value = static_cast<double>(stats.mutableMemoryUsage);
	} else if (statName == WBM_STALL_ACTIVE_KEY) {
		value = stats.stallActive ? 1 : 0;
	} else if (statName == WBM_STALL_ACTIVE_MS_KEY) {
		value = static_cast<double>(stats.stallActiveMs);
	} else {
		return false;
	}
	return true;
}

napi_status setNumberProperty(napi_env env, napi_value target, const char* key, uint64_t value) {
	napi_value jsValue;
	napi_status status = ::napi_create_double(env, static_cast<double>(value), &jsValue);
	if (status != napi_ok) {
		return status;
	}
	return ::napi_set_named_property(env, target, key, jsValue);
}

napi_status setBoolProperty(napi_env env, napi_value target, const char* key, bool value) {
	napi_value jsValue;
	napi_status status = ::napi_get_boolean(env, value, &jsValue);
	if (status != napi_ok) {
		return status;
	}
	return ::napi_set_named_property(env, target, key, jsValue);
}

} // namespace

DBStats::DBStats() {
	// These dependencies must be destroyed after the watchdog owner.
	(void)DBSettings::getInstance();
	(void)GlobalEvents::getInstance();
}

void DBStats::publishWriteBufferManager(rocksdb::WriteBufferManager* writeBufferManager) {
	this->writeBufferManager.store(writeBufferManager, std::memory_order_release);
}

void DBStats::ensureWriteBufferManagerWatchdog() {
	if (writeBufferManagerStallWarnMs() == 0) {
		return;
	}
	std::lock_guard<std::mutex> lock(this->watchdogMutex);
	// This path can run under databasesMutex -> writeBufferManagerMutex. Joining
	// here would deadlock against a retiring sample collecting registry inventory.
	if (this->watchdogStarted) {
		return;
	}
	this->watchdogStopRequested = false;
	this->writeBufferManagerWatchdogStopping.store(false, std::memory_order_relaxed);
	const uint64_t generation = ++this->watchdogGeneration;
	try {
		this->watchdogThread =
			std::thread([this, generation]() { this->runWriteBufferManagerWatchdog(generation); });
		this->watchdogStarted = true;
		this->writeBufferManagerWatchdogRunning.store(true, std::memory_order_relaxed);
	} catch (...) {
		this->watchdogStarted = false;
		this->writeBufferManagerWatchdogRunning.store(false, std::memory_order_relaxed);
	}
}

void DBStats::requestWriteBufferManagerWatchdogStop() {
	{
		std::lock_guard<std::mutex> lock(this->watchdogMutex);
		this->watchdogStopRequested = true;
		this->writeBufferManagerWatchdogStopping.store(true, std::memory_order_relaxed);
	}
	this->watchdogCv.notify_all();
}

void DBStats::joinWriteBufferManagerWatchdog() {
	std::thread toJoin;
	{
		std::lock_guard<std::mutex> lock(this->watchdogMutex);
		this->watchdogStopRequested = true;
		this->writeBufferManagerWatchdogStopping.store(true, std::memory_order_relaxed);
		if (this->watchdogStarted) {
			toJoin = std::move(this->watchdogThread);
			this->watchdogStarted = false;
		}
	}
	this->watchdogCv.notify_all();
	if (toJoin.joinable()) {
		toJoin.join();
	}
}

void DBStats::runWriteBufferManagerWatchdog(uint64_t generation) {
	setThreadName("rocksdb-wbm-watchdog");
	const uint64_t thresholdMs = writeBufferManagerStallWarnMs();
	WbmStallWatchdogState state;
	std::unique_lock<std::mutex> lock(this->watchdogMutex);
	auto retired = [&] {
		return this->watchdogStopRequested || this->watchdogGeneration != generation;
	};
	while (!retired()) {
		this->watchdogCv.wait_for(lock, std::chrono::milliseconds(WBM_STALL_SAMPLE_INTERVAL_MS));
		if (retired()) {
			break;
		}
		lock.unlock();
		try {
			this->sampleWriteBufferManagerStall(state, thresholdMs);
		} catch (...) {
			// A failed diagnostic must not take down the process; the next sample retries.
		}
		lock.lock();
	}
	if (this->watchdogGeneration == generation) {
		this->writeBufferManagerStallActiveMs.store(0, std::memory_order_relaxed);
		this->writeBufferManagerWatchdogRunning.store(false, std::memory_order_relaxed);
	}
}

void DBStats::sampleWriteBufferManagerStall(
	WbmStallWatchdogState& state,
	uint64_t thresholdMs
) {
	rocksdb::WriteBufferManager* writeBufferManager =
		this->writeBufferManager.load(std::memory_order_acquire);
	if (writeBufferManager == nullptr) {
		return;
	}
	WbmStallWatchdogState::Sample sample = state.onSample(
		writeBufferManager->IsStallActive(), WbmStallWatchdogState::Clock::now(), thresholdMs
	);
	this->writeBufferManagerStallActiveMs.store(sample.stallActiveMs, std::memory_order_relaxed);
	if (!sample.reportNow ||
		this->writeBufferManagerWatchdogStopping.load(std::memory_order_relaxed)) {
		return;
	}

	DBSettings& settings = DBSettings::getInstance();
	WriteBufferManagerStallReport report;
	report.stallActiveMs = sample.stallActiveMs;
	report.bufferSize = writeBufferManager->buffer_size();
	report.memoryUsage = writeBufferManager->memory_usage();
	report.mutableMemoryUsage = writeBufferManager->mutable_memtable_memory_usage();
	report.allowStall = settings.getWriteBufferManagerAllowStall();
	report.costToCache = settings.getWriteBufferManagerCostToCache();
	report.inventoryAvailable = DBRegistry::CollectWriteBufferManagerInventory(
		writeBufferManager, report.columnFamilies, report.maxWriteBufferSizeToMaintain
	);

	std::string line = formatWriteBufferManagerStallReport(report);
	const bool wroteToStderr = ::fprintf(stderr, "%s\n", line.c_str()) >= 0;
	if (wroteToStderr) {
		::fflush(stderr);
	}
	const bool emitted = emitGlobalEvent("log.warn", ListenerData::fromStrings({ line }));
	if (wroteToStderr || emitted) {
		state.markReported();
	}
}

WriteBufferManagerStats DBStats::getWriteBufferManagerStats(bool includeColumnFamilies) {
	DBSettings& settings = DBSettings::getInstance();
	WriteBufferManagerStats stats;
	stats.allowStall = settings.getWriteBufferManagerAllowStall();
	stats.costToCache = settings.getWriteBufferManagerCostToCache();
	stats.watchdogRunning = this->writeBufferManagerWatchdogRunning.load(std::memory_order_relaxed);

	rocksdb::WriteBufferManager* writeBufferManager =
		this->writeBufferManager.load(std::memory_order_acquire);
	if (writeBufferManager == nullptr) {
		return stats;
	}
	stats.enabled = true;
	stats.bufferSize = writeBufferManager->buffer_size();
	stats.memoryUsage = writeBufferManager->memory_usage();
	stats.mutableMemoryUsage = writeBufferManager->mutable_memtable_memory_usage();
	stats.stallActive = writeBufferManager->IsStallActive();
	stats.stallActiveMs =
		this->writeBufferManagerStallActiveMs.load(std::memory_order_relaxed);
	if (includeColumnFamilies) {
		stats.inventoryAvailable = DBRegistry::CollectWriteBufferManagerInventory(
			writeBufferManager, stats.columnFamilies, stats.maxWriteBufferSizeToMaintain
		);
	}
	return stats;
}

bool DBStats::getWriteBufferManagerStat(const std::string& statName, double& value) {
	return lookupWriteBufferManagerStat(
		statName, this->getWriteBufferManagerStats(false), value
	);
}

void DBStats::setWriteBufferManagerStatsOnObject(napi_env env, napi_value result) {
	WriteBufferManagerStats stats = this->getWriteBufferManagerStats(false);
	static constexpr const char* keys[] = {
		WBM_BUFFER_SIZE_KEY,
		WBM_MEMORY_USAGE_KEY,
		WBM_MUTABLE_MEMORY_USAGE_KEY,
		WBM_STALL_ACTIVE_KEY,
		WBM_STALL_ACTIVE_MS_KEY,
	};
	for (const char* key : keys) {
		double value = 0;
		if (!lookupWriteBufferManagerStat(key, stats, value)) {
			continue;
		}
		napi_value jsValue;
		if (::napi_create_double(env, value, &jsValue) == napi_ok) {
			::napi_set_named_property(env, result, key, jsValue);
		}
	}
}

napi_value DBStats::GetWriteBufferManagerStats(napi_env env, napi_callback_info info) {
	WriteBufferManagerStats stats = DBStats::getInstance().getWriteBufferManagerStats(true);

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_object(env, &result));
	NAPI_STATUS_THROWS(setNumberProperty(env, result, "bufferSize", stats.bufferSize));
	NAPI_STATUS_THROWS(setNumberProperty(env, result, "memoryUsage", stats.memoryUsage));
	NAPI_STATUS_THROWS(setNumberProperty(env, result, "mutableMemoryUsage", stats.mutableMemoryUsage));
	NAPI_STATUS_THROWS(setNumberProperty(env, result, "stallActiveMs", stats.stallActiveMs));
	NAPI_STATUS_THROWS(setNumberProperty(env, result, "columnFamilies", stats.columnFamilies));
	NAPI_STATUS_THROWS(setBoolProperty(env, result, "enabled", stats.enabled));
	NAPI_STATUS_THROWS(setBoolProperty(env, result, "allowStall", stats.allowStall));
	NAPI_STATUS_THROWS(setBoolProperty(env, result, "costToCache", stats.costToCache));
	NAPI_STATUS_THROWS(setBoolProperty(env, result, "stallActive", stats.stallActive));
	NAPI_STATUS_THROWS(setBoolProperty(env, result, "watchdogRunning", stats.watchdogRunning));
	NAPI_STATUS_THROWS(setBoolProperty(env, result, "inventoryAvailable", stats.inventoryAvailable));

	napi_value targets;
	NAPI_STATUS_THROWS(::napi_create_object(env, &targets));
	for (const auto& [target, count] : stats.maxWriteBufferSizeToMaintain) {
		napi_value countValue;
		NAPI_STATUS_THROWS(::napi_create_int64(env, static_cast<int64_t>(count), &countValue));
		NAPI_STATUS_THROWS(::napi_set_named_property(
			env, targets, std::to_string(target).c_str(), countValue
		));
	}
	NAPI_STATUS_THROWS(::napi_set_named_property(
		env, result, "maxWriteBufferSizeToMaintain", targets
	));

	return result;
}

void DBStats::Init(napi_env env, napi_value exports) {
	(void)DBStats::getInstance();

	napi_value writeBufferManagerStatsFn;
	NAPI_STATUS_THROWS_VOID(::napi_create_function(
		env,
		"getWriteBufferManagerStats",
		NAPI_AUTO_LENGTH,
		DBStats::GetWriteBufferManagerStats,
		nullptr,
		&writeBufferManagerStatsFn
	));
	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(
		env, exports, "getWriteBufferManagerStats", writeBufferManagerStatsFn
	));
}

DBStats::~DBStats() {
	this->joinWriteBufferManagerWatchdog();
}

} // namespace rocksdb_js
