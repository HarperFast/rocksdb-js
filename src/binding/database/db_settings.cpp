#include "database/db_settings.h"
#include <cstdio>
#include <random>
#include "napi/macros.h"
#include "core/platform.h"
#include "database/db_registry.h"
#include "napi/global_events.h"
#include "napi/helpers.h"
#include "napi/async.h"
#include "rocksdb/advanced_cache.h"

namespace rocksdb_js {

namespace {

uint64_t generateSeed() {
	std::random_device rd;
	uint64_t hi = static_cast<uint64_t>(rd());
	uint64_t lo = static_cast<uint64_t>(rd());
	return (hi << 32) | lo;
}

// Resolved once per process: the watchdog runs on its own thread, and ::getenv
// there is not safe against a concurrent process.env write on any JS thread
// (same reasoning as ROCKSDB_JS_PARK_TIMEOUT_MS).
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

/**
 * The constructor for the DBSettings class.
 */
DBSettings::DBSettings():
	blockCacheSize(32 * 1024 * 1024), // 32MB (RocksDB default)
	blockCache(nullptr),
	writeBufferManagerSize(0), // disabled by default
	writeBufferManagerCostToCache(false),
	writeBufferManagerAllowStall(false),
	writeBufferManager(nullptr),
	compactOnClose(false),
	verificationTableEntries(128 * 1024), // 128K slots = 1 MB at 8 bytes per slot
	verificationTableSeed(generateSeed()),
	verificationTable(nullptr)
{}

/**
 * Get the LRU block cache instance if the block cache size is greater than 0
 * and create it if it doesn't exist.
 *
 * @returns The block cache.
 *
 * @example
 * ```cpp
 * std::shared_ptr<rocksdb::Cache> cache = DBSettings::getInstance().getBlockCache();
 * ```
 */
std::shared_ptr<rocksdb::Cache> DBSettings::getBlockCache() {
	if (blockCacheSize == 0) {
		return nullptr;
	}
	if (!blockCache) {
		blockCache = rocksdb::NewLRUCache(blockCacheSize);
	}
	return blockCache;
}

/**
 * Get the WriteBufferManager instance, lazily creating it on first request.
 *
 * The manager is constructed once and reused across all databases opened in
 * this process. When `costToCache` is enabled, memtable memory is charged
 * against the shared block cache, so both subsystems draw from a single pool.
 *
 * The manager is intentionally not recreated on subsequent `config()` calls
 * because RocksDB stores a `shared_ptr<WriteBufferManager>` inside each open
 * database; swapping the instance would orphan any already-open DB. Changes
 * to `buffer_size` use `SetBufferSize()` which is the supported runtime
 * update path.
 *
 * @returns The write buffer manager, or `nullptr` if disabled (size == 0).
 */
std::shared_ptr<rocksdb::WriteBufferManager> DBSettings::getWriteBufferManager() {
	const size_t size = writeBufferManagerSize.load(std::memory_order_relaxed);
	if (size == 0) {
		return nullptr;
	}
	std::lock_guard<std::mutex> lock(writeBufferManagerMutex);
	// Re-check inside the lock: another thread may have torn down the value
	// (set size=0) between our atomic load and acquiring the lock.
	if (writeBufferManagerSize.load(std::memory_order_relaxed) == 0) {
		return nullptr;
	}
	if (!writeBufferManager) {
		std::shared_ptr<rocksdb::Cache> cache;
		if (writeBufferManagerCostToCache.load(std::memory_order_relaxed)) {
			cache = getBlockCache(); // may be nullptr if block cache is disabled
		}
		writeBufferManager = std::make_shared<rocksdb::WriteBufferManager>(
			writeBufferManagerSize.load(std::memory_order_relaxed),
			cache,
			writeBufferManagerAllowStall.load(std::memory_order_relaxed)
		);
		writeBufferManagerPtr.store(writeBufferManager.get(), std::memory_order_release);
	}
	if (writeBufferManagerAllowStall.load(std::memory_order_relaxed)) {
		this->ensureWriteBufferManagerWatchdog();
	}
	return writeBufferManager;
}

/**
 * Starts the stall watchdog if it is not already running. Only a stalling manager
 * can stall (`WriteBufferManager::ShouldStall` short-circuits on `allow_stall`),
 * so nothing is started otherwise.
 *
 * Thread creation is allowed to fail without taking the caller down with it: this
 * is an observability feature, and turning thread exhaustion into a failed
 * `config()` or database open would be strictly worse than losing the warn line.
 * The failure is visible as `watchdogRunning: false`.
 */
void DBSettings::ensureWriteBufferManagerWatchdog() {
	if (writeBufferManagerStallWarnMs() == 0) {
		return;
	}
	std::lock_guard<std::mutex> lock(this->watchdogMutex);
	// A stop that has been requested but not yet joined is left to its joiner:
	// this path runs under databasesMutex (DBRegistry::OpenDB holds it across
	// DBDescriptor::open), and the watchdog takes databasesMutex to collect its
	// inventory, so joining here could deadlock against a sample in flight.
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
		// Set here, not on the new thread: a caller that enables stalling and reads
		// back immediately must not be told the watchdog is absent.
		this->writeBufferManagerWatchdogRunning.store(true, std::memory_order_relaxed);
	} catch (...) {
		this->watchdogStarted = false;
		this->writeBufferManagerWatchdogRunning.store(false, std::memory_order_relaxed);
	}
}

void DBSettings::requestWriteBufferManagerWatchdogStop() {
	{
		std::lock_guard<std::mutex> lock(this->watchdogMutex);
		this->watchdogStopRequested = true;
		this->writeBufferManagerWatchdogStopping.store(true, std::memory_order_relaxed);
	}
	this->watchdogCv.notify_all();
}

void DBSettings::joinWriteBufferManagerWatchdog() {
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

void DBSettings::runWriteBufferManagerWatchdog(uint64_t generation) {
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
			// A report that could not be built (allocation, formatting) must not
			// take the thread down: the next sample retries.
		}
		lock.lock();
	}
	if (this->watchdogGeneration == generation) {
		this->writeBufferManagerStallActiveMs.store(0, std::memory_order_relaxed);
		this->writeBufferManagerWatchdogRunning.store(false, std::memory_order_relaxed);
	}
}

void DBSettings::sampleWriteBufferManagerStall(WbmStallWatchdogState& state, uint64_t thresholdMs) {
	rocksdb::WriteBufferManager* wbm = this->getWriteBufferManagerRaw();
	if (wbm == nullptr) {
		return;
	}
	WbmStallWatchdogState::Sample sample = state.onSample(
		wbm->IsStallActive(), WbmStallWatchdogState::Clock::now(), thresholdMs
	);
	this->writeBufferManagerStallActiveMs.store(sample.stallActiveMs, std::memory_order_relaxed);
	if (!sample.reportNow ||
		this->writeBufferManagerWatchdogStopping.load(std::memory_order_relaxed)) {
		return;
	}

	WriteBufferManagerStallReport report;
	report.stallActiveMs = sample.stallActiveMs;
	report.bufferSize = wbm->buffer_size();
	report.memoryUsage = wbm->memory_usage();
	report.mutableMemoryUsage = wbm->mutable_memtable_memory_usage();
	report.allowStall = this->writeBufferManagerAllowStall.load(std::memory_order_relaxed);
	report.costToCache = this->writeBufferManagerCostToCache.load(std::memory_order_relaxed);
	report.inventoryAvailable = DBRegistry::CollectWriteBufferManagerInventory(
		wbm, report.columnFamilies, report.maxWriteBufferSizeToMaintain
	);

	std::string line = formatWriteBufferManagerStallReport(report);
	// stderr does not depend on an application having registered a listener; the
	// event is the one an application can route. The episode is retired once
	// either has carried it — gating on stderr alone would re-report every second
	// for the whole stall when fd 2 is closed but a listener is attached.
	const bool wroteToStderr = ::fprintf(stderr, "%s\n", line.c_str()) >= 0;
	if (wroteToStderr) {
		::fflush(stderr);
	}
	const bool emitted = emitGlobalEvent("log.warn", ListenerData::fromStrings({ line }));
	if (wroteToStderr || emitted) {
		state.markReported();
	}
}

WriteBufferManagerStats DBSettings::getWriteBufferManagerStats(bool includeColumnFamilies) {
	WriteBufferManagerStats stats;
	stats.allowStall = this->writeBufferManagerAllowStall.load(std::memory_order_relaxed);
	stats.costToCache = this->writeBufferManagerCostToCache.load(std::memory_order_relaxed);
	stats.watchdogRunning = this->writeBufferManagerWatchdogRunning.load(std::memory_order_relaxed);

	rocksdb::WriteBufferManager* wbm = this->getWriteBufferManagerRaw();
	if (wbm == nullptr) {
		return stats;
	}
	stats.enabled = true;
	stats.bufferSize = wbm->buffer_size();
	stats.memoryUsage = wbm->memory_usage();
	stats.mutableMemoryUsage = wbm->mutable_memtable_memory_usage();
	stats.stallActive = wbm->IsStallActive();
	stats.stallActiveMs = this->writeBufferManagerStallActiveMs.load(std::memory_order_relaxed);
	if (includeColumnFamilies) {
		stats.inventoryAvailable = DBRegistry::CollectWriteBufferManagerInventory(
			wbm, stats.columnFamilies, stats.maxWriteBufferSizeToMaintain
		);
	}
	return stats;
}

DBSettings::~DBSettings() {
	this->joinWriteBufferManagerWatchdog();
}

/**
 * Get the global verification table instance, materializing it on first call.
 * After the first call, the table is fixed in size for the process lifetime.
 */
VerificationTable* DBSettings::getVerificationTable() {
	std::lock_guard<std::mutex> lock(verificationTableMutex);
	if (!verificationTable) {
		verificationTable = std::make_unique<VerificationTable>(
			verificationTableEntries, verificationTableSeed
		);
	}
	return verificationTable.get();
}

/**
 * Returns the verification table if already materialized, without creating it.
 */
VerificationTable* DBSettings::getVerificationTableRaw() {
	std::lock_guard<std::mutex> lock(verificationTableMutex);
	return verificationTable.get();
}

/**
 * The `config()` JavaScript function.
 *
 * @param env The Node.js environment.
 * @param info The callback info.
 * @returns The result of the operation.
 *
 * @example
 * ```js
 * rocksdb.config({ blockCacheSize: 1024 * 1024 }); // 1MB
 * ```
 */
napi_value DBSettings::Config(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);

	DBSettings& settings = DBSettings::getInstance();
	napi_value params = argv[0];

	int64_t blockCacheSize = 0;
	napi_status status = rocksdb_js::getProperty(env, params, "blockCacheSize", blockCacheSize, true);
	if (status == napi_ok) {
		if (blockCacheSize < 0) {
			::napi_throw_range_error(env, nullptr, "Block cache size must be a positive integer or 0 to disable caching");
			return nullptr;
		}

		settings.blockCacheSize = static_cast<size_t>(blockCacheSize);

		if (settings.blockCache) {
			settings.blockCache->SetCapacity(blockCacheSize);
		}
	}

	int64_t writeBufferManagerSize = 0;
	const bool wbmSizeProvided =
		rocksdb_js::getProperty(env, params, "writeBufferManagerSize", writeBufferManagerSize, true) == napi_ok;
	if (wbmSizeProvided && writeBufferManagerSize < 0) {
		::napi_throw_range_error(env, nullptr, "Write buffer manager size must be a positive integer or 0 to disable");
		return nullptr;
	}

	// `costToCache` is immutable after WBM creation — the cache reservation
	// manager is configured at construction. `allowStall` IS mutable via
	// SetAllowStall, so we propagate runtime changes to the live manager.
	// Already-open DBs see the new behavior on their next write.
	bool newCostToCache = settings.writeBufferManagerCostToCache.load(std::memory_order_relaxed);
	bool newAllowStall = settings.writeBufferManagerAllowStall.load(std::memory_order_relaxed);
	const bool costToCacheProvided =
		rocksdb_js::getProperty(env, params, "writeBufferManagerCostToCache", newCostToCache, true) == napi_ok;
	const bool allowStallProvided =
		rocksdb_js::getProperty(env, params, "writeBufferManagerAllowStall", newAllowStall, true) == napi_ok;

	// All WBM updates run under one critical section so the costToCache
	// invariant check, SetBufferSize, and SetAllowStall observe a single
	// consistent view of the manager. Throwing happens before any state
	// mutation, so a rejected costToCache change leaves the live manager
	// untouched.
	bool retireWatchdog = false;
	{
		std::lock_guard<std::mutex> lock(settings.writeBufferManagerMutex);
		const bool wbmAlreadyCreated = (settings.writeBufferManager != nullptr);
		const bool oldCostToCache = settings.writeBufferManagerCostToCache.load(std::memory_order_relaxed);

		if (wbmAlreadyCreated && costToCacheProvided && newCostToCache != oldCostToCache) {
			::napi_throw_error(env, nullptr,
				"writeBufferManagerCostToCache cannot be changed after the WriteBufferManager has been created; set it on the first config() call before any database is opened");
			return nullptr;
		}

		if (wbmSizeProvided) {
			const size_t newSize = static_cast<size_t>(writeBufferManagerSize);
			settings.writeBufferManagerSize.store(newSize, std::memory_order_relaxed);

			// If the manager was already created, adjust its buffer size in
			// place (SetBufferSize is atomic). RocksDB asserts new_size > 0,
			// so when "disabling" via size=0 we leave the existing manager
			// alone — already-open DBs keep their reference, and subsequent
			// opens skip the manager entirely (see getWriteBufferManager).
			// Runtime size=0 is a "no new attachments" signal, not a teardown.
			if (newSize > 0 && wbmAlreadyCreated) {
				settings.writeBufferManager->SetBufferSize(newSize);
			}
		}

		settings.writeBufferManagerCostToCache.store(newCostToCache, std::memory_order_relaxed);
		settings.writeBufferManagerAllowStall.store(newAllowStall, std::memory_order_relaxed);

		// Propagate allowStall to the live manager if it exists — this is
		// the RocksDB-supported runtime knob.
		if (wbmAlreadyCreated && allowStallProvided) {
			settings.writeBufferManager->SetAllowStall(newAllowStall);
			if (newAllowStall) {
				// The databases already attached can stall from here on, with no
				// further open to start the watchdog.
				settings.ensureWriteBufferManagerWatchdog();
			} else {
				// Nothing can stall any more, so the thread would poll forever and
				// report watchdogRunning against allowStall: false.
				retireWatchdog = true;
			}
		}
	}

	// Joined outside the critical section: the watchdog's report path writes to
	// stderr, which blocks on a full pipe, and every DBDescriptor::open needs
	// writeBufferManagerMutex. Same split, and the same reason, as the teardown
	// path in binding.cpp.
	if (retireWatchdog) {
		settings.joinWriteBufferManagerWatchdog();
		// Reconcile against the state that is actually current, not this call's
		// own argument: another env re-enabling stalling in the unlocked window
		// above would have found the thread still started and declined to start
		// one, leaving allowStall on with no alarm behind it.
		std::lock_guard<std::mutex> lock(settings.writeBufferManagerMutex);
		if (settings.writeBufferManagerAllowStall.load(std::memory_order_relaxed) &&
			settings.writeBufferManager) {
			settings.ensureWriteBufferManagerWatchdog();
		}
	}

	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, params, "compactOnClose", settings.compactOnClose, false));

	int64_t verificationTableEntries = 0;
	status = rocksdb_js::getProperty(env, params, "verificationTableEntries", verificationTableEntries, true);
	if (status == napi_ok) {
		if (verificationTableEntries < 0) {
			::napi_throw_range_error(env, nullptr, "Verification table entries must be a positive integer or 0 to disable verification");
			return nullptr;
		}

		std::lock_guard<std::mutex> lock(settings.verificationTableMutex);
		if (settings.verificationTable) {
			::napi_throw_error(env, nullptr, "Verification table size cannot be changed after the first database is opened");
			return nullptr;
		}
		settings.verificationTableEntries = static_cast<size_t>(verificationTableEntries);
	}

	NAPI_RETURN_UNDEFINED();
}

/**
 * The `getWriteBufferManagerStats()` JavaScript function. Process-wide: the
 * WriteBufferManager is a singleton shared by every database opened in this
 * process, including from worker threads.
 */
napi_value DBSettings::GetWriteBufferManagerStats(napi_env env, napi_callback_info info) {
	WriteBufferManagerStats stats = DBSettings::getInstance().getWriteBufferManagerStats(true);

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
	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "maxWriteBufferSizeToMaintain", targets));

	return result;
}

/**
 * Exports the `config()` and `getWriteBufferManagerStats()` functions to
 * JavaScript.
 *
 * @param env The Node.js environment.
 * @param exports The exports object.
 */
void DBSettings::Init(napi_env env, napi_value exports) {
	napi_value configFn;
	NAPI_STATUS_THROWS_VOID(::napi_create_function(
		env,
		"config",
		NAPI_AUTO_LENGTH,
		DBSettings::Config,
		nullptr,
		&configFn
	));

	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, "config", configFn));

	napi_value wbmStatsFn;
	NAPI_STATUS_THROWS_VOID(::napi_create_function(
		env,
		"getWriteBufferManagerStats",
		NAPI_AUTO_LENGTH,
		DBSettings::GetWriteBufferManagerStats,
		nullptr,
		&wbmStatsFn
	));
	NAPI_STATUS_THROWS_VOID(
		::napi_set_named_property(env, exports, "getWriteBufferManagerStats", wbmStatsFn)
	);
}

}
