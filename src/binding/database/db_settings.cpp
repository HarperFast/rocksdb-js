#include "database/db_settings.h"
#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <random>
#include <string_view>
#include <thread>
#include "database/db_registry.h"
#include "database/db_stats.h"
#include "napi/macros.h"
#include "napi/helpers.h"
#include "napi/async.h"
#include "rocksdb/advanced_cache.h"

namespace rocksdb_js {

namespace {

uint32_t defaultCommitThreads() {
	const unsigned cores = std::thread::hardware_concurrency();
	return cores == 0 ? 1 : std::min(4u, cores);
}

uint64_t generateSeed() {
	std::random_device rd;
	uint64_t hi = static_cast<uint64_t>(rd());
	uint64_t lo = static_cast<uint64_t>(rd());
	return (hi << 32) | lo;
}

/**
 * RocksDB publishes the new policy before starting the cross-DB sorter thread, so a failed start
 * restores the previous one; leaving it would also make a retry of the same value a no-op that
 * never starts the sorter. Caller holds `writeBufferManagerMutex`.
 */
bool setLiveFlushPolicy(
	napi_env env,
	rocksdb::WriteBufferManager& manager,
	rocksdb::WriteBufferFlushPolicy policy
) {
	const rocksdb::WriteBufferFlushPolicy previous = manager.flush_policy();
	try {
		manager.SetFlushPolicy(policy);
		return true;
	} catch (const std::exception& e) {
		try {
			manager.SetFlushPolicy(previous);
		} catch (...) {
			// Config() has no outer handler; the original failure is the one to report.
		}
		::napi_throw_error(env, nullptr, e.what());
		return false;
	}
}

} // namespace

/**
 * The constructor for the DBSettings class.
 */
DBSettings::DBSettings():
	blockCacheSize(32 * 1024 * 1024), // 32MB (RocksDB default)
	blockCache(nullptr),
	occLockBucketCount(1 << 16),
	occValidateSerial(false),
	commitThreads(defaultCommitThreads()),
	writeBufferManagerSize(0), // disabled by default
	writeBufferManagerCostToCache(false),
	writeBufferManagerAllowStall(false),
	writeBufferManagerFlushPolicy(-1),
	writeBufferManager(nullptr),
	compactOnClose(false),
	lifecycleWaitSeconds(30),
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
			writeBufferManagerAllowStall.load(std::memory_order_relaxed),
			getWriteBufferManagerFlushPolicy()
		);
		DBStats::getInstance().publishWriteBufferManager(writeBufferManager.get());
	}
	if (writeBufferManagerAllowStall.load(std::memory_order_relaxed)) {
		DBStats::getInstance().ensureWriteBufferManagerWatchdog();
	}
	return writeBufferManager;
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

	napi_value occLockBucketsValue;
	NAPI_STATUS_THROWS(::napi_get_named_property(env, params, "occLockBuckets", &occLockBucketsValue));
	napi_valuetype occLockBucketsType;
	NAPI_STATUS_THROWS(::napi_typeof(env, occLockBucketsValue, &occLockBucketsType));
	double count = 0;
	const bool occLockBucketsProvided = occLockBucketsType != napi_undefined && occLockBucketsType != napi_null;
	if (occLockBucketsProvided) {
		if (::napi_get_value_double(env, occLockBucketsValue, &count) != napi_ok) {
			::napi_throw_type_error(env, nullptr, "occLockBuckets must be a number");
			return nullptr;
		}
		if (!std::isfinite(count) || std::trunc(count) != count || count < 16 || count > (1 << 24)) {
			::napi_throw_range_error(env, nullptr, "occLockBuckets must be an integer between 16 and 16777216");
			return nullptr;
		}
	}

	napi_value occValidationValue;
	NAPI_STATUS_THROWS(::napi_get_named_property(env, params, "occValidation", &occValidationValue));
	napi_valuetype occValidationType;
	NAPI_STATUS_THROWS(::napi_typeof(env, occValidationValue, &occValidationType));
	bool validateSerial = false;
	const bool occValidationProvided = occValidationType != napi_undefined && occValidationType != napi_null;
	if (occValidationProvided) {
		if (occValidationType != napi_string) {
			::napi_throw_type_error(env, nullptr, "occValidation must be a string");
			return nullptr;
		}
		char policy[16];
		size_t length = 0;
		NAPI_STATUS_THROWS(::napi_get_value_string_utf8(env, occValidationValue, policy, sizeof(policy), &length));
		if (length == 6 && std::strcmp(policy, "serial") == 0) {
			validateSerial = true;
		} else if (!(length == 8 && std::strcmp(policy, "parallel") == 0)) {
			::napi_throw_range_error(env, nullptr, "occValidation must be 'parallel' or 'serial'");
			return nullptr;
		}
	}

	napi_value flushPolicyValue;
	NAPI_STATUS_THROWS(::napi_get_named_property(env, params, "writeBufferManagerFlushPolicy", &flushPolicyValue));
	napi_valuetype flushPolicyType;
	NAPI_STATUS_THROWS(::napi_typeof(env, flushPolicyValue, &flushPolicyType));
	rocksdb::WriteBufferFlushPolicy newFlushPolicy = rocksdb::WriteBufferFlushPolicy::kFlushOldest;
	const bool flushPolicyProvided = flushPolicyType != napi_undefined && flushPolicyType != napi_null;
	if (flushPolicyProvided) {
		if (flushPolicyType != napi_string) {
			::napi_throw_type_error(env, nullptr, "writeBufferManagerFlushPolicy must be a string");
			return nullptr;
		}
		char policy[32];
		size_t length = 0;
		NAPI_STATUS_THROWS(::napi_get_value_string_utf8(env, flushPolicyValue, policy, sizeof(policy), &length));
		const std::string_view name(policy, length);
		if (name == "oldest") {
			newFlushPolicy = rocksdb::WriteBufferFlushPolicy::kFlushOldest;
		} else if (name == "largest") {
			newFlushPolicy = rocksdb::WriteBufferFlushPolicy::kFlushLargest;
		} else if (name == "largestAcrossDBs") {
			newFlushPolicy = rocksdb::WriteBufferFlushPolicy::kFlushLargestAcrossDBs;
		} else {
			::napi_throw_range_error(env, nullptr,
				"writeBufferManagerFlushPolicy must be 'oldest', 'largest' or 'largestAcrossDBs'");
			return nullptr;
		}
	}

	napi_value commitThreadsValue;
	NAPI_STATUS_THROWS(::napi_get_named_property(env, params, "commitThreads", &commitThreadsValue));
	napi_valuetype commitThreadsType;
	NAPI_STATUS_THROWS(::napi_typeof(env, commitThreadsValue, &commitThreadsType));
	double commitThreads = 0;
	const bool commitThreadsProvided = commitThreadsType != napi_undefined && commitThreadsType != napi_null;
	if (commitThreadsProvided) {
		if (::napi_get_value_double(env, commitThreadsValue, &commitThreads) != napi_ok) {
			::napi_throw_type_error(env, nullptr, "commitThreads must be a number");
			return nullptr;
		}
		if (!std::isfinite(commitThreads) || std::trunc(commitThreads) != commitThreads || commitThreads < 1 || commitThreads > 64) {
			::napi_throw_range_error(env, nullptr, "commitThreads must be an integer between 1 and 64");
			return nullptr;
		}
	}

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
	bool enableStallAfterDrain = false;
	uint64_t stallGeneration = 0;
	{
		std::lock_guard<std::mutex> lock(settings.writeBufferManagerMutex);
		const bool wbmAlreadyCreated = (settings.writeBufferManager != nullptr);
		const bool oldCostToCache = settings.writeBufferManagerCostToCache.load(std::memory_order_relaxed);

		if (wbmAlreadyCreated && costToCacheProvided && newCostToCache != oldCostToCache) {
			::napi_throw_error(env, nullptr,
				"writeBufferManagerCostToCache cannot be changed after the WriteBufferManager has been created; set it on the first config() call before any database is opened");
			return nullptr;
		}

		// The derived policy follows allowStall; applied before anything else in this
		// section is mutated so a failure leaves the manager's settings unchanged.
		if (wbmAlreadyCreated && allowStallProvided && !setLiveFlushPolicy(
			env,
			*settings.writeBufferManager,
			DBSettings::resolveWriteBufferManagerFlushPolicy(
				settings.writeBufferManagerFlushPolicy.load(std::memory_order_relaxed), newAllowStall
			)
		)) {
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
		// Only a supplied value: storing the value read before this mutex would
		// let a concurrent config() undo another's change.
		if (allowStallProvided) {
			settings.writeBufferManagerAllowStall.store(newAllowStall, std::memory_order_relaxed);
			stallGeneration = ++settings.allowStallGeneration;
		}

		// Propagate allowStall to the live manager if it exists — this is
		// the RocksDB-supported runtime knob.
		if (wbmAlreadyCreated && allowStallProvided) {
			if (newAllowStall) {
				enableStallAfterDrain = true;
			} else {
				settings.writeBufferManager->SetAllowStall(false);
				DBStats::getInstance().disableWriteBufferManagerWatchdog();
			}
		}
	}

	if (enableStallAfterDrain) {
		// A stalling manager can block a synchronous write on the JS thread
		// that alone releases idle pooled iterators' pinned memtables. The flag
		// stored above already stops new pooling; close what is idle now, then
		// let the manager stall unless a later change superseded this one (a
		// superseding enable runs its own drain). If the drain fails, the flag
		// keeps pooling off and the manager is left not stalling.
		try {
			DBRegistry::CloseIdlePooledIterators();
		} catch (const std::exception& e) {
			::napi_throw_error(env, nullptr, e.what());
			return nullptr;
		}
		std::lock_guard<std::mutex> lock(settings.writeBufferManagerMutex);
		if (settings.allowStallGeneration == stallGeneration && settings.writeBufferManager) {
			settings.writeBufferManager->SetAllowStall(true);
			DBStats::getInstance().ensureWriteBufferManagerWatchdog();
		}
	}

	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, params, "compactOnClose", settings.compactOnClose, false));

	double lifecycleWaitSeconds = 0;
	bool lifecycleWaitProvided = false;
	NAPI_STATUS_THROWS(::napi_has_named_property(env, params, "lifecycleWaitSeconds", &lifecycleWaitProvided));
	if (lifecycleWaitProvided) {
		napi_value lifecycleWaitValue;
		NAPI_STATUS_THROWS(::napi_get_named_property(env, params, "lifecycleWaitSeconds", &lifecycleWaitValue));
		napi_valuetype lifecycleWaitType;
		NAPI_STATUS_THROWS(::napi_typeof(env, lifecycleWaitValue, &lifecycleWaitType));
		if (lifecycleWaitType == napi_undefined || lifecycleWaitType == napi_null) {
			lifecycleWaitProvided = false;
		}
	}
	if (lifecycleWaitProvided) {
		status = rocksdb_js::getProperty(env, params, "lifecycleWaitSeconds", lifecycleWaitSeconds, true);
		if (status != napi_ok) {
			::napi_throw_type_error(env, nullptr, "Lifecycle wait seconds must be a number");
			return nullptr;
		}
		if (!std::isfinite(lifecycleWaitSeconds) || lifecycleWaitSeconds <= 0 ||
			std::trunc(lifecycleWaitSeconds) != lifecycleWaitSeconds ||
			lifecycleWaitSeconds > std::numeric_limits<uint32_t>::max()
		) {
			::napi_throw_range_error(env, nullptr, "Lifecycle wait seconds must be a positive integer");
			return nullptr;
		}
		settings.lifecycleWaitSeconds.store(
			static_cast<uint32_t>(lifecycleWaitSeconds),
			std::memory_order_relaxed
		);
	}

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

	// Stored last: these fields are validated up front, but a later field's
	// throw must still leave them untouched. A getter that throws (rather than just
	// supplying the wrong type) leaves a pending exception none of the reads above
	// check for; catch it here, right before the stores, so it stays correct
	// regardless of which field above is last.
	bool exceptionPending = false;
	::napi_is_exception_pending(env, &exceptionPending);
	if (exceptionPending) {
		return nullptr;
	}
	// Also last: an explicit policy is permanent, so a rejected call must not leave it behind.
	if (flushPolicyProvided) {
		std::lock_guard<std::mutex> lock(settings.writeBufferManagerMutex);
		if (settings.writeBufferManager &&
			!setLiveFlushPolicy(env, *settings.writeBufferManager, newFlushPolicy)) {
			return nullptr;
		}
		settings.writeBufferManagerFlushPolicy.store(static_cast<int>(newFlushPolicy), std::memory_order_relaxed);
	}
	if (occLockBucketsProvided) {
		settings.occLockBucketCount.store(static_cast<uint32_t>(count), std::memory_order_relaxed);
	}
	if (occValidationProvided) {
		settings.occValidateSerial.store(validateSerial, std::memory_order_relaxed);
	}
	if (commitThreadsProvided) {
		settings.commitThreads.store(static_cast<uint32_t>(commitThreads), std::memory_order_relaxed);
	}

	NAPI_RETURN_UNDEFINED();
}

/**
 * Exports the `config()` function to JavaScript.
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
}

}
