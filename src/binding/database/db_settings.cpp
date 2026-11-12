#include "database/db_settings.h"
#include <algorithm>
#include <cmath>
#include <initializer_list>
#include <limits>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
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

/**
 * Reads an optional `config()` field: absent, null and undefined leave `out` empty. A value of the
 * wrong type throws `typeError`, or is ignored when none is given. Returns false with an exception
 * pending, including one thrown by a getter.
 */
template <typename T>
bool readField(
	napi_env env,
	napi_value params,
	const char* name,
	std::optional<T>& out,
	const char* typeError = nullptr
) {
	napi_value value;
	napi_valuetype type;
	napi_status status = ::napi_get_named_property(env, params, name, &value);
	if (status == napi_ok) {
		status = ::napi_typeof(env, value, &type);
	}
	if (status != napi_ok) {
		bool pending = false;
		::napi_is_exception_pending(env, &pending);
		if (!pending) {
			const std::string error = getNapiExtendedError(env, status);
			::napi_throw_error(env, nullptr, error.c_str());
		}
		return false;
	}
	if (type == napi_undefined || type == napi_null) {
		return true;
	}
	T result;
	// getValue() also accepts a Buffer for a string.
	const bool typeMatches = (!std::is_same_v<T, std::string> || type == napi_string) &&
		getValue(env, value, result) == napi_ok;
	if (typeMatches) {
		out = std::move(result);
	} else if (typeError) {
		::napi_throw_type_error(env, nullptr, typeError);
		return false;
	}
	return true;
}

template <typename T>
bool readEnumField(
	napi_env env,
	napi_value params,
	const char* name,
	std::initializer_list<std::pair<std::string_view, T>> values,
	std::optional<T>& out,
	const char* typeError,
	const char* rangeError
) {
	std::optional<std::string> text;
	if (!readField(env, params, name, text, typeError)) {
		return false;
	}
	if (!text) {
		return true;
	}
	for (const auto& [key, value] : values) {
		if (*text == key) {
			out = value;
			return true;
		}
	}
	::napi_throw_range_error(env, nullptr, rangeError);
	return false;
}

bool checkRange(napi_env env, bool valid, const char* rangeError) {
	if (!valid) {
		::napi_throw_range_error(env, nullptr, rangeError);
	}
	return valid;
}

bool isIntegerInRange(double value, double min, double max) {
	return std::isfinite(value) && std::trunc(value) == value && value >= min && value <= max;
}

template <typename A, typename T>
void storeIfSet(std::atomic<A>& target, const std::optional<T>& value) {
	if (value) {
		target.store(static_cast<A>(*value), std::memory_order_relaxed);
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
	transactionLogAbortMarkers(false),
	transactionLogWriteIntents(false),
	writeBufferManagerSize(0), // disabled by default
	writeBufferManagerCostToCache(false),
	writeBufferManagerAllowStall(false),
	writeBufferManagerFlushPolicy(-1),
	writeBufferManager(nullptr),
	compactOnClose(false),
	shutdownOnExit(true),
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

	// Every field is read and validated before any is applied.
	std::optional<double> occLockBuckets;
	std::optional<bool> occValidateSerial;
	std::optional<double> commitThreads;
	std::optional<bool> abortMarkers;
	std::optional<bool> writeIntents;
	std::optional<bool> compactOnClose;
	std::optional<bool> shutdownOnExit;
	std::optional<double> lifecycleWaitSeconds;
	std::optional<int64_t> blockCacheSize;
	std::optional<int64_t> wbmSize;
	std::optional<bool> costToCache;
	std::optional<bool> allowStall;
	std::optional<rocksdb::WriteBufferFlushPolicy> flushPolicy;
	std::optional<int64_t> vtEntries;
	if (
		!readField(env, params, "occLockBuckets", occLockBuckets, "occLockBuckets must be a number") ||
		!checkRange(env, !occLockBuckets || isIntegerInRange(*occLockBuckets, 16, 1 << 24),
			"occLockBuckets must be an integer between 16 and 16777216") ||
		!readEnumField(env, params, "occValidation", { { "parallel", false }, { "serial", true } },
			occValidateSerial,
			"occValidation must be a string",
			"occValidation must be 'parallel' or 'serial'") ||
		!readField(env, params, "commitThreads", commitThreads, "commitThreads must be a number") ||
		!checkRange(env, !commitThreads || isIntegerInRange(*commitThreads, 1, 64),
			"commitThreads must be an integer between 1 and 64") ||
		!readField(env, params, "transactionLogAbortMarkers", abortMarkers) ||
		!readField(env, params, "transactionLogWriteIntents", writeIntents) ||
		!readField(env, params, "compactOnClose", compactOnClose, "compactOnClose must be a boolean") ||
		!readField(env, params, "shutdownOnExit", shutdownOnExit, "shutdownOnExit must be a boolean") ||
		!readField(env, params, "lifecycleWaitSeconds", lifecycleWaitSeconds,
			"Lifecycle wait seconds must be a number") ||
		!checkRange(env,
			!lifecycleWaitSeconds ||
				isIntegerInRange(*lifecycleWaitSeconds, 1, std::numeric_limits<uint32_t>::max()),
			"Lifecycle wait seconds must be a positive integer") ||
		!readField(env, params, "blockCacheSize", blockCacheSize) ||
		!checkRange(env, !blockCacheSize || *blockCacheSize >= 0,
			"Block cache size must be a positive integer or 0 to disable caching") ||
		!readField(env, params, "writeBufferManagerSize", wbmSize) ||
		!checkRange(env, !wbmSize || *wbmSize >= 0,
			"Write buffer manager size must be a positive integer or 0 to disable") ||
		!readField(env, params, "writeBufferManagerCostToCache", costToCache) ||
		!readField(env, params, "writeBufferManagerAllowStall", allowStall) ||
		!readEnumField(env, params, "writeBufferManagerFlushPolicy", {
				{ "oldest", rocksdb::WriteBufferFlushPolicy::kFlushOldest },
				{ "largest", rocksdb::WriteBufferFlushPolicy::kFlushLargest },
				{ "largestAcrossDBs", rocksdb::WriteBufferFlushPolicy::kFlushLargestAcrossDBs },
			},
			flushPolicy,
			"writeBufferManagerFlushPolicy must be a string",
			"writeBufferManagerFlushPolicy must be 'oldest', 'largest' or 'largestAcrossDBs'") ||
		!readField(env, params, "verificationTableEntries", vtEntries) ||
		!checkRange(env, !vtEntries || *vtEntries >= 0,
			"Verification table entries must be a positive integer or 0 to disable verification")
	) {
		return nullptr;
	}

	// Checks against live state and the one fallible live update run before any store, under the
	// locks a concurrent first open takes, so it cannot land between check and apply.
	bool enableStallAfterDrain = false;
	uint64_t stallGeneration = 0;
	{
		std::lock_guard<std::mutex> lock(settings.writeBufferManagerMutex);
		const std::shared_ptr<rocksdb::WriteBufferManager>& manager = settings.writeBufferManager;

		// The cache reservation manager is configured at WBM construction.
		if (manager && costToCache &&
			*costToCache != settings.writeBufferManagerCostToCache.load(std::memory_order_relaxed)) {
			::napi_throw_error(env, nullptr,
				"writeBufferManagerCostToCache cannot be changed after the WriteBufferManager has been created; set it on the first config() call before any database is opened");
			return nullptr;
		}

		constexpr const char* verificationTableFrozen =
			"Verification table size cannot be changed after the first database is opened";
		if (vtEntries && settings.getVerificationTableRaw()) {
			::napi_throw_error(env, nullptr, verificationTableFrozen);
			return nullptr;
		}

		const bool updateLivePolicy = manager && (allowStall || flushPolicy);
		const rocksdb::WriteBufferFlushPolicy previousPolicy =
			manager ? manager->flush_policy() : rocksdb::WriteBufferFlushPolicy::kFlushOldest;
		if (updateLivePolicy && !setLiveFlushPolicy(
			env,
			*manager,
			DBSettings::resolveWriteBufferManagerFlushPolicy(
				flushPolicy
					? static_cast<int>(*flushPolicy)
					: settings.writeBufferManagerFlushPolicy.load(std::memory_order_relaxed),
				allowStall.value_or(settings.writeBufferManagerAllowStall.load(std::memory_order_relaxed))
			)
		)) {
			return nullptr;
		}

		// Rechecked under the lock that materializes the table: a first open on another thread may
		// have done so since the check above. The policy update stays outside this lock because
		// every verification-table read takes it.
		if (vtEntries) {
			bool frozen = false;
			{
				std::lock_guard<std::mutex> vtLock(settings.verificationTableMutex);
				frozen = settings.verificationTable != nullptr;
				if (!frozen) {
					settings.verificationTableEntries = static_cast<size_t>(*vtEntries);
				}
			}
			if (frozen) {
				if (updateLivePolicy) {
					try {
						manager->SetFlushPolicy(previousPolicy);
					} catch (...) {
						// The rejection is the error to report.
					}
				}
				::napi_throw_error(env, nullptr, verificationTableFrozen);
				return nullptr;
			}
		}

		// Before the WBM size and costToCache become visible: a first open may construct the manager
		// as soon as this mutex is released, and with costToCache it charges this cache.
		if (blockCacheSize) {
			settings.blockCacheSize = static_cast<size_t>(*blockCacheSize);
			if (settings.blockCache) {
				settings.blockCache->SetCapacity(*blockCacheSize);
			}
		}

		if (flushPolicy) {
			settings.writeBufferManagerFlushPolicy.store(static_cast<int>(*flushPolicy), std::memory_order_relaxed);
		}

		if (wbmSize) {
			settings.writeBufferManagerSize.store(static_cast<size_t>(*wbmSize), std::memory_order_relaxed);

			// If the manager was already created, adjust its buffer size in
			// place (SetBufferSize is atomic). RocksDB asserts new_size > 0,
			// so when "disabling" via size=0 we leave the existing manager
			// alone — already-open DBs keep their reference, and subsequent
			// opens skip the manager entirely (see getWriteBufferManager).
			// Runtime size=0 is a "no new attachments" signal, not a teardown.
			if (*wbmSize > 0 && manager) {
				manager->SetBufferSize(static_cast<size_t>(*wbmSize));
			}
		}

		// Only supplied values: storing a value read before this mutex would
		// let a concurrent config() undo another's change.
		storeIfSet(settings.writeBufferManagerCostToCache, costToCache);

		if (allowStall) {
			settings.writeBufferManagerAllowStall.store(*allowStall, std::memory_order_relaxed);
			stallGeneration = ++settings.allowStallGeneration;
			if (manager) {
				if (*allowStall) {
					enableStallAfterDrain = true;
				} else {
					manager->SetAllowStall(false);
					DBStats::getInstance().disableWriteBufferManagerWatchdog();
				}
			}
		}
	}

	if (compactOnClose) {
		settings.compactOnClose = *compactOnClose;
	}
	storeIfSet(settings.lifecycleWaitSeconds, lifecycleWaitSeconds);
	storeIfSet(settings.occLockBucketCount, occLockBuckets);
	storeIfSet(settings.occValidateSerial, occValidateSerial);
	storeIfSet(settings.commitThreads, commitThreads);
	storeIfSet(settings.transactionLogAbortMarkers, abortMarkers);
	storeIfSet(settings.transactionLogWriteIntents, writeIntents);
	storeIfSet(settings.shutdownOnExit, shutdownOnExit);

	if (enableStallAfterDrain) {
		// A stalling manager can block a synchronous write on the JS thread
		// that alone releases idle pooled iterators' pinned memtables. The flag
		// stored above already stops new pooling; close what is idle now, then
		// let the manager stall unless a later change superseded this one (a
		// superseding enable runs its own drain). If the drain fails, the flag
		// keeps pooling off and the manager is left not stalling; it runs after
		// every other store, so that failure leaves the rest of the call applied.
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
