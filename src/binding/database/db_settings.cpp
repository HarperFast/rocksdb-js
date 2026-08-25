#include "database/db_settings.h"
#include <cmath>
#include <random>
#include "napi/macros.h"
#include "core/platform.h"
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

} // namespace

/**
 * The constructor for the DBSettings class.
 */
DBSettings::DBSettings():
	blockCacheSize(32 * 1024 * 1024), // 32MB (RocksDB default)
	blockCache(nullptr),
	blobCacheSize(0), // disabled by default (RocksDB default)
	blobCache(nullptr),
	blobCacheSizeExplicit(false),
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
 * Kept separate from the block cache so blob contents cannot evict
 * index/filter/data blocks — the two have very different access patterns.
 *
 * @returns The blob cache, or null when disabled.
 */
std::shared_ptr<rocksdb::Cache> DBSettings::getBlobCache() {
	const size_t size = blobCacheSize.load(std::memory_order_relaxed);
	if (size == 0) {
		return nullptr;
	}
	std::lock_guard<std::mutex> lock(blobCacheMutex);
	// Re-check under the lock: Config() may have disabled the cache between the
	// atomic load and acquiring the lock.
	if (blobCacheSize.load(std::memory_order_relaxed) == 0) {
		return nullptr;
	}
	if (!blobCache) {
		blobCache = rocksdb::NewLRUCache(blobCacheSize.load(std::memory_order_relaxed));
	}
	return blobCache;
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

	int64_t blockCacheSize = 0;
	napi_status status = rocksdb_js::getProperty(env, params, "blockCacheSize", blockCacheSize, true);
	const bool blockCacheSizeProvided = status == napi_ok;
	if (blockCacheSizeProvided && blockCacheSize < 0) {
		::napi_throw_range_error(env, nullptr, "Block cache size must be a positive integer or 0 to disable caching");
		return nullptr;
	}

	// "Provided" has to mean present AND not undefined/null: `{ blobCacheSize:
	// undefined }` is how an optional config object spells "not set", and
	// treating it as present rejected the whole call. Everything here is parsed
	// and validated BEFORE either cache is resized, so a rejected call leaves the
	// settings exactly as it found them rather than half-applied.
	bool blobCacheSizeProvided = false;
	{
		bool hasBlobCacheSize = false;
		NAPI_STATUS_THROWS(::napi_has_named_property(env, params, "blobCacheSize", &hasBlobCacheSize));
		if (hasBlobCacheSize) {
			napi_value blobCacheSizeValue;
			NAPI_STATUS_THROWS(
				::napi_get_named_property(env, params, "blobCacheSize", &blobCacheSizeValue));
			napi_valuetype blobCacheSizeType;
			NAPI_STATUS_THROWS(::napi_typeof(env, blobCacheSizeValue, &blobCacheSizeType));
			blobCacheSizeProvided =
				blobCacheSizeType != napi_undefined && blobCacheSizeType != napi_null;
		}
	}

	size_t requestedBlobCacheSize = 0;
	if (blobCacheSizeProvided) {
		// Distinguish absent from present-but-wrong-type: reading through
		// getProperty's status alone would silently drop `blobCacheSize: '512MB'`
		// and leave the cache disabled with no error.
		// Parse as a double and validate BEFORE narrowing: the int64 overload
		// truncates fractional values and wraps anything past its range.
		double requested = 0;
		if (rocksdb_js::getProperty(env, params, "blobCacheSize", requested, true) != napi_ok) {
			::napi_throw_type_error(env, nullptr, "Blob cache size must be a number");
			return nullptr;
		}
		if (!std::isfinite(requested) || requested != std::trunc(requested) || requested < 0 ||
			requested > 9007199254740991.0
		) {
			::napi_throw_range_error(
				env,
				nullptr,
				"Blob cache size must be a non-negative integer no larger than Number.MAX_SAFE_INTEGER"
			);
			return nullptr;
		}
		requestedBlobCacheSize = static_cast<size_t>(requested);
	}

	if (blockCacheSizeProvided) {
		settings.blockCacheSize = static_cast<size_t>(blockCacheSize);

		if (settings.blockCache) {
			settings.blockCache->SetCapacity(blockCacheSize);
		}
	}

	if (blobCacheSizeProvided || blockCacheSizeProvided) {
		// The latch is read and written under the same lock as the size it
		// guards: `config()` runs on every env's JS thread, so reading it outside
		// would let a derived update from one thread land after — and overwrite —
		// an explicit budget committed by another.
		std::lock_guard<std::mutex> lock(settings.blobCacheMutex);
		bool update = blobCacheSizeProvided;
		size_t configuredBlobCacheSize = requestedBlobCacheSize;
		if (blobCacheSizeProvided) {
			settings.blobCacheSizeExplicit.store(true, std::memory_order_relaxed);
		} else if (!settings.blobCacheSizeExplicit.load(std::memory_order_relaxed)) {
			// Derive only while the caller has never stated a budget: an unrelated
			// later `config({ blockCacheSize })` says nothing about blobs and must
			// not discard one.
			configuredBlobCacheSize = static_cast<size_t>(blockCacheSize) / 10;
			update = true;
		}

		if (update) {
			settings.blobCacheSize.store(configuredBlobCacheSize, std::memory_order_relaxed);

			// Resizes databases that already have the cache attached. A database
			// opened while the size was 0 has no blob_cache and is unaffected — see
			// the note on RocksDatabaseConfig.blobCacheSize.
			if (settings.blobCache) {
				settings.blobCache->SetCapacity(configuredBlobCacheSize);
			}
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
