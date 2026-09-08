#ifndef __DB_REGISTRY_H__
#define __DB_REGISTRY_H__

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include "database/db_descriptor.h"
#include "database/db_handle.h"
#include "transaction/transaction.h"

namespace rocksdb_js {

/**
 * Lightweight key for the registry map. Composed of the fields that uniquely
 * identify a database instance: path, whether it was opened read-only, and the
 * secondary workspace path (empty for non-secondary opens — a plain read-only
 * open and a secondary open of the same path are different RocksDB instance
 * kinds and must not share a descriptor, while two secondary opens with
 * different workspaces are distinct secondary instances by design; the
 * workspace is stored resolved — `Database::Open` runs it through
 * `resolveIdentityPath` — so two spellings of one directory are one key).
 * Using a dedicated key type lets callers look up entries before a
 * `DBDescriptor` has been opened. Every purge/close path must reconstruct the FULL key (use
 * `descriptorKey()`) — a partial key silently misses the entry and leaks the
 * descriptor and its open RocksDB.
 */
struct DBKey {
	/**
	 * The database's resolved filesystem identity (`DBDescriptor::identityPath`),
	 * never a caller's raw spelling: two spellings of one directory would
	 * otherwise hold two descriptors for it, and a second secondary instance
	 * would open one workspace twice (invariant 20).
	 */
	std::string path;
	bool readOnly;
	std::string secondaryPath;

	bool operator==(const DBKey& other) const {
		return path == other.path && readOnly == other.readOnly &&
			secondaryPath == other.secondaryPath;
	}
};

struct DBKeyHash {
	size_t operator()(const DBKey& key) const {
		return std::hash<std::string>()(key.path) ^
		       std::hash<bool>()(key.readOnly) ^
		       (std::hash<std::string>()(key.secondaryPath) << 1);
	}
};

/**
 * The one construction point for a live descriptor's registry key. Purge and
 * close paths must use this rather than hand-building a partial key.
 */
inline DBKey descriptorKey(const DBDescriptor& descriptor) {
	return DBKey{descriptor.identityPath, descriptor.readOnly, descriptor.secondaryPath};
}

/**
 * Entry in the database registry containing both the descriptor and a condition
 * variable for coordinating access to that specific path.
 */
struct DBRegistryEntry final {
	std::shared_ptr<DBDescriptor> descriptor;
	std::shared_ptr<std::condition_variable> condition;

	// Default constructor
	DBRegistryEntry() : condition(std::make_shared<std::condition_variable>()) {}

	DBRegistryEntry(std::shared_ptr<DBDescriptor> desc)
		: descriptor(std::move(desc)), condition(std::make_shared<std::condition_variable>()) {}
};


struct DBHandleParams final {
	std::shared_ptr<DBDescriptor> descriptor;
	std::shared_ptr<ColumnFamilyDescriptor> columnDescriptor;

	DBHandleParams(std::shared_ptr<DBDescriptor> descriptor, std::shared_ptr<ColumnFamilyDescriptor> columnDescriptor)
		: descriptor(std::move(descriptor)), columnDescriptor(std::move(columnDescriptor)) {}
};

/**
 * Tracks all RocksDB databases instances using a RocksDBDescriptor that
 * contains a weak reference to the database and column families.
 */
class DBRegistry final {
private:
	/**
	 * Private constructor.
	 */
	DBRegistry() = default;

	/**
	 * Map of database path to registry entry containing both the descriptor
	 * and condition variable for that path.
	 */
	std::unordered_map<DBKey, DBRegistryEntry, DBKeyHash> databases;

	/**
	 * Mutex to protect the databases map.
	 */
	std::mutex databasesMutex;

	/**
	 * Where every database this process has opened keeps its files: the latest
	 * blob directory per column family, and the canonical `db_paths` established
	 * by writable opens (`DBRegistry::RecordLayout`).
	 *
	 * `destroy()` accepts a CLOSED handle, and closing the last handle to a
	 * path takes the descriptor — and the registry entry the layout would be
	 * read from — with it. `db_paths` is written nowhere (RocksDB serializes it
	 * in its "not yet supported" block), so nothing on disk can put it back.
	 *
	 * Keyed by path rather than by handle and retained across `PurgeAll`, which is
	 * reached from the public `shutdown()`. Authority, default-marker lifetime,
	 * and column-family drop rules are AGENTS invariant 17.
	 *
	 * Its own mutex, deliberately a leaf: `DropColumnFamily` reaches a descriptor's
	 * `layoutMutex` while holding `databasesMutex`, so anything recording a
	 * layout from under `layoutMutex` must not reach back for a registry lock.
	 */
	std::unordered_map<std::string, DBFileLayout> knownLayouts;
	std::mutex knownLayoutsMutex;

	/**
	 * The singleton instance of the registry.
	 */
	static std::unique_ptr<DBRegistry> instance;

public:
	static void CloseDB(const std::shared_ptr<DBHandle> handle);

	/**
	 * Counts the live column families that draw on `wbm`, grouped by their
	 * effective `max_write_buffer_size_to_maintain` — the inventory the stall
	 * report needs to explain a full budget.
	 *
	 * Only descriptors that attached this exact manager are counted. A dropped
	 * column family still charging the manager through a live handle is counted
	 * too, and stops being counted once that last handle closes.
	 *
	 * Reads only cached integers under `databasesMutex` -> `columnsMutex` (the
	 * order `OpenDB` already establishes) and copies no `shared_ptr` out, so it
	 * cannot inflate a descriptor's use count and make a racing close skip its
	 * purge. Safe to call from a non-JS thread.
	 *
	 * Both mutex levels are acquired with `try_lock`, returning false rather than
	 * waiting: registry teardown or column-family creation can hold them across
	 * RocksDB work that waits out a write stall, so blocking here would silence
	 * the stall alarm during exactly the incident it exists to report.
	 */
	static bool CollectWriteBufferManagerInventory(
		const rocksdb::WriteBufferManager* wbm,
		uint64_t& columnFamilies,
		std::map<int64_t, uint64_t>& maxWriteBufferSizeToMaintain
	);
#ifdef DEBUG
	static void DebugLogDescriptorRefs();
#endif
	static void DestroyDB(const std::string& path);
	static rocksdb::Status DropColumnFamily(
		const std::shared_ptr<DBDescriptor>& descriptor,
		const std::string& columnName,
		rocksdb::ColumnFamilyHandle* column
	);
	static void AssertDbPathsExtendRetained(
		const std::string& path,
		const std::vector<rocksdb::DbPath>& requested,
		rocksdb::Env* env
	);
	static void ForgetLayout(const std::string& path);
	static void RecordLayout(const std::string& path, DBFileLayout layout, bool writableOpen);
	static void Init(napi_env env, napi_value exports);
	static std::unique_ptr<DBHandleParams> OpenDB(const std::string& path, const DBOptions& options);
	static void PurgeAll();
	static void PurgeIfUnreferenced(const DBKey& key);
	static napi_value RegistryStatus(napi_env env, napi_callback_info info);
	static void CloseTransactionsByEnv(napi_env env);
	static void RemoveListenersByEnv(napi_env env);
	static void ReleaseCommitCompletionsByEnv(napi_env env);
	static void ReleaseParkTimeoutsByEnv(napi_env env);
	static void Shutdown();
	static size_t Size();
};

} // namespace rocksdb_js

#endif
