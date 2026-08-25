#ifndef __DB_REGISTRY_H__
#define __DB_REGISTRY_H__

#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>
#include "database/db_descriptor.h"
#include "database/db_handle.h"
#include "transaction/transaction.h"

namespace rocksdb_js {

/**
 * Lightweight key for the registry map. Composed of the two fields that
 * uniquely identify a database instance: path and whether it was opened
 * read-only. Using a dedicated key type lets callers look up entries before
 * a `DBDescriptor` has been opened.
 */
struct DBKey {
	std::string path;
	bool readOnly;

	bool operator==(const DBKey& other) const {
		return path == other.path && readOnly == other.readOnly;
	}
};

struct DBKeyHash {
	size_t operator()(const DBKey& key) const {
		return std::hash<std::string>()(key.path) ^
		       std::hash<bool>()(key.readOnly);
	}
};

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
	 * The last known file layout of every database this process has opened,
	 * kept after its descriptor is purged.
	 *
	 * `destroy()` accepts a CLOSED handle, and closing the last handle to a
	 * path takes the descriptor — and the registry entry the layout would be
	 * read from — with it. `blob_dir` could be recovered from the OPTIONS file;
	 * `db_paths` is written nowhere (RocksDB serializes it in its "not yet
	 * supported" block), so without this the destroy runs against a default
	 * `rocksdb::Options`, whose layout is "everything under the database
	 * directory": the directory goes, every tiered SST file stays, and the call
	 * reports success.
	 *
	 * Keyed by path and not by handle, so a handle closed before a later open
	 * appended a storage path cannot destroy from its own older copy. Entries
	 * are erased when the path is destroyed; a process that opens and closes
	 * many distinct databases retains one small record per path, which is the
	 * price of `destroy()` after `close()` working at all.
	 *
	 * Its own mutex, deliberately a leaf: `DestroyDB` reaches a descriptor's
	 * `layoutMutex` while holding `databasesMutex`, so anything recording a
	 * layout from under `layoutMutex` must not reach back for `databasesMutex`.
	 */
	std::unordered_map<std::string, DBFileLayout> knownLayouts;
	std::mutex knownLayoutsMutex;

	/**
	 * The singleton instance of the registry.
	 */
	static std::unique_ptr<DBRegistry> instance;

public:
	static void CloseDB(const std::shared_ptr<DBHandle> handle);
#ifdef DEBUG
	static void DebugLogDescriptorRefs();
#endif
	static void DestroyDB(const std::string& path);
	static void RecordLayout(const std::string& path, DBFileLayout layout);
	static void Init(napi_env env, napi_value exports);
	static std::unique_ptr<DBHandleParams> OpenDB(const std::string& path, const DBOptions& options);
	static void PurgeAll();
	static void PurgeIfUnreferenced(const std::string& path, bool readOnly);
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
