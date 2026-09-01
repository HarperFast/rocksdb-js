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
	 * and column-family drop rules are AGENTS invariant 18.
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
#ifdef DEBUG
	static void DebugLogDescriptorRefs();
#endif
	static void DestroyDB(const std::string& path);
	static rocksdb::Status DropColumnFamily(
		const std::shared_ptr<DBDescriptor>& descriptor,
		const std::string& columnName,
		rocksdb::ColumnFamilyHandle* column
	);
	static bool RecordLayout(const std::string& path, DBFileLayout layout, bool writableOpen);
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
