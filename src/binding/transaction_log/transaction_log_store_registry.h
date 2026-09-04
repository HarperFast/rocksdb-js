#ifndef __TRANSACTION_LOG_STORE_REGISTRY_H__
#define __TRANSACTION_LOG_STORE_REGISTRY_H__

#include <chrono>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <string>
#include <node_api.h>
#include "transaction_log_store.h"

namespace rocksdb_js {

/**
 * Configuration for transaction log stores associated with a database path.
 */
struct TransactionLogStoreConfig final {
	/**
	 * The path to the transaction logs directory.
	 */
	std::string transactionLogsPath;

	/**
	 * The caller's spelling used only in API results. Filesystem access uses the
	 * captured identity above so a later symlink/CWD change cannot redirect it.
	 */
	std::string transactionLogsDisplayPath;

	/**
	 * The threshold for the transaction log file's last modified time to be
	 * older than the retention period before it is rotated to the next sequence
	 * number. A threshold of 0 means ignore age check.
	 */
	float transactionLogMaxAgeThreshold;

	/**
	 * The maximum size of a transaction log file in bytes before it is rotated
	 * to the next sequence number. A max size of 0 means no limit.
	 */
	uint32_t transactionLogMaxSize;

	/**
	 * The retention period of transaction logs in milliseconds.
	 */
	std::chrono::milliseconds transactionLogRetentionMs;
};

/**
 * Entry in the transaction log store registry containing the stores map,
 * configuration, and reference count.
 */
struct TransactionLogStoreRegistryEntry final {
	/**
	 * Map of transaction log store name to store instance.
	 */
	std::map<std::string, std::shared_ptr<TransactionLogStore>> stores;

	/**
	 * Mutex to protect the stores map.
	 */
	std::mutex storesMutex;

	/**
	 * Configuration for this database path's transaction log stores.
	 */
	TransactionLogStoreConfig config;

	/**
	 * Reference count tracking how many DBDescriptors are using this entry.
	 */
	size_t refCount = 0;

	TransactionLogStoreRegistryEntry() = default;

	TransactionLogStoreRegistryEntry(const TransactionLogStoreConfig& cfg)
		: config(cfg), refCount(1) {}
};

/**
 * Result of a CoolTransactionLogs() pass.
 */
struct TransactionLogCoolResult final {
	/**
	 * The number of memory maps that had pages advised cold.
	 */
	uint32_t maps = 0;

	/**
	 * The total number of file-backed bytes advised cold across all maps.
	 */
	uint64_t bytes = 0;
};

/**
 * Global registry that manages transaction log stores by database path.
 * This ensures that transaction log stores are shared across multiple
 * DBDescriptors (e.g., write and read-only) for the same database path.
 */
class TransactionLogStoreRegistry final {
private:
	/**
	 * Private constructor for singleton.
	 */
	TransactionLogStoreRegistry() = default;

	/**
	 * Map of database path to registry entry. Uses shared_ptr so entries can
	 * be safely accessed after releasing entriesMutex by taking a copy.
	 */
	std::unordered_map<std::string, std::shared_ptr<TransactionLogStoreRegistryEntry>> entries;

	/**
	 * Mutex to protect the entries map.
	 */
	std::mutex entriesMutex;

	/**
	 * The singleton instance of the registry.
	 */
	static std::unique_ptr<TransactionLogStoreRegistry> instance;

public:
	/**
	 * Initializes the singleton instance.
	 */
	static void Init();

	/**
	 * Shuts down the registry and closes all stores.
	 */
	static void Shutdown();

	// Every `dbPath` below is `DBDescriptor::identityPath` — the database path
	// resolved to filesystem identity ONCE at open — never raw caller text. The
	// entries map is what makes the read-only/writable store guards meet, so two
	// spellings of one directory must land on one entry; and resolving here
	// instead would consult the process CWD on every call, so a `process.chdir()`
	// would remap a live handle's key (a writable `ResolveStore` would then create
	// a second store outside the database, and `Unregister` would leak the first
	// entry's stores). See DBDescriptor::identityPath.

	/**
	 * Registers a DBDescriptor for the given database path. Increments the
	 * reference count for the path. If this is the first descriptor for the
	 * path, creates a new entry with the given configuration.
	 *
	 * @param dbPath The database path.
	 * @param config The transaction log store configuration.
	 */
	static void Register(const std::string& dbPath, const TransactionLogStoreConfig& config);

	/**
	 * Throws when a writable open would reuse transaction log stores that were
	 * loaded read-only (no tail recovery ran, so writer appends would land past
	 * a torn tail — invariant 5); the decision is made per live store, since the
	 * path-global entry can outlive the handle that created it. Call BEFORE
	 * constructing the descriptor: a
	 * throw from Register itself would run the half-built descriptor's close()
	 * and its Unregister would decrement the read-only entry's refcount it
	 * never incremented. Opens are serialized by DBRegistry's databasesMutex,
	 * so check-then-register cannot interleave with another open.
	 */
	static void EnsureWritableRegistrationSafe(const std::string& dbPath, bool readOnly);

	/**
	 * Unregisters a DBDescriptor for the given database path. Decrements the
	 * reference count. If the reference count reaches zero, closes and removes
	 * all transaction log stores for that path.
	 *
	 * @param dbPath The database path.
	 */
	static void Unregister(const std::string& dbPath);

	/**
	 * Discovers existing transaction log stores in the transaction logs
	 * directory for the given database path.
	 *
	 * @param dbPath The database path.
	 * @param callerReadOnly Whether the OPENING handle's database is
	 * read-only/secondary. Like ResolveStore, the caller's mode decides — the
	 * path-global entry is shared by every handle on the path and outlives the
	 * one that created it, so a writer that has since closed must not make this
	 * discovery load stores writably (retention purge and recoverTail()
	 * truncation against what may be a live primary's logs — invariant 5).
	 */
	static void DiscoverStores(const std::string& dbPath, bool callerReadOnly);

	/**
	 * Resolves (finds or creates) a transaction log store by name for the
	 * given database path.
	 *
	 * @param dbPath The database path.
	 * @param name The name of the transaction log store.
	 * @param callerReadOnly Whether the resolving handle's database is
	 * read-only/secondary. The CALLER's mode — never the path-global entry,
	 * which is shared by every handle on the path — decides whether a missing
	 * store may be created: a read-only caller gets only stores live in this
	 * process and never mkdirs into what may by now be a foreign live primary's
	 * tree. "Live in this process" includes a store an in-process writer
	 * created after this handle opened; only a cross-process primary's new
	 * stores are invisible until reopen.
	 * @returns The transaction log store, or null for a read-only caller whose
	 * store is not resident.
	 */
	static std::shared_ptr<TransactionLogStore> ResolveStore(
		const std::string& dbPath,
		const std::string& name,
		bool callerReadOnly
	);

	/**
	 * Lists all transaction log store names for the given database path.
	 *
	 * @param env The N-API environment.
	 * @param dbPath The database path.
	 * @returns A JavaScript array of store names.
	 */
	static napi_value ListStores(napi_env env, const std::string& dbPath);

	/**
	 * Purges transaction logs for the given database path.
	 *
	 * @param env The N-API environment.
	 * @param dbPath The database path.
	 * @param options Purge options (before, destroy, name).
	 * @returns A JavaScript array of removed file paths.
	 */
	static napi_value PurgeStores(napi_env env, const std::string& dbPath, napi_value options);

	/**
	 * Gets all stores for the given database path. Used by flush event
	 * listeners to notify stores of flush events.
	 *
	 * @param dbPath The database path.
	 * @returns A vector of transaction log stores (may be empty if path not found).
	 */
	static std::vector<std::shared_ptr<TransactionLogStore>> GetStores(const std::string& dbPath);

	/**
	 * Gets the number of entries in the registry (for debugging/testing).
	 */
	static size_t Size();

	/**
	 * Advises the kernel that the file-backed pages of every mapped transaction
	 * log across all registered database paths are cold (MADV_COLD), so they are
	 * reclaimed first under memory pressure. Intended to be called periodically
	 * by a single timer driven from the host (e.g. Harper's main thread); since
	 * this registry is a process-global singleton shared across all worker
	 * threads, one call covers every worker's maps. Safe to call concurrently
	 * with reads and writes — see TransactionLogFile::adviseCold().
	 *
	 * @returns The number of maps cooled and total bytes advised.
	 */
	static TransactionLogCoolResult CoolTransactionLogs();
};

} // namespace rocksdb_js

#endif
