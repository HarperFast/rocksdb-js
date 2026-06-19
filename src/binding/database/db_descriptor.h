#ifndef __DB_DESCRIPTOR_H__
#define __DB_DESCRIPTOR_H__

#include <memory>
#include <node_api.h>
#include <atomic>
#include <queue>
#include <set>
#include <functional>
#include "rocksdb/db.h"
#include "rocksdb/statistics.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/options_util.h"
#include "options/db_options.h"
#include "transaction/transaction_handle.h"
#include "transaction_log/transaction_log_store_registry.h"
#include "core/platform.h"
#include "napi/event_emitter.h"
#include "napi/helpers.h"
#include "napi/async.h"

namespace rocksdb_js {

// forward declarations
struct ColumnFamilyDescriptor;
struct DBDescriptor;
struct LockHandle;
struct TransactionHandle;
struct UserSharedBufferData;
struct UserSharedBufferFinalizeData;

/**
 * Custom deleter for RocksDB that waits for any background compaction to
 * complete before destroying the database instance. Compaction is triggered
 * by DBDescriptor::close() before this deleter runs.
 */
struct DBDeleter {
	void operator()(rocksdb::DB* db) const {
		if (db) {
			DEBUG_LOG("DBDeleter::operator() Waiting for compaction and closing database\n");
			// Wait for any background compaction to complete and close the database
			rocksdb::WaitForCompactOptions options;
			options.close_db = true;
			db->WaitForCompact(options);
			DEBUG_LOG("DBDeleter::operator() Closed database, deleting\n");
			delete db;
			DEBUG_LOG("DBDeleter::operator() Deleted database\n");
		}
	}
};

/**
 * Descriptor for a RocksDB database, its column families, and any in-flight
 * transactions. The DBRegistry uses this to track active databases and reuse
 * RocksDB instances.
 */
struct DBDescriptor final : public std::enable_shared_from_this<DBDescriptor> {
	/**
	 * The path of the database.
	 */
	std::string path;

	/**
	 * The mode of the database: optimistic or pessimistic. `DBRegistry`
	 * defaults this to `DBMode::Optimistic`.
	 */
	DBMode mode;

	/**
	 * Whether the database was opened in readonly mode via
	 * `DB::OpenForReadOnly`. When true, write operations are not supported.
	 */
	bool readOnly;

	/**
	 * The RocksDB database instance.
	 */
	std::shared_ptr<rocksdb::DB> db;

	/**
	 * Map of column family name to column family handle.
	 */
	std::unordered_map<std::string, std::shared_ptr<ColumnFamilyDescriptor>> columns;

	/**
	 * Mutex to protect the columns map. Column families can be unregistered on
	 * drop (see `unregisterColumnFamily`) while other threads iterate the map:
	 * the JS thread via the `columns` getter or `DBRegistry::OpenDB`, libuv
	 * worker threads via `flush()`, and a closing thread via `close()`. Lock
	 * ordering: when both are held, `DBRegistry::databasesMutex` is acquired
	 * BEFORE `columnsMutex`; `columnsMutex` is never held while acquiring the
	 * registry mutex.
	 */
	std::mutex columnsMutex;

	/**
	 * The RocksDB statistics instance.
	 */
	std::shared_ptr<rocksdb::Statistics> statistics;

	/**
	 * Map of transaction id to transaction handle.
	 */
	std::unordered_map<uint32_t, std::shared_ptr<TransactionHandle>> transactions;

	/**
	 * Atomic counter for generating unique transaction IDs for this RocksDB
	 * instance. Sadly we cannot use RocksDB's transaction IDs because they are
	 * implementation-dependent and are assigned lazily with a default of 0
	 * causing collisions in the transactions map.
	 */
	std::atomic<uint32_t> nextTransactionId{1};

	/**
	 * Mutex to protect the transactions map and closables set.
	 */
	std::mutex txnsMutex;

	/**
	 * Set of closables to be closed when the descriptor is closed.
	 */
	std::map<Closable*, std::weak_ptr<Closable>> closables;

	/**
	 * Mutex to protect the locks map.
	 */
	std::mutex locksMutex;

	/**
	 * Map of lock key to lock handle.
	 */
	std::unordered_map<std::string, std::shared_ptr<LockHandle>> locks;

	/**
	 * A flag used by the `DBRegistry` to indicate the database is being closed,
	 * this descriptor should not be used, and it should create a new
	 * descriptor.
	 */
	std::atomic<bool> closing{false};

	/**
	 * Counter tracking in-flight database operations. close() uses
	 * atomic::wait() to block until this reaches zero.
	 */
	std::atomic<uint32_t> operationsInFlight{0};

	/**
	 * Mutex to prevent concurrent compaction operations.
	 */
	std::mutex compactMutex;

	/**
	 * Per-database event emitter. Listeners attached here only fire for events
	 * emitted on this descriptor. Cleaned up per-DBHandle on close and fully
	 * cleared when the descriptor itself closes.
	 */
	EventEmitter events;

private:
	DBDescriptor(
		const std::string& path,
		const DBOptions& options,
		std::shared_ptr<rocksdb::DB> db,
		std::unordered_map<std::string, std::shared_ptr<ColumnFamilyDescriptor>>&& columns,
		std::shared_ptr<rocksdb::Statistics> statistics
	);

public:
	static std::shared_ptr<DBDescriptor> open(const std::string& path, const DBOptions& options);
	~DBDescriptor();

	void close();
	bool isClosing() const { return this->closing.load(); }

	/**
	 * Atomically transitions the descriptor into the closing state. Returns
	 * true if this call performed the transition (the caller now owns the
	 * close and must run `finishClose()`), false if it was already closing.
	 *
	 * Lets `DBRegistry::CloseDB` publish the closing state while still holding
	 * `databasesMutex`, so a concurrent `OpenDB` (which inspects `isClosing()`
	 * under the same lock) waits instead of handing the descriptor to a new
	 * handle that would then be closed out from under it.
	 */
	bool beginClose() { return !this->closing.exchange(true); }

	/**
	 * Performs the actual close work (flush, close handles, release resources).
	 * Only valid after `beginClose()` returned true; `close()` is the all-in-one
	 * entry point that claims and then runs this.
	 */
	void finishClose();

	void attach(std::shared_ptr<Closable> closable);
	void detach(std::shared_ptr<Closable> closable);

	/**
	 * Gets a single statistic value.
	 *
	 * @example
	 * ```typescript
	 * const stat = db.getStat('rocksdb.block.cache.miss');
	 * ```
	 */
	napi_value getStat(napi_env env, const std::string& statName);

	/**
	 * Gets all statistics.
	 *
	 * @example
	 * ```typescript
	 * const stats = db.getStats();
	 * ```
	 */
	bool getStats(napi_env env, bool all, napi_value* result);

	void lockCall(
		napi_env env,
		std::string& key,
		napi_value callback,
		napi_deferred deferred,
		std::shared_ptr<DBHandle> owner
	);
	void lockEnqueueCallback(
		napi_env env,
		std::string& key,
		napi_value callback,
		std::shared_ptr<DBHandle> owner,
		bool skipEnqueueIfExists,
		napi_deferred deferred,
		bool* isNewLock
	);
	bool lockExistsByKey(std::string& key);
	bool lockReleaseByKey(std::string& key);
	void lockReleaseByOwner(DBHandle* owner);
	void onCallbackComplete(const std::string& key);

	void transactionAdd(std::shared_ptr<TransactionHandle> txnHandle);
	std::shared_ptr<TransactionHandle> transactionGet(uint32_t id);
	void transactionRemove(std::shared_ptr<TransactionHandle> txnHandle);
	uint32_t transactionGetNextId();

	/**
	 * Removes a dropped column family from the columns map (under
	 * `columnsMutex`) so a later open-by-name creates a fresh column family
	 * instead of reusing the dangling dropped handle. DBHandles still holding
	 * the descriptor keep it alive via their shared_ptr; only the by-name
	 * lookup is removed.
	 *
	 * @param columnName The name of the dropped column family.
	 */
	void unregisterColumnFamily(const std::string& columnName);

	/**
	 * Creates a new user shared buffer or returns an existing one.
	 *
	 * @param env The environment of the current callback.
	 * @param key The key of the user shared buffer.
	 * @param defaultBuffer The default buffer to use if the user shared buffer does
	 * not exist.
	 * @param callbackRef An optional callback reference to remove the listener when
	 * the user shared buffer is garbage collected.
	 */
	napi_value getUserSharedBuffer(
		napi_env env,
		std::string& key,
		std::shared_ptr<DBHandle> dbHandle,
		napi_value defaultBuffer,
		napi_ref callbackRef = nullptr
	);

	napi_ref addListener(napi_env env, std::string& key, napi_value callback, std::weak_ptr<DBHandle> owner);
	bool notify(std::string key, ListenerData* data);
	napi_value listeners(napi_env env, std::string& key);
	napi_value removeListener(napi_env env, std::string& key, napi_value callback);
	void removeListenersByOwner(DBHandle* owner);
	void removeListenersByEnv(napi_env env);

	napi_value listTransactionLogStores(napi_env env);
	napi_value purgeTransactionLogs(napi_env env, napi_value options);
	std::shared_ptr<TransactionLogStore> resolveTransactionLogStore(const std::string& name);
	rocksdb::Status flush();

	/**
	 * Compacts a range of keys in the specified column family. This method is
	 * thread-safe and uses a mutex to prevent concurrent compaction operations.
	 *
	 * @param column The column family to compact.
	 * @param start The start key of the range (nullptr for beginning).
	 * @param end The end key of the range (nullptr for end).
	 * @returns The status of the compaction operation.
	 */
	rocksdb::Status compactRange(
		rocksdb::ColumnFamilyHandle* column,
		const rocksdb::Slice* start,
		const rocksdb::Slice* end
	);
};

/**
 * State to pass into `napi_call_threadsafe_function()` for a lock callback.
 */
struct LockCallbackCompletionData final {
	LockCallbackCompletionData(const std::string& k, std::weak_ptr<DBDescriptor> d, napi_deferred def = nullptr)
		: key(k), descriptor(d), deferred(def) {}

	/**
	 * The key of the lock.
	 */
	std::string key;

	/**
	 * The descriptor of the database.
	 */
	std::weak_ptr<DBDescriptor> descriptor;

	/**
	 * Optional deferred promise to resolve when the lock is released (for withLock).
	 */
	napi_deferred deferred;

	/**
	 * Flag indicating resolve/reject callback has been called and prevent
	 * the callback from being called again.
	 */
	std::atomic<bool> completed{false};
};

/**
 * Holds a threadsafe callback and its associated deferred promise (if any).
 */
struct LockCallback final {
	LockCallback(napi_threadsafe_function callback, napi_deferred deferred = nullptr)
		: callback(callback), deferred(deferred) {}

	napi_threadsafe_function callback;
	napi_deferred deferred;
};

/**
 * Tracks a queue of callbacks for a lock, the lock owner, and whether a
 * callback is currently running to prevent multiple callbacks from being
 * executed at the same time.
 */
struct LockHandle final {
	LockHandle(std::weak_ptr<DBHandle> owner, napi_env env)
		: owner(owner), env(env) {}

	~LockHandle() {
		while (!threadsafeCallbacks.empty()) {
			LockCallback lockCallback = threadsafeCallbacks.front();
			threadsafeCallbacks.pop();
			NAPI_STATUS_THROWS_VOID(::napi_release_threadsafe_function(lockCallback.callback, napi_tsfn_release));
		}
	}

	/**
	 * A queue of threadsafe callbacks to fire in sequence.
	 */
	std::queue<LockCallback> threadsafeCallbacks;

	/**
	 * The owner of the lock. Used to release any locks owned by a database
	 * instance that is being closed.
	 */
	std::weak_ptr<DBHandle> owner;

	/**
	 * Flag indicating whether the current callback is running. It's used when
	 * a new callback is enqueued to determine if we should call the callback
	 * immediately (false) or add it to the queue (true).
	 */
	std::atomic<bool> isRunning = false;

	/**
	 * The environment of the current callback.
	 */
	napi_env env;
};

/**
 * Contains the buffer and buffer size for a user shared buffer.
 */
struct UserSharedBufferData final {
	/**
	 * The data of the user shared buffer.
	 */
	char* data;

	/**
	 * The size of the user shared buffer.
	 */
	size_t size;

	UserSharedBufferData(void* sourceData, size_t size) : size(size) {
		this->data = new char[size];
		::memcpy(this->data, sourceData, size);
	}

	~UserSharedBufferData() {
		delete[] this->data;
	}

	// delete copy constructor and copy assignment to prevent accidental copying
	UserSharedBufferData(const UserSharedBufferData&) = delete;
	UserSharedBufferData& operator=(const UserSharedBufferData&) = delete;
};

/**
 * Finalize data for user shared buffer ArrayBuffers to clean up map entries
 * when the ArrayBuffer is garbage collected.
 *
 * Holds a strong reference to the underlying `UserSharedBufferData` so the
 * backing storage outlives any ColumnFamilyDescriptor / DBDescriptor teardown
 * until JS releases every retained ArrayBuffer for the key. The weak pointers
 * to `DBHandle` / `ColumnFamilyDescriptor` are used for opportunistic cleanup
 * (removing listeners, erasing map entries) when those are still alive.
 */
struct UserSharedBufferFinalizeData final {
	std::string key;
	std::weak_ptr<DBHandle> dbHandle;
	std::weak_ptr<ColumnFamilyDescriptor> columnDescriptor;
	std::shared_ptr<UserSharedBufferData> sharedData;
	napi_ref callbackRef;

	UserSharedBufferFinalizeData(
		const std::string& k,
		std::weak_ptr<DBHandle> d,
		std::weak_ptr<ColumnFamilyDescriptor> c,
		std::shared_ptr<UserSharedBufferData> data,
		napi_ref callbackRef = nullptr
	) : key(k), dbHandle(d), columnDescriptor(c), sharedData(std::move(data)), callbackRef(callbackRef) {}
};

/**
 * Contains the column family handle and map of user shared buffers.
 */
struct ColumnFamilyDescriptor final {
	/**
	 * The column family handle.
	 */
	std::shared_ptr<rocksdb::ColumnFamilyHandle> column;

	/**
	 * Map of user shared buffers by key.
	 */
	std::unordered_map<std::string, std::shared_ptr<UserSharedBufferData>> userSharedBuffers;

	/**
	 * Mutex to protect the user shared buffers map.
	 */
	std::mutex userSharedBuffersMutex;

	ColumnFamilyDescriptor(std::shared_ptr<rocksdb::ColumnFamilyHandle> column) : column(column) {}

	~ColumnFamilyDescriptor() {
		DEBUG_LOG("%p ColumnFamilyDescriptor::~ColumnFamilyDescriptor destroying column family descriptor\n", this);
	}

	void releaseUserSharedBuffer(const std::string& key, const std::shared_ptr<UserSharedBufferData>& sharedData) {
		DEBUG_LOG("%p ColumnFamilyDescriptor::releaseUserSharedBuffer releasing user shared buffer (use_count: %ld) for key:", this, sharedData.use_count());
		DEBUG_LOG_KEY_LN(key);

		std::lock_guard<std::mutex> lock(this->userSharedBuffersMutex);
		DEBUG_LOG("%p ColumnFamilyDescriptor::releaseUserSharedBuffer locked user shared buffers map (size: %ld)\n", this, this->userSharedBuffers.size());
		auto iter = this->userSharedBuffers.find(key);
		DEBUG_LOG("%p ColumnFamilyDescriptor::releaseUserSharedBuffer created iterator\n", this);
		if (iter != this->userSharedBuffers.end() && iter->second == sharedData) {
			DEBUG_LOG("%p ColumnFamilyDescriptor::releaseUserSharedBuffer found user shared buffer (use_count: %ld) for key:", this, sharedData.use_count());
			DEBUG_LOG_KEY_LN(key);

			// Each live external ArrayBuffer keeps one strong ref via its
			// finalize data; the map entry is a second strong ref. If the
			// current finalizer's ref + the map entry are the only two left,
			// no other ArrayBuffers exist for this key and the map entry is
			// safe to evict here. Otherwise leave the entry in place so future
			// getUserSharedBuffer() calls keep returning the same mapping.
			if (sharedData.use_count() <= 2) {
				this->userSharedBuffers.erase(key);
				DEBUG_LOG("%p ColumnFamilyDescriptor::releaseUserSharedBuffer removed user shared buffer for key:", this);
				DEBUG_LOG_KEY_LN(key);
			}
		} else {
			DEBUG_LOG("%p ColumnFamilyDescriptor::releaseUserSharedBuffer user shared buffer not found for key:", this);
			DEBUG_LOG_KEY_LN(key);
		}
	}
};

} // namespace rocksdb_js

#endif
