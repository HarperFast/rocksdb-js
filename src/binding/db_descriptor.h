#ifndef __DB_DESCRIPTOR_H__
#define __DB_DESCRIPTOR_H__

#include <memory>
#include <node_api.h>
#include <atomic>
#include <queue>
#include <set>
#include <functional>
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/options_util.h"
#include "db_options.h"
#include "transaction_handle.h"
#include "transaction_log_store.h"
#include "util.h"

namespace rocksdb_js {

// forward declarations
struct ColumnFamilyDescriptor;
struct DBDescriptor;
struct ListenerCallback;
struct ListenerData;
struct LockHandle;
struct TransactionHandle;
struct UserSharedBufferData;
struct UserSharedBufferFinalizeData;

/**
 * Custom deleter for RocksDB that calls WaitForCompact with close_db=true
 * before destroying the database instance.
 */
struct DBDeleter {
	void operator()(rocksdb::DB* db) const {
		if (db) {
			DEBUG_LOG("DBDeleter::operator() Compacting and closing database\n");
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
	 * The RocksDB database instance.
	 */
	std::shared_ptr<rocksdb::DB> db;

	/**
	 * Map of column family name to column family handle.
	 */
	std::unordered_map<std::string, std::shared_ptr<ColumnFamilyDescriptor>> columns;

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
	 * Map of listener callbacks by key.
	 */
	std::unordered_map<std::string, std::vector<std::shared_ptr<ListenerCallback>>> listenerCallbacks;

	/**
	 * Mutex to protect the listener callbacks map.
	 */
	std::mutex listenerCallbacksMutex;

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

	/**
	 * The path to the transaction logs.
	 */
	std::string transactionLogsPath;

	/**
	 * Map of transaction logs by name.
	 */
	std::map<std::string, std::shared_ptr<TransactionLogStore>> transactionLogStores;

	/**
	 * Mutex to protect the transaction logs map.
	 */
	std::mutex transactionLogMutex;

private:
	DBDescriptor(
		const std::string& path,
		const DBOptions& options,
		std::shared_ptr<rocksdb::DB> db,
		std::unordered_map<std::string, std::shared_ptr<ColumnFamilyDescriptor>>&& columns
	);

	void discoverTransactionLogStores();

public:
	static std::shared_ptr<DBDescriptor> open(const std::string& path, const DBOptions& options);
	~DBDescriptor();

	void close();
	bool isClosing() const { return this->closing.load(); }

	void attach(std::shared_ptr<Closable> closable);
	void detach(std::shared_ptr<Closable> closable);

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

	napi_value listTransactionLogStores(napi_env env);
	napi_value purgeTransactionLogs(napi_env env, napi_value options);
	std::shared_ptr<TransactionLogStore> resolveTransactionLogStore(const std::string& name);
	rocksdb::Status flush();
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
 * Finalize data for user shared buffer ArrayBuffers to clean up map entries
 * when the ArrayBuffer is garbage collected.
 */
struct UserSharedBufferFinalizeData final {
	std::string key;
	std::weak_ptr<DBHandle> dbHandle;
	std::weak_ptr<ColumnFamilyDescriptor> columnDescriptor;
	std::weak_ptr<UserSharedBufferData> sharedData;
	napi_ref callbackRef;

	UserSharedBufferFinalizeData(
		const std::string& k,
		std::weak_ptr<DBHandle> d,
		std::weak_ptr<ColumnFamilyDescriptor> c,
		std::weak_ptr<UserSharedBufferData> data,
		napi_ref callbackRef = nullptr
	) : key(k), dbHandle(d), columnDescriptor(c), sharedData(data), callbackRef(callbackRef) {}
};

/**
 * A struct to hold the serialized arguments to emit to the listener callbacks.
 */
struct ListenerData final {
	std::string args;

	ListenerData(size_t size) : args(size, '\0') {}
	ListenerData(const ListenerData& other) : args(other.args) {}
};

/**
 * A wrapper for a listener callback that holds the threadsafe callback, env,
 * and callback reference. The callback reference is used to remove the listener
 * callback and the env is used for cleanup.
 */
struct ListenerCallback final {
	/**
	 * The environment of the current callback.
	 */
	napi_env env;

	/**
	 * The threadsafe function of the current callback. This is what is
	 * actually called when the event is emitted.
	 */
	napi_threadsafe_function threadsafeCallback;

	/**
	 * The callback reference of the current callback. This is used to remove
	 * the listener callback.
	 */
	napi_ref callbackRef;

	/**
	 * The DBHandle that owns this listener (weak reference to avoid cycles).
	 */
	std::weak_ptr<DBHandle> owner;

	ListenerCallback(napi_env env, napi_ref callbackRef, std::weak_ptr<DBHandle> owner)
		: env(env), threadsafeCallback(nullptr), callbackRef(callbackRef), owner(owner) {}
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
		DEBUG_LOG("%p ColumnFamilyDescriptor::~ColumnFamilyDescriptor destroying column family descriptor", this);
	}

	void releaseUserSharedBuffer(const std::string& key, std::shared_ptr<UserSharedBufferData> sharedData) {
		DEBUG_LOG("%p ColumnFamilyDescriptor::releaseUserSharedBuffer releasing user shared buffer (use_count: %ld) for key:", this, sharedData.use_count());
		DEBUG_LOG_KEY_LN(key);

		std::lock_guard<std::mutex> lock(this->userSharedBuffersMutex);
		DEBUG_LOG("%p ColumnFamilyDescriptor::releaseUserSharedBuffer locked user shared buffers map (size: %ld)", this, this->userSharedBuffers.size());
		auto iter = this->userSharedBuffers.find(key);
		DEBUG_LOG("%p ColumnFamilyDescriptor::releaseUserSharedBuffer created iterator", this);
		if (iter != this->userSharedBuffers.end() && iter->second == sharedData) {
			DEBUG_LOG("%p ColumnFamilyDescriptor::releaseUserSharedBuffer found user shared buffer (use_count: %ld) for key:", this, sharedData.use_count());
			DEBUG_LOG_KEY_LN(key);

			// check if this shared_ptr is about to become the last reference
			// (map entry + this finalizer's copy = 2, after finalizer exits only map = 1)
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
