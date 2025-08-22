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
#include "util.h"

namespace rocksdb_js {

// forward declarations
struct DBDescriptor;
struct ListenerCallback;
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
	DBDescriptor(
		std::string path,
		DBMode mode,
		std::shared_ptr<rocksdb::DB> db,
		std::unordered_map<std::string, std::shared_ptr<rocksdb::ColumnFamilyHandle>> columns
	);
	~DBDescriptor();

	void attach(Closable* closable);
	void detach(Closable* closable);

	void lockCall(
		napi_env env,
		std::string key,
		napi_value callback,
		napi_deferred deferred,
		std::shared_ptr<DBHandle> owner
	);
	void lockEnqueueCallback(
		napi_env env,
		std::string key,
		napi_value callback,
		std::shared_ptr<DBHandle> owner,
		bool skipEnqueueIfExists,
		napi_deferred deferred,
		bool* isNewLock
	);
	bool lockExistsByKey(std::string key);
	bool lockReleaseByKey(std::string key);
	void lockReleaseByOwner(DBHandle* owner);
	void onCallbackComplete(const std::string& key);

	void transactionAdd(std::shared_ptr<TransactionHandle> txnHandle);
	std::shared_ptr<TransactionHandle> transactionGet(uint32_t id);
	void transactionRemove(uint32_t id);

	napi_value getUserSharedBuffer(
		napi_env env,
		std::string key,
		napi_value defaultBuffer,
		std::function<void()> finalizeFn = nullptr
	);

	napi_ref addListener(napi_env env, std::string key, napi_value callback);
	napi_value emit(napi_env env, std::string key, napi_value args);
	napi_value listeners(napi_env env, std::string key);
	napi_value removeListener(napi_env env, std::string key, napi_value callback);

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
	std::unordered_map<std::string, std::shared_ptr<rocksdb::ColumnFamilyHandle>> columns;

	/**
	 * Map of transaction id to transaction handle.
	 */
	std::unordered_map<uint32_t, std::shared_ptr<TransactionHandle>> transactions;

	/**
	 * Mutex to protect the transactions map and closables set.
	 */
	std::mutex txnsMutex;

	/**
	 * Set of closables to be closed when the descriptor is closed.
	 */
	std::set<Closable*> closables;

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
	 * Map of user shared buffers by key.
	 */
	std::unordered_map<std::string, std::shared_ptr<UserSharedBufferData>> userSharedBuffers;

	/**
	 * Mutex to protect the user shared buffers map.
	 */
	std::mutex userSharedBuffersMutex;

	/**
	 * Map of listener callbacks by key.
	 */
	std::unordered_map<std::string, std::vector<std::shared_ptr<ListenerCallback>>> listenerCallbacks;

	/**
	 * Mutex to protect the listener callbacks map.
	 */
	std::mutex listenerCallbacksMutex;
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
		: owner(owner), isRunning(false), env(env) {}

	~LockHandle() {
		while (!threadsafeCallbacks.empty()) {
			LockCallback lockCallback = threadsafeCallbacks.front();
			threadsafeCallbacks.pop();
			::napi_release_threadsafe_function(lockCallback.callback, napi_tsfn_release);
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
	std::atomic<bool> isRunning;

	/**
	 * The environment of the current callback.
	 */
	napi_env env;
};

/**
 * Contains the buffer and buffer size for a user shared buffer.
 */
struct UserSharedBufferData final {
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

	char* data;
	size_t size;
};

/**
 * Finalize data for user shared buffer ArrayBuffers to clean up map entries
 * when the ArrayBuffer is garbage collected.
 */
struct UserSharedBufferFinalizeData final {
	UserSharedBufferFinalizeData(
		const std::string& k,
		std::weak_ptr<DBDescriptor> d,
		std::shared_ptr<UserSharedBufferData> data,
		std::function<void()> fn = nullptr
	) : key(k), descriptor(d), sharedData(data), finalizeFn(fn) {}

	std::string key;
	std::weak_ptr<DBDescriptor> descriptor;
	std::shared_ptr<UserSharedBufferData> sharedData;
	std::function<void()> finalizeFn;
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

    ListenerCallback(napi_env env, napi_threadsafe_function tsfn, napi_ref callbackRef)
        : env(env), threadsafeCallback(tsfn), callbackRef(callbackRef) {}

	// move constructor
	ListenerCallback(ListenerCallback&& other) noexcept
		: env(other.env), threadsafeCallback(other.threadsafeCallback), callbackRef(other.callbackRef) {
		other.env = nullptr;
		other.threadsafeCallback = nullptr;
		other.callbackRef = 0;
	}

	// move assignment operator
	ListenerCallback& operator=(ListenerCallback&& other) noexcept {
		if (this != &other) {
			// clean up current resources first
			this->release();

			// transfer ownership
			env = other.env;
			threadsafeCallback = other.threadsafeCallback;
			callbackRef = other.callbackRef;

			// invalidate source
			other.env = nullptr;
			other.threadsafeCallback = nullptr;
			other.callbackRef = nullptr;
		}
		return *this;
	}

	// delete copy constructor and copy assignment to prevent accidental copying
	ListenerCallback(const ListenerCallback&) = delete;
	ListenerCallback& operator=(const ListenerCallback&) = delete;

	~ListenerCallback() {
		release();
	}

	void release() {
		if (this->callbackRef && this->env) {
			::napi_delete_reference(this->env, this->callbackRef);
			this->callbackRef = nullptr;
		}

		if (this->threadsafeCallback) {
			// ::napi_release_threadsafe_function(this->threadsafeCallback, napi_tsfn_release);
			this->threadsafeCallback = nullptr;
		}
	}
};

} // namespace rocksdb_js

#endif
