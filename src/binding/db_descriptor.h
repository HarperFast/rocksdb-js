#ifndef __DB_DESCRIPTOR_H__
#define __DB_DESCRIPTOR_H__

#include <memory>
#include <node_api.h>
#include <atomic>
#include <queue>
#include <set>
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "db_options.h"
#include "transaction_handle.h"
#include "util.h"

namespace rocksdb_js {

// forward declarations
struct TransactionHandle;
struct DBDescriptor;
struct LockHandle;

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
		std::shared_ptr<DBHandle> owner
	);
	void lockEnqueueCallback(
		napi_env env,
		std::string key,
		napi_value callback,
		std::shared_ptr<DBHandle> owner,
		bool skipEnqueueIfExists,
		bool* isNewLock
	);
	bool lockExistsByKey(std::string key);
	bool lockReleaseByKey(std::string key);
	void lockReleaseByOwner(DBHandle* owner);
	void onCallbackComplete(const std::string& key);

	void transactionAdd(std::shared_ptr<TransactionHandle> txnHandle);
	std::shared_ptr<TransactionHandle> transactionGet(uint32_t id);
	void transactionRemove(uint32_t id);

	std::string path;
	DBMode mode;
	std::shared_ptr<rocksdb::DB> db;
	std::unordered_map<std::string, std::shared_ptr<rocksdb::ColumnFamilyHandle>> columns;
	std::unordered_map<uint32_t, std::shared_ptr<TransactionHandle>> transactions;
	std::mutex mutex;
	std::set<Closable*> closables;
	std::mutex locksMutex;
	std::unordered_map<std::string, std::shared_ptr<LockHandle>> locks;
	std::shared_ptr<std::atomic<bool>> valid;
};

/**
 * State to pass into `napi_call_threadsafe_function()` for a lock callback.
 */
struct LockCallbackCompletionData final {
	LockCallbackCompletionData(const std::string& k, std::weak_ptr<DBDescriptor> d)
		: key(k), descriptor(d) {}

	/**
	 * The key of the lock.
	 */
	std::string key;

	/**
	 * The descriptor of the database.
	 */
	std::weak_ptr<DBDescriptor> descriptor;

	/**
	 * Flag indicating resolve/reject callback has been called and prevent
	 * the callback from being called again.
	 */
	std::atomic<bool> completed{false};
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
			napi_threadsafe_function threadsafeCallback = threadsafeCallbacks.front();
			threadsafeCallbacks.pop();
			::napi_release_threadsafe_function(threadsafeCallback, napi_tsfn_release);
		}
	}

	/**
	 * A queue of threadsafe callbacks to fire in sequence.
	 */
	std::queue<napi_threadsafe_function> threadsafeCallbacks;

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

} // namespace rocksdb_js

#endif
