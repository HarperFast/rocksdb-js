#ifndef __DB_DESCRIPTOR_H__
#define __DB_DESCRIPTOR_H__

#include <memory>
#include <node_api.h>
#include <set>
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "db_options.h"
#include "transaction_handle.h"
#include "util.h"

namespace rocksdb_js {

// forward declare TransactionHandle because of circular dependency
struct TransactionHandle;

// Forward declaration
struct DBDescriptor;

/**
 * Data for a lock callback completion.
 */
struct LockCallbackCompletionData final {
	std::string key;
	DBDescriptor* descriptor;
	std::shared_ptr<std::atomic<bool>> valid;

	LockCallbackCompletionData(const std::string& k, DBDescriptor* d, std::shared_ptr<std::atomic<bool>> v)
		: key(k), descriptor(d), valid(v) {}
};

/**
 * Tracks a lock's callbacks, owner, and execution state for a specific key.
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

		while (!jsCallbacks.empty()) {
			napi_ref jsCallbackRef = jsCallbacks.front();
			jsCallbacks.pop();
			::napi_delete_reference(env, jsCallbackRef);
		}
	}

	std::queue<napi_threadsafe_function> threadsafeCallbacks;
	std::queue<napi_ref> jsCallbacks;
	std::weak_ptr<DBHandle> owner;
	std::atomic<bool> isRunning;
	napi_env env;
};

/**
 * Descriptor for a RocksDB database, its column families, and any in-flight
 * transactions. The DBRegistry uses this to track active databases and reuse
 * RocksDB instances.
 */
struct DBDescriptor final {
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

} // namespace rocksdb_js

#endif
