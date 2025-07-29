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

struct CallbackCompletionData {
	std::string key;
	DBDescriptor* descriptor;
	std::shared_ptr<std::atomic<bool>> valid;
	
	CallbackCompletionData(const std::string& k, DBDescriptor* d, std::shared_ptr<std::atomic<bool>> v) 
		: key(k), descriptor(d), valid(v) {}
};

struct LockHandle final {
	LockHandle(std::weak_ptr<DBHandle> owner)
		: owner(owner), isRunning(false) {}

	std::queue<napi_threadsafe_function> callbacks;
	std::queue<napi_ref> js_callbacks;  // Original JS callback references
	std::weak_ptr<DBHandle> owner;
	std::atomic<bool> isRunning;  // Atomic flag to track execution state

	// Remove fireNext method - it will be moved to DBDescriptor
};

/**
 * Descriptor for a RocksDB database, its column families, and any in-flight
 * transactions. The DBRegistry uses this to track active databases.
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
		bool* isNewLock,
		bool skipEnqueueIfExists
	);
	bool lockExists(std::string key);
	bool lockRelease(std::string key);
	void fireNextCallback(const std::string& key);
	void fireNextCallbackImmediate(napi_env env, const std::string& key);
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
