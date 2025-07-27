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

struct LockHandle final {
	LockHandle(std::weak_ptr<DBHandle> owner)
		: owner(owner) {}
	std::set<napi_threadsafe_function> callbacks;
	std::weak_ptr<DBHandle> owner;
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
};

} // namespace rocksdb_js

#endif
