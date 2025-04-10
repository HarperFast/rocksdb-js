#ifndef __DB_DESCRIPTOR_H__
#define __DB_DESCRIPTOR_H__

#include <memory>
#include <node_api.h>
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "db_options.h"
#include "transaction_handle.h"

namespace rocksdb_js {

struct TransactionHandle;

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

	void addTransaction(std::shared_ptr<TransactionHandle> txnHandle);
	std::shared_ptr<TransactionHandle> getTransaction(uint32_t id);
	void removeTransaction(std::shared_ptr<TransactionHandle> txnHandle);

	std::string path;
	DBMode mode;
	std::shared_ptr<rocksdb::DB> db;
	std::unordered_map<std::string, std::shared_ptr<rocksdb::ColumnFamilyHandle>> columns;
	std::unordered_map<uint32_t, std::shared_ptr<TransactionHandle>> transactions;
	std::mutex txnMutex;
};

} // namespace rocksdb_js

#endif
