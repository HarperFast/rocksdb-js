#ifndef __TRANSACTION_HANDLE_H__
#define __TRANSACTION_HANDLE_H__

#include "db_handle.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include <memory>
#include <mutex>

namespace rocksdb_js {

// forward declare DBHandle because of circular dependency
// (DBHandle -> DBDescriptor -> TransactionHandle)
struct DBHandle;

/**
 * A handle to a RocksDB transaction. This is used to keep the transaction
 * alive until the transaction is committed or aborted.
 *
 * It also has a reference to the database handle so that the transaction knows
 * which column family to use.
 *
 * This handle contains `get()`, `put()`, and `remove()` methods which are
 * shared between the `Database` and `Transaction` classes.
 */
struct TransactionHandle final {
	TransactionHandle(std::shared_ptr<DBHandle> dbHandle);
	~TransactionHandle();

	rocksdb::Status get(rocksdb::Slice& key, std::string& result, std::shared_ptr<DBHandle> dbHandleOverride = nullptr);
	rocksdb::Status getSync(rocksdb::Slice& key, std::string& result, std::shared_ptr<DBHandle> dbHandleOverride = nullptr);
	rocksdb::Status putSync(rocksdb::Slice& key, rocksdb::Slice& value, std::shared_ptr<DBHandle> dbHandleOverride = nullptr);
	rocksdb::Status removeSync(rocksdb::Slice& key, std::shared_ptr<DBHandle> dbHandleOverride = nullptr);
	void release();

	std::shared_ptr<DBHandle> dbHandle;
	rocksdb::Transaction* txn;
	std::mutex commitMutex;
	uint32_t id;
};

} // namespace rocksdb_js

#endif