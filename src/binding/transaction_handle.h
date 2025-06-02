#ifndef __TRANSACTION_HANDLE_H__
#define __TRANSACTION_HANDLE_H__

#include <memory>
#include <mutex>
#include "db_handle.h"
#include "db_iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "util.h"

namespace rocksdb_js {

// forward declare DBHandle and DBIteratorOptions because of circular dependency
struct DBHandle;
struct DBIteratorOptions;

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
struct TransactionHandle final : Closable {
	TransactionHandle(std::shared_ptr<DBHandle> dbHandle, bool disableSnapshot = false);
	~TransactionHandle();

	void close() override;

	napi_value get(
		napi_env env,
		rocksdb::Slice& key,
		napi_value resolve,
		napi_value reject,
		std::shared_ptr<DBHandle> dbHandleOverride = nullptr
	);

	/**
	 * Gets the number of keys within a range or in the entire RocksDB database.
	 *
	 * @param itOptions - The iterator options.
	 * @param count - The number of keys.
	 * @param dbHandleOverride - Database handle override to use instead of the
	 * transaction's database handle when called via the `NativeDatabase` with
	 * the `transaction` property set.
	 */
	void getCount(
		DBIteratorOptions& itOptions,
		uint64_t& count,
		std::shared_ptr<DBHandle> dbHandleOverride = nullptr
	);

	rocksdb::Status getSync(
		rocksdb::Slice& key,
		std::string& result,
		std::shared_ptr<DBHandle> dbHandleOverride = nullptr
	);

	rocksdb::Status putSync(
		rocksdb::Slice& key,
		rocksdb::Slice& value,
		std::shared_ptr<DBHandle> dbHandleOverride = nullptr
	);

	rocksdb::Status removeSync(
		rocksdb::Slice& key,
		std::shared_ptr<DBHandle> dbHandleOverride = nullptr
	);

	std::shared_ptr<DBHandle> dbHandle;
	rocksdb::Transaction* txn;
	std::mutex commitMutex;
	uint32_t id;
};

} // namespace rocksdb_js

#endif