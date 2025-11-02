#ifndef __TRANSACTION_HANDLE_H__
#define __TRANSACTION_HANDLE_H__

#include <memory>
#include <mutex>
#include <unordered_map>
#include "db_handle.h"
#include "db_iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "transaction_log_entry.h"
#include "util.h"

namespace rocksdb_js {

struct DBHandle;
struct DBIteratorOptions;
struct TransactionLogStore;

/**
 * Transaction state enumeration
 */
enum class TransactionState {
	Pending,    // Transaction is active and can accept operations
	Committing, // Transaction is in the process of committing (async only)
	Committed,  // Transaction has been successfully committed
	Aborted     // Transaction has been aborted/rolled back
};

/**
 * A handle to a RocksDB transaction. This is used to keep the transaction
 * alive until the transaction is committed or aborted.
 *
 * It also has a reference to the database handle so that the transaction knows
 * which column family to use.
 *
 * This handle contains `get()`, `put()`, and `remove()` methods which are
 * shared between the `Database` and `Transaction` classes.
 *
 * Each instance of this class is bound to a JavaScript `Transaction` instance.
 * Since a JS instance is bound to a single thread, we don't need any mutexes.
 */
struct TransactionHandle final : Closable, AsyncWorkHandle, std::enable_shared_from_this<TransactionHandle> {
	/**
	 * The database handle.
	 */
	std::shared_ptr<DBHandle> dbHandle;

	/**
	 * The node environment. This is needed to release the database reference
	 * when the transaction is closed.
	 */
	napi_env env;

	/**
	 * A reference to the main `rocksdb_js` exports object.
	 */
	napi_ref jsDatabaseRef;

	/**
	 * Whether to disable snapshots.
	 */
	bool disableSnapshot;

	/**
	 * The transaction id assigned by the database descriptor.
	 */
	uint32_t id;

	/**
	 * Whether a snapshot has been set.
	 */
	bool snapshotSet;

	/**
	 * The start timestamp of the transaction.
	 */
	uint64_t startTimestamp;

	/**
	 * The state of the transaction.
	 */
	TransactionState state;

	/**
	 * The RocksDB transaction.
	 */
	rocksdb::Transaction* txn;

	/**
	 * A map of store names to their log entries.
	 * This groups entries by store to enable efficient batch commits per store.
	 */
	std::unordered_map<std::string, std::vector<std::unique_ptr<TransactionLogEntry>>> entriesByStore;

	TransactionHandle(
		std::shared_ptr<DBHandle> dbHandle,
		napi_env env,
		napi_ref jsDatabaseRef,
		bool disableSnapshot = false
	);
	~TransactionHandle();

	void addLogEntry(std::unique_ptr<TransactionLogEntry> entry);

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
};

} // namespace rocksdb_js

#endif