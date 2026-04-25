#ifndef __TRANSACTION_HANDLE_H__
#define __TRANSACTION_HANDLE_H__

#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>
#include "db_handle.h"
#include "db_iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "transaction_log_entry.h"
#include "util.h"
#include "verification_table.h"

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
	double startTimestamp;

	/**
	 * The state of the transaction.
	 */
	TransactionState state;

	/**
	 * The RocksDB transaction.
	 */
	rocksdb::Transaction* txn;

	/**
	 * A batch of log entries to write to the transaction log. It can only be
	 * set once via `addLogEntry()`.
	 */
	std::unique_ptr<TransactionLogEntryBatch> logEntryBatch;

	/**
	 * VT slots (ascending pointer order) that this transaction has registered
	 * intent for. Parallel to heldTrackers. Populated by registerIntent() in
	 * the libuv execute thread; cleared by releaseIntent().
	 */
	std::vector<std::atomic<uint64_t>*> sortedWriteSlots;

	/**
	 * LockTracker pointers held by this transaction — one per entry in
	 * sortedWriteSlots. Each tracker's refcount is incremented by 1 for our
	 * hold; decremented in releaseIntent().
	 */
	std::vector<LockTracker*> heldTrackers;

	/**
	 * A weak reference to the transaction log store this transaction is bound to.
	 * Once set, a transaction can only add entries to this specific log store.
	 */
	std::weak_ptr<TransactionLogStore> boundLogStore;

	/**
	 * The position of the beginning of the log entries that were written for this transaction.
	 * This is used for tracking of visible commits available in transaction log, once the transaction is successfully committed.
	 */
	LogPosition committedPosition;

	TransactionHandle(
		std::shared_ptr<DBHandle> dbHandle,
		napi_env env,
		napi_ref jsDatabaseRef,
		bool disableSnapshot = false
	);
	~TransactionHandle();

	void resetTransaction();

	/**
	 * Registers write intent for all keys in the current write batch.
	 * For each key, CAS-installs (or joins) a LockTracker in the VT slot,
	 * tagging it as "write in flight". Slots are acquired in ascending pointer
	 * order to prevent deadlock when multiple transactions contend.
	 *
	 * Called from the libuv execute thread before txn->Commit().
	 */
	void registerIntent(VerificationTable* vt, uintptr_t dbPtr);

	/**
	 * Releases all registered write intent. Decrements each LockTracker's
	 * holder count; the last holder CASes the slot back to 0. Frees trackers
	 * whose refcount reaches zero. Clears sortedWriteSlots and heldTrackers.
	 *
	 * Called from the libuv execute thread after txn->Commit() (success or
	 * IsBusy), and from close() to clean up orphaned intent.
	 */
	void releaseIntent();

	void addLogEntry(std::unique_ptr<TransactionLogEntry> entry);

	void close() override;

	napi_value get(
		napi_env env,
		std::string& key,
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
		rocksdb::PinnableSlice& result,
		rocksdb::ReadOptions& readOptions,
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