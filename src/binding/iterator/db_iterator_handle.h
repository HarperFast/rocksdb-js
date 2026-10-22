#ifndef __DB_ITERATOR_HANDLE_H__
#define __DB_ITERATOR_HANDLE_H__

#include <atomic>
#include <mutex>
#include <string>
#include "database/db_handle.h"
#include "iterator/db_iterator.h"
#include "transaction/transaction_handle.h"
#include "napi/macros.h"
#include "transaction/transaction.h"
#include "core/platform.h"
#include "napi/helpers.h"
#include "napi/async.h"

namespace rocksdb_js {

/**
 * A handle that owns the RocksDB iterator and is used to clean up the iterator
 * when the Iterator JS object is garbage collected.
 */
struct DBIteratorHandle final : Closable, public std::enable_shared_from_this<DBIteratorHandle> {
	/**
	 * Initializes the iterator handle using a database handle.
	 */
	DBIteratorHandle(std::shared_ptr<DBHandle> dbHandle, DBIteratorOptions& options);

	/**
	 * Initializes the iterator handle using a transaction handle.
	 *
	 * `dbHandleOverride` selects the column family to iterate. A transaction
	 * belongs to a database rather than a single column family, so a caller
	 * iterating through another DBI in the same database passes that DBI's
	 * handle here. Defaults to the transaction's own column family.
	 */
	DBIteratorHandle(
		std::shared_ptr<TransactionHandle> txnHandle,
		DBIteratorOptions& options,
		std::shared_ptr<DBHandle> dbHandleOverride = nullptr,
		bool pooled = false
	);

	/**
	 * Cleans up the iterator handle.
	 */
	~DBIteratorHandle();

	/**
	 * Closes the iterator handle.
	 */
	void close() override;
	bool closeIfOpen();

	/**
	 * Resolves the encoded start and end keys and installs them as the RocksDB
	 * read bounds.
	 */
	void init(DBIteratorOptions& options);

	/**
	 * Counts the entries from the current position to the end of the range.
	 *
	 * Returns false when the descriptor began closing mid-scan. The scan is
	 * unbounded and its caller holds an `OperationGuard`, which `finishClose()`
	 * drains with an untimed wait, so it must abort rather than block teardown
	 * for the length of the range. The partial count is never reported.
	 */
	[[nodiscard]] bool countRemaining(uint64_t& count);

	/**
	 * Whether the iterator is positioned on a key inside the requested range.
	 */
	bool valid() const;

	void advance() {
		if (this->pooled && !this->boundedFallback) {
			this->lastKey.assign(this->iterator->key().data(), this->iterator->key().size());
			this->advanceFrom(rocksdb::Slice(this->lastKey));
		} else if (this->reverse) {
			this->iterator->Prev();
		} else {
			this->iterator->Next();
		}
	}

	/**
	 * Like `advance()`, given a copy of the current key that stays valid until
	 * this returns (the bytes just handed to JS), so a pooled iterator need not
	 * copy every key to know where a bounded fallback resumes.
	 */
	void advanceFrom(const rocksdb::Slice& currentKey);

	/**
	 * Caller holds `iteratorMutex` and has checked the handle is still open.
	 */
	void reuse(DBIteratorOptions& options);

	/**
	 * Ends the current range on a pooled handle: drops a bounded fallback
	 * iterator and restores the unbounded one for the next `reuse()`. Caller
	 * holds `iteratorMutex`.
	 */
	void endRange() noexcept;

	/**
	 * Whether the column family this pooled handle reads is still `target`'s
	 * current one. Only meaningful while the caller holds an `OperationGuard`.
	 */
	bool readsColumnOf(const std::shared_ptr<DBHandle>& target) const;

	std::shared_ptr<DBHandle> dbHandle;
	std::shared_ptr<TransactionHandle> txnHandle;
	bool exclusiveStart;
	bool inclusiveEnd;
	bool reverse;
	bool values;
	bool needsStableValueBuffer;
	bool enforceBounds;
	std::unique_ptr<rocksdb::Iterator> iterator;
	std::string startKeyStr;
	std::string endKeyStr;
	rocksdb::Slice startKey;
	rocksdb::Slice endKey;
	std::mutex iteratorMutex;
	bool transactionRegistered = false;

	/**
	 * Owned by the transaction's iterator pool: the RocksDB iterator has no
	 * read bounds (the handle enforces them) and is re-seeked for each range.
	 */
	const bool pooled;

	/**
	 * Sitting in the transaction's idle slots. Guarded by the transaction's
	 * `iteratorsMutex`; an idle handle is not counted in `activeIteratorCount`.
	 */
	bool idle = false;

	/**
	 * Attached to the descriptor's closables, so whoever closes it outside the
	 * JS finalizer detaches it afterwards.
	 */
	std::atomic<bool> attached{false};

	/**
	 * Removes the handle from the descriptor's closables if it was attached.
	 * Call after `close()`.
	 */
	void detachFromDescriptor() noexcept;

private:
	/**
	 * Seeks the iterator to the first or last key, or the start key if
	 * `exclusiveStart` is true.
	 */
	void seek();

	void setRange(DBIteratorOptions& options);

	/**
	 * Replaces the unbounded pooled iterator, which failed or hit its
	 * tombstone skip limit, with a bounded one for the rest of this range, so
	 * the caller sees exactly what a fresh iterator would: a bounded iterator
	 * never reads past the range. Positions after `resumeAfter`, or at the
	 * range start when it is null.
	 */
	void fallBackToBounded(const rocksdb::Slice* resumeAfter);

	std::weak_ptr<ColumnFamilyDescriptor> column;
	std::unique_ptr<rocksdb::Iterator> unboundedIterator;
	bool boundedFallback = false;
	std::string lastKey;
};

/**
 * Pool counters for the calling thread, for tests. Thread-local so workers
 * never contend on a shared cache line.
 */
struct IteratorPoolStats {
	uint64_t created = 0;
	uint64_t reused = 0;
	uint64_t fallbacks = 0;
};
extern thread_local IteratorPoolStats iteratorPoolStats;

/**
 * Whether transaction iterators may be pooled: RocksDB 11+ (the version
 * `test/native/transaction_iterator_reuse_test.cc` pins the reseek
 * semantics for) and not disabled with `ROCKSDB_JS_ITERATOR_POOL=0`.
 */
bool iteratorPoolEnabled();

} // namespace rocksdb_js

#endif
