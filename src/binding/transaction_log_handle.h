#ifndef __TRANSACTION_LOG_HANDLE_H__
#define __TRANSACTION_LOG_HANDLE_H__

#include <memory>
#include <string>
#include <vector>
#include "db_handle.h"
#include "transaction_log_store.h"

namespace rocksdb_js {

struct TransactionLogHandle final : Closable {
	/**
	 * The database handle.
	 */
	std::weak_ptr<DBHandle> dbHandle;

	/**
	 * The transaction log store.
	 *
	 * Held as a strong reference rather than a weak_ptr so steady-state
	 * read methods don't pay the atomic refcount of weak_ptr::lock() per
	 * call — under high concurrent QPS, that CAS on a single shared
	 * cache line is a real bottleneck. The handle keeps the store alive
	 * for as long as the handle itself is alive; addEntry re-resolves
	 * to a fresh store if this one has been marked closing.
	 */
	std::shared_ptr<TransactionLogStore> store;

	/**
	 * The name of the transaction log store.
	 */
	std::string logName;

	/**
	 * Whether the associated DBHandle's DBDescriptor is opened in readonly mode.
	 */
	bool readOnly;

	/**
	 * The transaction id.
	 */
	uint32_t transactionId;

	/**
	 * Per-handle snapshot of all log files in the store, sorted by
	 * sequence number ascending. Refreshed only when the store's
	 * `filesVersion` counter advances (rotation, registration, purge);
	 * the steady state hits this cache for every query without ever
	 * touching dataSetsMutex.
	 *
	 * The shared_ptr keeps cached files alive even if they're purged
	 * from the store's sequenceFiles map — their mmap and on-disk path
	 * remain valid (Unix unlinks are deferred until last open closes).
	 *
	 * Each handle has its own cache, so concurrent subscribers don't
	 * contend on shared state.
	 */
	std::shared_ptr<const std::vector<std::shared_ptr<TransactionLogFile>>> cachedFiles;
	uint64_t cachedFilesVersion = 0;

	/**
	 * Creates a new transaction log handle.
	 */
	TransactionLogHandle(
		const std::shared_ptr<DBHandle>& dbHandle,
		const std::string& logName,
		bool readOnly
	);

	/**
	 * Destroys the transaction log handle.
	 */
	~TransactionLogHandle();

	/**
	 * Adds an entry to the transaction log.
	 */
	void addEntry(
		uint32_t transactionId,
		char* data,
		uint32_t size
	);

	std::weak_ptr<MemoryMap> getMemoryMap(uint32_t sequenceNumber);
	LogPosition findPosition(double timestamp);
	LogPosition getLastFlushed();
	uint64_t getLogFileSize(uint32_t sequenceNumber);
	std::weak_ptr<LogPosition> getLastCommittedPosition();
	/**
	 * Closes the transaction log handle.
	 */
	void close();

private:
	/**
	 * Refreshes `cachedFiles` if the store's `filesVersion` has advanced.
	 * Cheap atomic load when the cache is up to date.
	 */
	void refreshFilesCache(TransactionLogStore& store);

	/**
	 * Look up a file by sequence number in `cachedFiles`. Returns nullptr if
	 * the cache is empty or the sequence isn't present. O(log N) binary
	 * search since the snapshot is sorted ascending by sequence number.
	 *
	 * Caller must have called refreshFilesCache() first.
	 */
	std::shared_ptr<TransactionLogFile> lookupCachedFile(uint32_t sequenceNumber);
};

} // namespace rocksdb_js

#endif
