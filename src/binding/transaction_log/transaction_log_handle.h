#ifndef __TRANSACTION_LOG_HANDLE_H__
#define __TRANSACTION_LOG_HANDLE_H__

#include <memory>
#include <string>
#include "database/db_handle.h"
#include "transaction_log_store.h"

namespace rocksdb_js {

// forward declaration
struct TransactionLogStoreStats;

struct TransactionLogHandle final : Closable {
	/**
	 * The database handle.
	 */
	std::weak_ptr<DBHandle> dbHandle;

	/**
	 * The transaction log store.
	 */
	std::weak_ptr<TransactionLogStore> store;

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

	std::shared_ptr<MemoryMap> getMemoryMap(uint32_t sequenceNumber);
	LogPosition findPosition(double timestamp);
	LogPosition getLastFlushed();
	uint64_t getLogFileSize(uint32_t sequenceNumber);
	std::weak_ptr<LogPosition> getLastCommittedPosition();

	/**
	 * Fills `out` with the store's statistics. Re-resolves the store if it has
	 * been released (mirroring addEntry). Returns false if the database has been
	 * closed and no store can be resolved.
	 */
	bool collectStats(TransactionLogStoreStats& out);

	/**
	 * Closes the transaction log handle.
	 */
	void close();

};

} // namespace rocksdb_js

#endif
