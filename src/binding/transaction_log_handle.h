#ifndef __TRANSACTION_LOG_HANDLE_H__
#define __TRANSACTION_LOG_HANDLE_H__

#include <memory>
#include <string>
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
	 */
	std::weak_ptr<TransactionLogStore> store;

	/**
	 * The name of the transaction log store.
	 */
	std::string logName;

	/**
	 * The transaction id.
	 */
	uint32_t transactionId;

	/**
	 * Creates a new transaction log handle.
	 */
	TransactionLogHandle(const std::shared_ptr<DBHandle>& dbHandle, const std::string& logName);

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

};

} // namespace rocksdb_js

#endif
