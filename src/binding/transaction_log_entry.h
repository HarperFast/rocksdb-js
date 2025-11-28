#ifndef __TRANSACTION_LOG_ENTRY_H__
#define __TRANSACTION_LOG_ENTRY_H__

#include <memory>
#include "transaction_log_store.h"

namespace rocksdb_js {

struct TransactionLogStore;

/**
 * A log entry that is pending to be written to the transaction log on commit.
 */
struct TransactionLogEntry final {
	/**
	 * The transaction log store.
	 */
	std::shared_ptr<TransactionLogStore> store;

	/**
	 * The log entry data.
	 */
	std::unique_ptr<char[]> data;

	/**
	 * The size of the log entry.
	 */
	uint32_t size;

	/**
	 * Creates a new transaction log entry.
	 */
	TransactionLogEntry(
		std::shared_ptr<TransactionLogStore> store,
		std::unique_ptr<char[]> data,
		uint32_t size
	) :
		store(store),
		data(std::move(data)),
		size(size)
	{}
};

/**
 * A batch of transaction log entries with state tracking for partial writes
 * across multiple log files.
 */
struct TransactionLogEntryBatch final {
	/**
	 * The timestamp for this batch.
	 */
	double timestamp;

	/**
	 * The timestamp of the earliest active transaction associated with this
	 * batch's store.
	 */
	double earliestActiveTransactionTimestamp = 0;

	/**
	 * The vector of entries to write.
	 */
	std::vector<std::unique_ptr<TransactionLogEntry>> entries;

	/**
	 * The index of the current entry being written.
	 */
	uint32_t currentEntryIndex = 0;

	/**
	 * The number of bytes already written from the current entry's data
	 * (excluding the transaction header).
	 */
	uint32_t currentEntryBytesWritten = 0;

	/**
	 * Whether the transaction header for the current entry has been written.
	 */
	bool currentEntryHeaderWritten = false;

	TransactionLogEntryBatch(const double timestamp) :
		timestamp(timestamp)
	{}

	/**
	 * Adds a new entry to the batch.
	 */
	void addEntry(std::unique_ptr<TransactionLogEntry> entry) {
		this->entries.push_back(std::move(entry));
	}

	/**
	 * Checks if all entries have been written.
	 */
	bool isComplete() const {
		return this->currentEntryIndex >= this->entries.size();
	}
};

} // namespace rocksdb_js

#endif
