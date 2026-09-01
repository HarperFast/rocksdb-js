#ifndef __TRANSACTION_LOG_ENTRY_H__
#define __TRANSACTION_LOG_ENTRY_H__

#include <memory>
#include "transaction_log_file.h"
#include "transaction_log_store.h"
#include "core/encoding.h"

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
		char* data,
		uint32_t size
	) :
		store(store)
	{
		this->size = size + TRANSACTION_LOG_ENTRY_HEADER_SIZE;
		this->data = std::make_unique<char[]>(this->size);

		// write the transaction header (13 bytes)
		// skip timestamp for now, it will be written when the batch is written
		writeUint32BE(this->data.get() + 8, static_cast<uint32_t>(size)); // data length
		writeUint8(this->data.get() + 12, 0); // flags
		::memcpy(this->data.get() + 13, data, size);
	}
};

/**
 * A batch of transaction log entries. A batch is placed wholly in one segment
 * so the last-entry flag always closes the transaction in that same file.
 */
struct TransactionLogEntryBatch final {
	/**
	 * The timestamp for this batch: the transaction's clock snapshot at
	 * addLogEntry time. On a stamping-activated store, writeBatch replaces it
	 * with the claimed receiver-local stamp (the batch key).
	 */
	double timestamp;

	/**
	 * Whether `timestamp` came from a caller-supplied setTimestamp() rather
	 * than the receiver's monotonic clock — claim provenance for the skew
	 * check (core/local_stamp.h).
	 */
	bool timestampFromCaller = false;

	/**
	 * The vector of entries to write.
	 */
	std::vector<std::unique_ptr<TransactionLogEntry>> entries;

	/**
	 * The index of the current entry being written.
	 */
	uint32_t currentEntryIndex = 0;

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
