#ifndef __TRANSACTION_LOG_ENTRY_H__
#define __TRANSACTION_LOG_ENTRY_H__

#include <memory>
#include <node_api.h>
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
	 * The log entry data of the log entry owned by Node.js.
	 */
	char* data;

	/**
	 * The log entry data owned by rocksdb-js.
	 */
	std::unique_ptr<char[]> ownedData;

	/**
	 * The size of the log entry.
	 */
	uint32_t size;

	/**
	 * The node environment.
	 */
	napi_env env;

	/**
	 * A reference to the buffer of the log entry.
	 */
	napi_ref bufferRef;

	/**
	 * Creates a new transaction log entry by copy.
	 */
	TransactionLogEntry(
		std::shared_ptr<TransactionLogStore> store,
		std::unique_ptr<char[]> data,
		uint32_t size
	) :
		store(store),
		data(data.get()),
		ownedData(std::move(data)),
		size(size),
		env(nullptr),
		bufferRef(nullptr)
	{}

	/**
	 * Creates a new transaction log entry by reference.
	 */
	TransactionLogEntry(
		std::shared_ptr<TransactionLogStore> store,
		char* data,
		uint32_t size,
		napi_env env,
		napi_ref bufferRef
	) :
		store(store),
		data(data),
		ownedData(nullptr),
		size(size),
		env(env),
		bufferRef(bufferRef)
	{}

	/**
	 * Destroys the transaction log entry.
	 */
	~TransactionLogEntry() {
		// delete the buffer reference to allow the buffer to be GC'd
		if (this->bufferRef != nullptr) {
			::napi_delete_reference(this->env, this->bufferRef);
			this->bufferRef = nullptr;
		}
	}
};

/**
 * A batch of transaction log entries with state tracking for partial writes
 * across multiple log files.
 */
struct TransactionLogEntryBatch final {
	/**
	 * The timestamp for this batch.
	 */
	uint64_t timestamp;

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

	TransactionLogEntryBatch(const uint64_t timestamp) :
		timestamp(timestamp)
	{}

	/**
	 * Adds a new entry to the batch.
	 */
	void addEntry(std::unique_ptr<TransactionLogEntry> entry) {
		this->entries.push_back(std::move(entry));
	}

	/**
	 * Gets the remaining bytes to write from the current entry.
	 */
	uint32_t getCurrentEntryRemainingBytes() const {
		if (this->currentEntryIndex >= this->entries.size()) {
			return 0;
		}
		return this->entries[this->currentEntryIndex]->size - this->currentEntryBytesWritten;
	}

	/**
	 * Gets the total remaining bytes to write across all entries.
	 */
	uint32_t getTotalRemainingBytes() const {
		if (this->currentEntryIndex >= this->entries.size()) {
			return 0;
		}

		uint32_t total = this->getCurrentEntryRemainingBytes();
		for (uint32_t i = this->currentEntryIndex + 1; i < this->entries.size(); ++i) {
			total += this->entries[i]->size;
		}
		return total;
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
