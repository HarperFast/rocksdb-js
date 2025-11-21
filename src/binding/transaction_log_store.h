#ifndef __TRANSACTION_LOG_STORE_H__
#define __TRANSACTION_LOG_STORE_H__

#include <string>
#include <filesystem>
#include <map>
#include <mutex>
#include <atomic>
#include <functional>
#include "transaction_log_entry.h"
#include "transaction_log_file.h"

namespace rocksdb_js {

// forward declarations
struct TransactionLogEntryBatch;
struct TransactionLogFile;

struct TransactionLogStore final {
	/**
	 * The name of the transaction log store.
	 */
	std::string name;

	/**
	 * The directory containing the transaction store's sequence log files.
	 */
	std::filesystem::path path;

	/**
	 * The maximum size of a transaction log file in bytes. A max size of 0
	 * means no limit.
	 */
	uint32_t maxSize;

	/**
	 * The retention period for transaction logs in milliseconds.
	 */
	std::chrono::milliseconds retentionMs;

	/**
	 * The current sequence number of the transaction log file.
	 */
	uint32_t currentSequenceNumber = 1;

	/**
	 * The next sequence number to use for the transaction log file.
	 */
	uint32_t nextSequenceNumber = 2;

	/**
	 * The map of sequence numbers to transaction log files.
	 */
	std::map<uint32_t, std::unique_ptr<TransactionLogFile>> sequenceFiles;

	/**
	 * The mutex to protect the transaction log store.
	 */
	std::mutex storeMutex;

	/**
	 * The flag indicating if the transaction log store is closing. Once a store
	 * is closed, it cannot be reopened.
	 *
	 * This is used to prevent concurrent closes and to ensure that no new
	 * operations are added to the store after it is closed.
	 */
	std::atomic<bool> isClosing = false;

	TransactionLogStore(
		const std::string& name,
		const std::filesystem::path& path,
		const uint32_t maxSize,
		const std::chrono::milliseconds& retentionMs
	);

	~TransactionLogStore();

	/**
	 * Closes the transaction log store and all associated log files.
	 * This method waits for all active operations to complete before closing.
	 */
	void close();

	/**
	 * Queries the transaction log store.
	 */
	void query();

	/**
	 * Purges transaction logs.
	 */
	void purge(
		std::function<void(const std::filesystem::path&)> visitor = nullptr,
		const bool all = false
	);

	/**
	 * Registers a log file for the given sequence number.
	 *
	 * @param path The path to the log file to register.
	 * @param sequenceNumber The sequence number of the log file to register.
	 */
	void registerLogFile(const std::filesystem::path& path, const uint32_t sequenceNumber);

	/**
	 * Writes a batch of transaction log entries to the store.
	 */
	void writeBatch(TransactionLogEntryBatch& batch);

	/**
	 * Load all transaction logs from a directory into a new transaction log store
	 * instance. If the retention period is set, any transaction logs that are older
	 * than the retention period will be removed.
	 *
	 * @param path The path to the transaction log store directory.
	 * @param maxSize The maximum size of a transaction log before it is rotated to
	 * the next sequence number.
	 * @param retentionMs The retention period for transaction logs.
	 * @returns The transaction log store.
	 */
	static std::shared_ptr<TransactionLogStore> load(
		const std::filesystem::path& path,
		const uint32_t maxSize,
		const std::chrono::milliseconds& retentionMs
	);

private:
	/**
	 * Opens a log file for the given sequence number. If the log file does not
	 * exist, it will be created.
	 *
	 * Important! This method must be called with `storeMutex` already locked.
	 *
	 * @param sequenceNumber The sequence number of the log file to open.
	 * @returns The log file.
	 */
	TransactionLogFile* getLogFile(const uint32_t sequenceNumber);
};

} // namespace rocksdb_js

#endif
