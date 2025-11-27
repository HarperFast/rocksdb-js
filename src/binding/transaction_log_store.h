#ifndef __TRANSACTION_LOG_STORE_H__
#define __TRANSACTION_LOG_STORE_H__

#include <string>
#include <filesystem>
#include <map>
#include <set>
#include <mutex>
#include <atomic>
#include <functional>
#include "rocksdb/db.h"
#include "transaction_log_entry.h"
#include "transaction_log_file.h"

namespace rocksdb_js {

// forward declarations
struct TransactionLogEntryBatch;
struct TransactionLogFile;
struct MemoryMap;

struct SequencePosition { // forward declaration doesn't work here because it is used in an array
	rocksdb::SequenceNumber rocksSequenceNumber;
	uint64_t position;
};

/**
* Structure to hold the last committed position of a transaction log file, that is exposed
* to JS through an external buffer with reference counting.
*/
struct PositionHandle {
	/**
	 * The full position of the last committed transaction, combining the sequence
	 * with the offset within the file, as a single 64-bit word that can be accessed
	 * from JS.
	 */
	uint64_t position;
	/**
	 * Reference counting for use with JS external buffers
	 */
	std::atomic<unsigned int> refCount = 1;
};

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
	 * The maximum size of a transaction log file in bytes before it is rotated
	 * to the next sequence number. A max size of 0 means no limit.
	 */
	uint32_t maxFileSize;

	/**
	 * The retention period for transaction logs in milliseconds.
	 */
	std::chrono::milliseconds retentionMs;

	/**
	 * The threshold for the transaction log file's last modified time to be
	 * older than the retention period before it is rotated to the next sequence
	 * number. A threshold of 0 means ignore age check.
	 */
	float maxAgeThreshold;

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

	/**
	 * The set of transactions that have been written to log files in this store, but
	 * have not been committed (to RocksDB) yet. We track these because we don't want
	 * the transactions in the log to be visible until they are committed and consistent.
	 */
	std::set<uint64_t> uncommittedTransactionPositions;

	/**
	 * An array of recent sequence positions to correlate a database sequence number with a transaction log position,
	 * so that when a flush event occurs, we can determine what part of the transaction log has been fully flushed
	 * to the RocksDB database.
	 * We are not attempting to track every single transaction log position and sequence number, there could be a very large
	 * number. Instead this an array where each n position represents an n^2 frequencies of correlations. This is enough
	 * that we won't lose more than half of what has to be replayed since the last flush.
	 */
	SequencePosition recentlyCommittedSequencePositions[20];

	unsigned int nextSequencePositionsCount = 0;

	TransactionLogFile* flushedTrackerFile = nullptr;

	/**
	 * The next sequence position to use for a new transaction log entry.
	 */
	uint64_t nextSequencePosition = 0;

	PositionHandle* positionHandle;

	TransactionLogStore(
		const std::string& name,
		const std::filesystem::path& path,
		const uint32_t maxFileSize,
		const std::chrono::milliseconds& retentionMs,
		const float maxAgeThreshold
	);

	~TransactionLogStore();

	/**
	 * Closes the transaction log store and all associated log files.
	 * This method waits for all active operations to complete before closing.
	 */
	void close();

	/**
	 * Notifies the transaction log store that a RocksDB commit operation has finished, and the transactions sequence number.
	 */
	void commitFinished(uint64_t position, rocksdb::SequenceNumber rocksSequenceNumber);

	/**
	 * Called when a database flush job is finished, so that we can record how much of the transaction log has been flushed to db.
	 */
	void databaseFlushed(rocksdb::SequenceNumber rocksSequenceNumber);

	/**
	 * Queries the transaction log store.
	 */
	void query();

	/**
	 * Memory maps the transaction log file for the given sequence number.
	 **/
	MemoryMap* getMemoryMap(uint32_t logSequenceNumber);

	/**
	* Get the log file size.
	**/
	uint32_t getLogFileSize(uint32_t logSequenceNumber);

	/**
	 * Get the shared represention object representing the last committed position.
	 **/
	PositionHandle* getLastCommittedPosition();

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
	uint64_t writeBatch(TransactionLogEntryBatch& batch);

	/**
	 * Load all transaction logs from a directory into a new transaction log
	 * store instance. If the retention period is set, any transaction logs that
	 * are older than the retention period will be removed.
	 *
	 * @param path The path to the transaction log store directory.
	 * @param maxFileSize The maximum size of a transaction log before it is
	 * rotated to the next sequence number.
	 * @param retentionMs The retention period for transaction logs.
	 * @returns The transaction log store.
	 */
	static std::shared_ptr<TransactionLogStore> load(
		const std::filesystem::path& path,
		const uint32_t maxFileSize,
		const std::chrono::milliseconds& retentionMs,
		const float maxAgeThreshold
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
