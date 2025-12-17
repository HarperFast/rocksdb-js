#ifndef __TRANSACTION_LOG_STORE_H__
#define __TRANSACTION_LOG_STORE_H__

#include <string>
#include <filesystem>
#include <map>
#include <set>
#include <memory>
#include <mutex>
#include <atomic>
#include <functional>
#include "rocksdb/db.h"
#include "transaction_log_entry.h"
#include "transaction_log_file.h"

#define RECENTLY_COMMITTED_POSITIONS_SIZE 24

namespace rocksdb_js {

// forward declarations
struct TransactionLogEntryBatch;
struct TransactionLogFile;
struct MemoryMap;

#define LOG_POSITION_SIZE 8

/**
* Structure to hold the last committed position of a transaction log file, that
* is exposed to JS through an external buffer.
*
* The size of this union is stored in `LOG_POSITION_SIZE`.
*/
union LogPosition {
	struct {
		/**
		* This offset relative to the start of the log file.
		*/
		uint32_t positionInLogFile;
		/**
		* The sequence number of the log file.
		*/
		uint32_t logSequenceNumber;
	};

	/**
	 * The full position of the last committed transaction, combining the sequence
	 * with the offset within the file, as a single 64-bit word that can be accessed
	 * from JS.
	 */
	double fullPosition;

	bool operator()( const LogPosition a, const LogPosition b ) const {
		// Comparing fullPosition is presumably faster than comparing the individual fields and should work on little-endian...
		// unless compilers can figure this out?
		return a.logSequenceNumber == b.logSequenceNumber ?
			a.positionInLogFile < b.positionInLogFile :
			a.logSequenceNumber < b.logSequenceNumber;
	};

	LogPosition() = default;

	LogPosition(uint32_t positionInLogFile, uint32_t logSequenceNumber) {
		this->positionInLogFile = positionInLogFile;
		this->logSequenceNumber = logSequenceNumber;
	}

	LogPosition(const LogPosition& other) = default;

	LogPosition& operator=(const LogPosition& other) {
		this->positionInLogFile = other.positionInLogFile;
		this->logSequenceNumber = other.logSequenceNumber;
		return *this;
	}
};

/**
* Holds a RocksDB sequence number (which Rocks uses to track the version of the database and is returned by flush events)
* and the corresponding position in the transaction log.
*/
struct SequencePosition { // forward declaration doesn't work here because it is used in an array
	rocksdb::SequenceNumber rocksSequenceNumber;
	LogPosition position;
};

struct TransactionLogStore final {
	/**
	 * The name of the transaction log store.
	 */
	std::string name;

	/**
	 * The timestamp of the most recent transaction log batch being written.
	 * This value is used in the transaction log file header timestamp.
	 */
	double latestTimestamp = 0;

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
	std::map<uint32_t, std::shared_ptr<TransactionLogFile>> sequenceFiles;

	/**
	 * The mutex to protect writing with the transaction log store.
	 */
	std::mutex writeMutex;

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
	std::set<LogPosition, LogPosition> uncommittedTransactionPositions;

	/**
	 * An array of recent sequence positions to correlate a database sequence number with a transaction log position,
	 * so that when a flush event occurs, we can determine what part of the transaction log has been fully flushed
	 * to the RocksDB database.
	 * We are not attempting to track every single transaction log position and sequence number, there could be a very large
	 * number. Instead this is an array where each n position represents an n^2 frequencies of correlations. This is enough
	 * that we won't lose more than half of what has to be replayed since the last flush.
	 */
	SequencePosition recentlyCommittedSequencePositions[RECENTLY_COMMITTED_POSITIONS_SIZE];

	/**
	 * The mutex to protect the transaction data sets.
	 */
	std::mutex dataSetsMutex;

	/**
	 * A counter for the number of recentlyCommittedSequencePositions updates we have made so that we can use 2^n modulus
	 * frequencies to assign to recentlyCommittedSequencePositions
	 */
	unsigned int nextSequencePositionsCount = 0;

	/**
	 * This file is used to track how much of the transaction log has been flushed to the database.
	 */
	TransactionLogFile* flushedTrackerFile = nullptr;

	/**
	 * The next sequence position to use for a new transaction log entry.
	 */
	LogPosition nextLogPosition = { 0, 0 };

	/**
	 * Data structure to hold the last committed position of a transaction log file, that is exposed
	 * to JS through an external buffer with reference counting.
	 */
	std::shared_ptr<LogPosition> lastCommittedPosition;

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
	void commitFinished(LogPosition position, rocksdb::SequenceNumber rocksSequenceNumber);

	/**
	 * Called when a database flush job is finished, so that we can record how much of the transaction log has been flushed to db.
	 */
	void databaseFlushed(rocksdb::SequenceNumber rocksSequenceNumber);
	// TODO: We should probably implement a databaseFlushStart so we can pin the current flush sequence number in memory for better accuracy
	// once we have added support for flush events

	/**
	 * Memory maps the transaction log file for the given sequence number.
	 **/
	std::weak_ptr<MemoryMap> getMemoryMap(uint32_t logSequenceNumber);

	/**
	* Get the log file size.
	**/
	uint64_t getLogFileSize(uint32_t logSequenceNumber);

	/**
	 * Get the shared represention object representing the last committed position.
	 **/
	std::weak_ptr<LogPosition> getLastCommittedPosition();

	/**
	 * Finds the transaction log file position with the oldest transaction that is equal to, or
	 * newer than, the provided timestamp.
	 */
	LogPosition findPositionByTimestamp(double timestamp);

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
	void writeBatch(TransactionLogEntryBatch& batch, LogPosition& logPosition);

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
	std::shared_ptr<TransactionLogFile> getLogFile(const uint32_t sequenceNumber);
};

} // namespace rocksdb_js

#endif
