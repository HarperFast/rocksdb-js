#ifndef __TRANSACTION_LOG_STORE_H__
#define __TRANSACTION_LOG_STORE_H__

#include <string>
#include <filesystem>
#include <map>
#include <set>
#include <mutex>
#include <atomic>
#include <functional>
#include "transaction_log_entry.h"
#include "transaction_log_file.h"

namespace rocksdb_js {

// forward declarations
struct TransactionLogEntryBatch;
struct TransactionLogFile;
struct MemoryMap;

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

	/**
	 * The set of transactions that have been written to log files in this store, but
	 * have not been committed (to RocksDB) yet. We track these because we don't want
	 * the transactions in the log to be visible until they are committed and consistent.
	 */
	std::set<uint64_t> uncommittedTransactionPositions;
	/**
	 * The next sequence position to use for a new transaction log entry.
	 */
	uint64_t nextSequencePosition = 0;

	PositionHandle* positionHandle;

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
	 * Writes a batch of transaction log entries to the store.
	 */
	uint64_t commit(TransactionLogEntryBatch& batch);

	/**
	 * Notifies the transaction log store that a RocksDB commit operation has finished.
	 */
	void commitFinished(uint64_t position);

	/**
	 * Queries the transaction log store.
	 */
	void query();

	/**
	 * Memory maps the transaction log file for the given sequence number.
	 **/
	MemoryMap* getMemoryMap(uint32_t sequenceNumber);

	/**
	* Get the log file size.
	**/
	uint32_t getLogFileSize(uint32_t sequenceNumber);

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
