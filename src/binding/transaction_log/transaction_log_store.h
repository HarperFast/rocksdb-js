#ifndef __TRANSACTION_LOG_STORE_H__
#define __TRANSACTION_LOG_STORE_H__

#include <string>
#include <filesystem>
#include <fstream>
#include <algorithm>
#include <map>
#include <vector>
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

	bool operator<(const LogPosition& other) const {
		return logSequenceNumber == other.logSequenceNumber ?
			positionInLogFile < other.positionInLogFile :
			logSequenceNumber < other.logSequenceNumber;
	}

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

/**
 * A plain (no N-API) snapshot of a transaction log store's statistics. Produced
 * by `TransactionLogStore::collectStats()` and converted to a JavaScript object
 * by the N-API layer. Keeping this free of N-API makes it unit-testable under
 * GoogleTest and reusable by both the per-log (`log.getStats()`) and the
 * DB-level aggregate (`db.getStats()` `txnlog.*` keys) paths.
 *
 * All sizes are in bytes; timestamps are milliseconds since the Unix epoch.
 */
struct TransactionLogStoreStats {
	// identity
	std::string name;
	std::string path;

	// files (on disk)
	uint32_t fileCount = 0;
	uint32_t currentSequenceNumber = 0;
	uint32_t oldestSequenceNumber = 0;
	uint64_t totalSizeBytes = 0;
	uint32_t currentFileSize = 0;

	// memory
	uint64_t mappedBytes = 0;  // virtual address space (current file maps full maxFileSize on POSIX)
	uint64_t overlayBytes = 0; // POSIX file-backed overlay portion (0 on Windows)
	uint32_t activeMaps = 0;

	// transaction state
	int32_t pendingTransactions = 0;
	uint32_t uncommittedTransactions = 0;

	// positions / recovery
	LogPosition nextLogPosition = { 0, 0 };
	LogPosition lastCommittedPosition = { 0, 0 };
	bool hasLastCommittedPosition = false;
	LogPosition lastFlushedPosition = { 0, 0 };
	uint64_t replayGapBytes = 0;

	// purge / retention gauges
	double oldestFileAgeMs = 0;
	uint32_t purgeableFiles = 0;
	uint32_t retainedUnflushedFiles = 0;

	// lifetime totals
	uint64_t transactionsWritten = 0;
	uint64_t entriesWritten = 0;
	uint64_t bytesWritten = 0;
	uint64_t rotations = 0;
	uint64_t filesPurged = 0;
	uint64_t bytesPurged = 0;
	uint64_t purgeRuns = 0;
	uint64_t databaseFlushes = 0;
	uint64_t writeFailures = 0;
	double lastPurgeMs = 0;

	// config
	uint32_t maxFileSize = 0;
	uint64_t retentionMs = 0;
	float maxAgeThreshold = 0;
};

/**
 * The canonical set of summarized `txnlog.*` statistics exposed by
 * `db.getStats()`, `db.getStat()`, and the `stats.tickers` catalog. Defined
 * once here as an X-macro — `X(jsKey, TransactionLogStoreStats field)` — so all
 * three stay in sync. Each value is the sum of the named
 * TransactionLogStoreStats field across all of a database's logs.
 * `txnlog.logCount` is the number of logs and is handled separately (it is not
 * a per-store field sum).
 */
#define TRANSACTION_LOG_SUMMARY_LOG_COUNT_KEY "txnlog.logCount"
#define TRANSACTION_LOG_SUMMARY_STATS(X) \
	X("txnlog.fileCount", fileCount) \
	X("txnlog.totalSizeBytes", totalSizeBytes) \
	X("txnlog.mappedBytes", mappedBytes) \
	X("txnlog.overlayBytes", overlayBytes) \
	X("txnlog.activeMaps", activeMaps) \
	X("txnlog.pendingTransactions", pendingTransactions) \
	X("txnlog.uncommittedTransactions", uncommittedTransactions) \
	X("txnlog.transactionsWritten", transactionsWritten) \
	X("txnlog.bytesWritten", bytesWritten) \
	X("txnlog.replayGapBytes", replayGapBytes)

/**
 * A single file to include in a transaction log backup, produced by
 * `TransactionLogStore::snapshotForBackup()`. `byteLimit` is the number of bytes
 * to copy from the start of the file — for the current (actively-appended) log
 * file this is the atomic `size` captured at snapshot time, a stable and
 * complete prefix (the log is append-only and `size` is bumped only after the
 * bytes land). `mtime` is the source's on-disk modified time and MUST be
 * preserved on the destination: the store derives file age (rotation/retention)
 * from mtime, so a restored file with a fresh mtime would break retention.
 * `immutable` is true for rotated log files, which are never rewritten and so
 * can be hard-linked instead of copied.
 *
 * `inlineContents`, when non-empty, holds the file's bytes captured at snapshot
 * time; consumers write these instead of re-reading `sourcePath`. This is used
 * for `txn.state`, which is rewritten in place (not appended) — re-reading it at
 * copy time could observe a newer flushed position that points past the extents
 * captured for the log files, so its content is frozen here.
 */
struct TransactionLogBackupEntry final {
	std::string relativeName;              // e.g. "3.txnlog" or "txn.state"
	std::filesystem::path sourcePath;      // absolute path to the live file
	uint64_t byteLimit;                    // bytes to copy from offset 0
	std::filesystem::file_time_type mtime; // source mtime, to preserve on the copy
	bool immutable;                        // rotated log file → safe to hard-link
	std::string inlineContents;            // if non-empty, use these bytes verbatim
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
	 * The current sequence number of the transaction log file. Atomic because it
	 * is written on the write path (under writeMutex) but read on the read path
	 * (getMemoryMap/findPositionByTimestamp under dataSetsMutex) — different
	 * locks, so the accesses would otherwise be a data race. Relaxed ordering is
	 * sufficient: readers only need a coherent value, not ordering against other
	 * state (the mutexes already provide that).
	 */
	std::atomic<uint32_t> currentSequenceNumber = 1;

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
	 * The number of transactions that have been bound to this store (via UseLog
	 * or addLogEntry) but have not yet called writeBatch(). Once writeBatch()
	 * runs the transaction's position is tracked in uncommittedTransactionPositions,
	 * so the counter is decremented there. If the transaction is closed without
	 * ever writing, the counter is decremented in TransactionHandle::close().
	 *
	 * This is used by purgeTransactionLogs() to avoid destroying a store that
	 * still has in-flight transactions that haven't written yet, which would
	 * cause writeBatch() to write to a closed store and create orphaned log files.
	 */
	std::atomic<int> pendingTransactionCount = 0;

	/**
	 * Positions written to log files but not yet committed to RocksDB. Kept as a
	 * sorted vector (ascending) so the minimum is always front(). A sorted vector
	 * is faster than std::set for the small sizes seen here (typically 1–16 entries)
	 * because it avoids per-node heap allocation and is cache-friendly.
	 */
	std::vector<LogPosition> uncommittedTransactionPositions;

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
	 * A fast mutex that guards only pendingTransactionCount increments/decrements
	 * and the isClosing flag assignment in tryClose(). It is never held during I/O
	 * or any long operation, so it cannot cause the main-thread stalls that
	 * holding writeMutex from UseLog/addLogEntry would cause.
	 *
	 * Lock ordering: transactionBindMutex → writeMutex → dataSetsMutex.
	 */
	std::mutex transactionBindMutex;

	/**
	 * A counter for the number of recentlyCommittedSequencePositions updates we have made so that we can use 2^n modulus
	 * frequencies to assign to recentlyCommittedSequencePositions
	 */
	unsigned int nextSequencePositionsCount = 0;

	/**
	 * Protects flushedStateFile, lastWrittenFlushedPosition, and all I/O on
	 * "txn.state". This is a separate, lightweight lock so that
	 * getLastFlushedPosition() — which is called from doPurge() while
	 * dataSetsMutex is already held — never needs to acquire dataSetsMutex,
	 * eliminating that deadlock path.
	 *
	 * Lock ordering: dataSetsMutex → flushedStateMutex.
	 * Never acquire dataSetsMutex while already holding flushedStateMutex.
	 */
	std::mutex flushedStateMutex;

	/**
	 * This file stream is used to track how much of the transaction log has been flushed to the database.
	 */
	std::ofstream flushedStateFile;

	/**
	 * The last flushed position that was written to the state file.
	 */
	LogPosition lastWrittenFlushedPosition = { 0, 0 };

	/**
	 * The next sequence position to use for a new transaction log entry.
	 */
	LogPosition nextLogPosition = { 0, 0 };

	/**
	 * Data structure to hold the last committed position of a transaction log file, that is exposed
	 * to JS through an external buffer with reference counting.
	 */
	std::shared_ptr<LogPosition> lastCommittedPosition;

	/**
	 * Lifetime counters exposed via `collectStats()`. These are observability-only
	 * and never gate behavior, so they are plain relaxed atomics: writers (which
	 * already hold writeMutex/dataSetsMutex) bump them, and `collectStats()` reads
	 * them without taking a lock. `lastPurgeMs` is milliseconds since the Unix
	 * epoch of the most recent purge (0 if never purged).
	 */
	std::atomic<uint64_t> transactionsWritten = 0;
	std::atomic<uint64_t> entriesWritten = 0;
	std::atomic<uint64_t> bytesWritten = 0;
	std::atomic<uint64_t> rotations = 0;
	std::atomic<uint64_t> filesPurged = 0;
	std::atomic<uint64_t> bytesPurged = 0;
	std::atomic<uint64_t> purgeRuns = 0;
	std::atomic<uint64_t> databaseFlushes = 0;
	std::atomic<uint64_t> writeFailures = 0;
	std::atomic<double> lastPurgeMs = 0;

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
	 * Used when the parent database is being closed — always closes regardless
	 * of whether there are active transactions.
	 */
	void close();

	/**
	 * Attempts to close the transaction log store atomically. Returns false
	 * (and does not close) if there are any active transactions — either
	 * bound-but-not-yet-written (pendingTransactionCount > 0) or
	 * written-but-not-yet-committed (non-sentinel uncommitted positions).
	 *
	 * The check and the "mark as closing" transition are performed together
	 * under writeMutex, the same lock held by writeBatch() for its entire
	 * duration. This prevents the check from racing with a concurrent writeBatch
	 * that is in the process of decrementing pendingTransactionCount.
	 *
	 * Used by purgeTransactionLogs() to safely destroy a store.
	 */
	bool tryClose();

	/**
	 * Notifies the transaction log store that a RocksDB commit operation has finished, and the transactions sequence number.
	 */
	void commitFinished(LogPosition position, rocksdb::SequenceNumber rocksSequenceNumber);

	/**
	 * Cleans up when a transaction handle is closed or garbage collected without
	 * having successfully committed (e.g. commit failed with IsBusy and was
	 * abandoned without retrying), removing its position from the uncommitted set.
	 */
	void commitAborted(LogPosition position);

	/**
	 * Called when a database flush job is finished, so that we can record how much of the transaction log has been flushed to db.
	 */
	void databaseFlushBegin(rocksdb::SequenceNumber rocksSequenceNumber);

	/**
	 * Called when a database flush job is finished, so that we can record how much of the transaction log has been flushed to db.
	 */
	void databaseFlushed(rocksdb::SequenceNumber rocksSequenceNumber);

	/**
	 * Memory maps the transaction log file for the given sequence number and
	 * returns a strong reference. For a frozen file the log file keeps only a
	 * weak handle, so this reference (handed to the JS external buffer) owns the
	 * mapping; releasing it unmaps the file.
	 **/
	std::shared_ptr<MemoryMap> getMemoryMap(uint32_t logSequenceNumber);

	/**
	 * Advances currentSequenceNumber to the next sequence, first downgrading the
	 * memory map of the file being rotated away from to a weak reference (so a
	 * reader that mapped it while current no longer pins it). Must be called on
	 * the write path (under writeMutex).
	 */
	void rotateToNextSequence(const std::shared_ptr<TransactionLogFile>& oldFile);

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
	 * Reads and returns the last flushed position from the txn.state file.
	 */
	LogPosition getLastFlushedPosition();

	/**
	 * Returns a point-in-time snapshot of the files that make up this store, for
	 * backup. Enumerates the sequence files under `dataSetsMutex` (holding
	 * shared_ptrs so they outlive the lock), capturing each file's current `size`
	 * as the byte limit and its on-disk mtime, plus the `txn.state` side file if
	 * present. Rotated files (sequence < current) are marked `immutable`.
	 */
	std::vector<TransactionLogBackupEntry> snapshotForBackup();

	/**
	 * Fills `out` with a point-in-time snapshot of this store's statistics
	 * (file/memory/transaction gauges, purge gauges, and lifetime counters).
	 *
	 * Reads the lifetime counters lock-free, then takes dataSetsMutex to walk
	 * the sequence files. The last-flushed position is read up front (before
	 * dataSetsMutex) to keep the dataSetsMutex → flushedStateMutex lock ordering.
	 */
	void collectStats(TransactionLogStoreStats& out);

	/**
	 * Purges transaction logs. By default, it deletes transaction log files older than the
	 * retention period (3 days). If `before` is provided, it deletes transaction log files older
	 * than the specified timestamp. If `all` is true, it deletes all transaction log files.
	 */
	void purge(
		std::function<void(const std::filesystem::path&, uint32_t entryCount)> visitor,
		const bool all,
		const uint64_t before,
		const bool countEntries = false
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
	 * Important! This method must be called with `writeMutex` already locked.
	 *
	 * @param sequenceNumber The sequence number of the log file to open.
	 * @returns The log file.
	 */
	std::shared_ptr<TransactionLogFile> getLogFile(const uint32_t sequenceNumber);

	void doPurge(
		std::function<void(const std::filesystem::path&, uint32_t entryCount)> visitor = nullptr,
		const bool all = false,
		const uint64_t before = 0,
		const bool countEntries = false
	);

	/**
	 * Advances the active log file to the next sequence number and records a
	 * rotation. Must be called with writeMutex held (the only paths that rotate
	 * — writeBatch() and doPurge() — both hold it).
	 */
	inline void advanceSequence() {
		this->currentSequenceNumber.store(this->nextSequenceNumber++, std::memory_order_relaxed);
		this->rotations.fetch_add(1, std::memory_order_relaxed);
	}

	// Sorted-vector helpers for uncommittedTransactionPositions.
	// All callers must hold dataSetsMutex.
	void positionInsert(LogPosition pos) {
		auto it = std::lower_bound(uncommittedTransactionPositions.begin(),
		                           uncommittedTransactionPositions.end(), pos);
		// Mimic std::set::insert: skip if the element already exists.
		if (it == uncommittedTransactionPositions.end() || pos < *it) {
			uncommittedTransactionPositions.insert(it, pos);
		}
	}
	void positionErase(LogPosition pos) {
		auto it = std::lower_bound(uncommittedTransactionPositions.begin(),
		                           uncommittedTransactionPositions.end(), pos);
		if (it != uncommittedTransactionPositions.end() && !(*it < pos) && !(pos < *it)) {
			uncommittedTransactionPositions.erase(it);
		}
	}

	/**
	 * Performs the actual close work. Must be called with both writeMutex and
	 * dataSetsMutex already held, and isClosing already set to true.
	 */
	void doClose();
};

} // namespace rocksdb_js

#endif
