#include <chrono>
#include <exception>
#include <sstream>
#include <vector>
#include "transaction_log_store.h"
#include "core/debug.h"
#include "core/encoding.h"
#include "core/platform.h"
#include "napi/global_events.h"
#include "fstream"

namespace rocksdb_js {

// Helper function to extract exception message from exception_ptr
static std::string getExceptionMessage(std::exception_ptr eptr) {
	if (!eptr) {
		return "No exception information available";
	}
	try {
		std::rethrow_exception(eptr);
	} catch (const std::exception& e) {
		return e.what();
	} catch (const std::string& s) {
		return s;
	} catch (const char* s) {
		return s;
	} catch (...) {
		return "Unknown exception type";
	}
}

TransactionLogStore::TransactionLogStore(
	const std::string& name,
	const std::filesystem::path& path,
	const uint32_t maxFileSize,
	const std::chrono::milliseconds& retentionMs,
	const float maxAgeThreshold
) :
	name(name),
	path(path),
	maxFileSize(maxFileSize),
	retentionMs(retentionMs),
	maxAgeThreshold(maxAgeThreshold)
{
	DEBUG_LOG("%p TransactionLogStore::TransactionLogStore Opening transaction log store \"%s\"\n", this, this->name.c_str());
	lastCommittedPosition = std::make_shared<LogPosition>();
	uncommittedTransactionPositions.reserve(16);
	for (int i = 0; i < RECENTLY_COMMITTED_POSITIONS_SIZE; i++) { // initialize recent commits to not match until values are entered
		this->recentlyCommittedSequencePositions[i].position = { 0, 0 };
		this->recentlyCommittedSequencePositions[i].rocksSequenceNumber = 0x7FFFFFFFFFFFFFFF; // maximum int64, won't match any commit
	}
}

TransactionLogStore::~TransactionLogStore() {
	DEBUG_LOG("%p TransactionLogStore::~TransactionLogStore Closing transaction log store \"%s\"\n", this, this->name.c_str());
	this->close();
}

void TransactionLogStore::doClose() {
	// Assumes writeMutex and dataSetsMutex are already held, and isClosing == true.
	std::vector<std::shared_ptr<TransactionLogFile>> logFilesToClose;

	DEBUG_LOG("%p TransactionLogStore::close Closing transaction log store \"%s\"\n", this, this->name.c_str());

	for (auto it = this->sequenceFiles.begin(); it != this->sequenceFiles.end(); ) {
		auto logFile = it->second;
		logFilesToClose.push_back(logFile);
		it = this->sequenceFiles.erase(it);
	}

	// Close the state file if it's open. flushedStateMutex must be held for
	// all flushedStateFile access; release it before calling doPurge() since
	// doPurge() → getLastFlushedPosition() will acquire flushedStateMutex
	// itself (and we must not hold it when doPurge re-acquires it).
	{
		std::lock_guard<std::mutex> flushedLock(this->flushedStateMutex);
		if (this->flushedStateFile.is_open()) {
			this->flushedStateFile.close();
		}
	}

	for (auto& logFile : logFilesToClose) {
		logFile->close();
	}

	this->doPurge();
}

void TransactionLogStore::close() {
	// set the closing flag to prevent concurrent closes
	bool expected = false;
	if (!this->isClosing.compare_exchange_strong(expected, true)) {
		// already closing, return early
		DEBUG_LOG("%p TransactionLogStore::close Already closing, skipping \"%s\"\n", this, this->name.c_str());
		return;
	}

	std::lock_guard<std::mutex> lock(this->writeMutex);
	std::lock_guard<std::mutex> dataSetsLock(this->dataSetsMutex);
	this->doClose();
}

bool TransactionLogStore::tryClose() {
	// Fast path: already closing.
	if (this->isClosing.load(std::memory_order_relaxed)) {
		return true;
	}

	// Phase 1 — quick count check under transactionBindMutex.
	// transactionBindMutex is a lightweight lock used only for pendingTransactionCount
	// increments and the isClosing assignment; it is never held during I/O.
	{
		std::lock_guard<std::mutex> bindLock(this->transactionBindMutex);
		if (this->isClosing.load(std::memory_order_relaxed)) return true;
		if (this->pendingTransactionCount.load(std::memory_order_relaxed) > 0) {
			DEBUG_LOG("%p TransactionLogStore::tryClose Skipping (phase 1): pendingTransactionCount=%d\n",
				this, this->pendingTransactionCount.load(std::memory_order_relaxed));
			return false;
		}
	}

	// Phase 2 — drain any in-progress writeBatch and check uncommitted positions.
	// Acquiring writeMutex here blocks until any concurrent writeBatch() finishes
	// (writeBatch holds writeMutex for its full duration, including the
	// pendingTransactionCount decrement and position insertion at the end).
	{
		std::lock_guard<std::mutex> writeLock(this->writeMutex);
		std::lock_guard<std::mutex> dataLock(this->dataSetsMutex);
		if (this->isClosing.load(std::memory_order_relaxed)) return true;
		for (const auto& pos : this->uncommittedTransactionPositions) {
			if (pos.positionInLogFile == this->nextLogPosition.positionInLogFile &&
				pos.logSequenceNumber == this->nextLogPosition.logSequenceNumber) {
				continue; // sentinel entry, not a real transaction
			}
			DEBUG_LOG("%p TransactionLogStore::tryClose Skipping (phase 2): uncommitted position (%u, %u)\n",
				this, pos.logSequenceNumber, pos.positionInLogFile);
			return false;
		}
	}

	// Phase 3 — re-check count and atomically set isClosing under transactionBindMutex.
	// A new UseLog/addLogEntry may have bound a transaction between phases 1 and 3,
	// so we must re-verify. Once isClosing is set here, all future bind attempts
	// will fail (they also check isClosing under transactionBindMutex).
	{
		std::lock_guard<std::mutex> bindLock(this->transactionBindMutex);
		if (this->isClosing.load(std::memory_order_relaxed)) return true;
		if (this->pendingTransactionCount.load(std::memory_order_relaxed) > 0) {
			DEBUG_LOG("%p TransactionLogStore::tryClose Skipping (phase 3): pendingTransactionCount=%d\n",
				this, this->pendingTransactionCount.load(std::memory_order_relaxed));
			return false;
		}
		bool expected = false;
		this->isClosing.compare_exchange_strong(expected, true);
	}

	// Phase 4 — perform the actual close.
	// Any writeBatch() that starts after phase 3 will see isClosing=true and
	// throw immediately. Phase 4 waits for any writeBatch() that already holds
	// writeMutex to finish before we tear down the store.
	{
		std::lock_guard<std::mutex> writeLock(this->writeMutex);
		std::lock_guard<std::mutex> dataLock(this->dataSetsMutex);
		this->doClose();
	}
	return true;
}

std::shared_ptr<TransactionLogFile> TransactionLogStore::getLogFile(const uint32_t sequenceNumber) {
	std::lock_guard<std::mutex> lock(this->dataSetsMutex);
	auto it = this->sequenceFiles.find(sequenceNumber);
	std::shared_ptr<TransactionLogFile> logFile = it != this->sequenceFiles.end() ? it->second : nullptr;

	if (!logFile) {
		DEBUG_LOG("%p TransactionLogStore::getLogFile No log file found, creating store \"%s\" (seq=%u)\n",
			this, this->path.string().c_str(), sequenceNumber);

		// ensure the directory exists before creating the file (should already exist)
		DEBUG_LOG("%p TransactionLogStore::getLogFile Creating directory: %s\n", this, this->path.string().c_str());
		rocksdb_js::tryCreateDirectory(this->path);

		std::string filename = std::to_string(sequenceNumber) + ".txnlog";
		auto logFilePath = this->path / filename;
		logFile = std::make_shared<TransactionLogFile>(logFilePath, sequenceNumber, true);
		this->sequenceFiles[sequenceNumber] = logFile;
		this->nextLogPosition = { 0, sequenceNumber };
	}

	return logFile;
}

void TransactionLogStore::rotateToNextSequence(const std::shared_ptr<TransactionLogFile>& oldFile) {
	// The file we are rotating away from is now frozen: drop its strong memory-map
	// reference so the mapping is released once any JS buffers a reader mapped from
	// it while it was current are GC'd, rather than staying pinned until the file
	// is re-read as frozen or purged.
	//
	// Called under writeMutex (the write path). Take dataSetsMutex too so the
	// downgrade + currentSequenceNumber bump are atomic with respect to readers
	// that derive a file's strong-vs-weak map ownership from currentSequenceNumber
	// — getMemoryMap() and findPositionByTimestamp() both compute `isCurrent` under
	// dataSetsMutex. Without this, a reader could read currentSequenceNumber == N
	// (isCurrent=true) and re-pin file N's map strongly in getMemoryMapLocked()
	// after this method has already downgraded it, permanently retaining the
	// mapping and defeating the release. Serializing here forces the reader to be
	// wholly before (sees N, then this downgrade undoes its strong ref) or wholly
	// after (sees N+1, maps frozen/weak). Lock order is writeMutex -> dataSetsMutex,
	// consistent with the rest of the store; no caller holds dataSetsMutex already.
	std::lock_guard<std::mutex> lock(this->dataSetsMutex);
	if (oldFile) {
		oldFile->downgradeMapToFrozen();
	}
	this->advanceSequence();
	this->nextLogPosition = { 0, this->currentSequenceNumber.load(std::memory_order_relaxed) };
}

std::shared_ptr<MemoryMap> TransactionLogStore::getMemoryMap(uint32_t logSequenceNumber) {
	std::lock_guard<std::mutex> lock(this->dataSetsMutex);
	auto it = this->sequenceFiles.find(logSequenceNumber);
	auto logFile = it != this->sequenceFiles.end() ? it->second.get() : nullptr;
	if (!logFile) {
		return nullptr;
	}
	if (!logFile->isOpen()) {
		logFile->open(this->latestTimestamp);
	}
	// Return a strong reference: for a frozen file the log file itself keeps only
	// a weak handle, so this strong ref (and the JS external buffer it is handed
	// to) is what keeps the mapping alive.
	//
	// `isCurrent` decides strong-vs-weak ownership down in getMemoryMapLocked().
	// We hold dataSetsMutex across the read and the getMemoryMap() call, and
	// rotateToNextSequence() takes the same lock around its downgrade + sequence
	// bump, so this isCurrent value cannot go stale mid-call (a frozen file can
	// never be re-pinned as current here).
	bool isCurrent = this->currentSequenceNumber.load(std::memory_order_relaxed) == logSequenceNumber;
	return logFile->getMemoryMap(isCurrent ?
		maxFileSize : // if it is the most current log, it will be growing so we need to allocate the max size
		logFile->size.load(std::memory_order_relaxed), // otherwise it is frozen, use the file size
		isCurrent);
}

uint64_t TransactionLogStore::getLogFileSize(uint32_t logSequenceNumber) {
	std::lock_guard<std::mutex> lock(this->dataSetsMutex);

	if (logSequenceNumber > 0) {
		auto it = this->sequenceFiles.find(logSequenceNumber);
		auto logFile = it != this->sequenceFiles.end() ? it->second.get() : nullptr;
		if (!logFile) {
			return 0;
		}
		if (!logFile->isOpen()) {
			logFile->open(this->latestTimestamp);
		}
		return logFile->size;
	}

	// get the total size of all log files
	uint64_t size = 0;
	for (auto& [key, logFile] : this->sequenceFiles) {
		if (!logFile->isOpen()) {
			logFile->open(this->latestTimestamp);
		}
		size += logFile->size;
	}
	return size;
}

std::weak_ptr<LogPosition> TransactionLogStore::getLastCommittedPosition() {
	// Initialize lastCommittedPosition if it's still at {0, 0} and invalid
	if (this->lastCommittedPosition->fullPosition == 0) {
		// Get flushed position before acquiring lock to avoid deadlock
		LogPosition flushedPosition = this->getLastFlushedPosition();

		std::lock_guard<std::mutex> lock(this->dataSetsMutex);

		// Double-check after acquiring lock
		if (this->lastCommittedPosition->fullPosition == 0) {
			LogPosition initialPosition = { 0, 0 };

			// First, try to use the last flushed position from disk
			if (flushedPosition.fullPosition > 0) {
				initialPosition = flushedPosition;
			}
			// If no flushed position exists, use the beginning of the first extant log file
			else if (!this->sequenceFiles.empty()) {
				auto firstLogFile = this->sequenceFiles.begin()->second;
				initialPosition = { TRANSACTION_LOG_FILE_HEADER_SIZE, firstLogFile->sequenceNumber };
			}
			// Otherwise, use current position
			else {
				uint32_t currentSeq = this->currentSequenceNumber.load(std::memory_order_relaxed);
				if (currentSeq > 0) {
					initialPosition = { TRANSACTION_LOG_FILE_HEADER_SIZE, currentSeq };
				}
			}

			*this->lastCommittedPosition = initialPosition;
		}
	}
	return this->lastCommittedPosition;
}

LogPosition TransactionLogStore::findPositionByTimestamp(double timestamp) {
	std::lock_guard<std::mutex> lock(this->dataSetsMutex);
	uint32_t sequenceNumber = this->currentSequenceNumber.load(std::memory_order_relaxed);
	bool isCurrent = true;
	uint32_t positionInLogFile = 0;
	auto it = this->sequenceFiles.find(sequenceNumber);
	if (it == this->sequenceFiles.end()) {
		// it is possible that the current log file doesn't exist yet, so we need to look at the previous one
		it = this->sequenceFiles.find(--sequenceNumber);
		isCurrent = false;
	}
	while (it != this->sequenceFiles.end()) {
		auto logFile = it->second.get();
		// Directory iteration order is unspecified, so registerLogFile() may not
		// have opened an older file before a higher sequence became current.
		if (!logFile->isOpen()) {
			logFile->open(this->latestTimestamp);
		}
		positionInLogFile = logFile->findPositionByTimestamp(
			timestamp,
			isCurrent ? this->maxFileSize : logFile->size.load(std::memory_order_relaxed),
			isCurrent
		);
		// a position of zero means that the timestamp is before the log file header's timestamp, greater than that,
		// we are in the correct log file to start searching
		if (positionInLogFile > 0) {
			if (positionInLogFile == 0xFFFFFFFF) {
				// beyond the end of this log file
				if (sequenceNumber < this->currentSequenceNumber.load(std::memory_order_relaxed)) {
					// revert to next one (because it exists)
					break;
				} else { // otherwise position at the end of the log file (JS code can filter from here)
					positionInLogFile = logFile->size;
				}
			}
			// found a valid position in the log file
			return { positionInLogFile, sequenceNumber };
		}
		isCurrent = false;
		it = this->sequenceFiles.find(--sequenceNumber);
	};
	// we iterated too far, return to the beginning position in the current log file
	return { TRANSACTION_LOG_FILE_HEADER_SIZE, sequenceNumber + 1 };
}

LogPosition TransactionLogStore::getLastFlushedPosition() {
	std::lock_guard<std::mutex> flushedLock(this->flushedStateMutex);
	auto stateFilePath = this->path / "txn.state";
	std::ifstream inputFile(stateFilePath, std::ios::binary | std::ios::in);
	LogPosition position = { 0, 0 };

	if (inputFile.is_open()) {
		inputFile.read(reinterpret_cast<char*>(&position), sizeof(position));
		inputFile.close();
	}

	return position;
}

void TransactionLogStore::ensureExtent(const std::shared_ptr<TransactionLogFile>& file) {
	if (file->size.load(std::memory_order_relaxed) != 0 || file->isOpen()) {
		return;
	}

	// Never borrow the active segment's handle. Its lifecycle belongs to the
	// write path — load()'s post-discovery activation and getLogFile() open it
	// before the first append — so the only way it reads size 0 and closed is the
	// window between getLogFile() creating it and the first append, where 0 is
	// the truth. A close() here would drop a handle (and, on Windows, the mapping)
	// the next append expects to still hold.
	if (file->sequenceNumber == this->currentSequenceNumber.load(std::memory_order_relaxed)) {
		return;
	}

	// Only a definite absence skips the open: a stat that *errors* leaves us
	// unable to tell, and both callers fail unsafely on an unresolved extent,
	// while the worst case of opening a since-deleted path is a 13-byte header
	// stub that the next startup rescan purges.
	std::error_code existsEc;
	if (!std::filesystem::exists(file->path, existsEc) && !existsEc) {
		return;
	}

	file->open(this->latestTimestamp);
	file->close();
}

std::vector<TransactionLogBackupEntry> TransactionLogStore::snapshotForBackup() {
	// Capture txn.state (the flushed-position side file) FIRST, and inline its
	// bytes. Reading it before the log-file sizes guarantees its recorded flushed
	// position is <= the sizes we capture next (sizes only grow), so a restored
	// store never sees a committed/flushed offset past the backed-up bytes. It is
	// captured inline (not re-read at copy time) because it is rewritten in place
	// — a later re-read could observe a newer position past the captured extents.
	TransactionLogBackupEntry stateEntry;
	bool hasStateEntry = false;
	{
		// databaseFlushed() (RocksDB's OnFlushComplete callback) rewrites txn.state
		// in place under flushedStateMutex, and a flush can fire mid-backup — an
		// unsynchronized read could tear, decoding a position that is neither the
		// old nor the new value and may point past the log extents captured below
		// (same discipline as getLastFlushedPosition()). The lock is scoped to this
		// block, which touches nothing but the 8-byte state file, and is released
		// before dataSetsMutex is taken below (ordering: dataSetsMutex →
		// flushedStateMutex, never the reverse).
		std::lock_guard<std::mutex> flushedLock(this->flushedStateMutex);

		std::filesystem::path statePath = this->path / "txn.state";
		std::error_code existsEc;
		if (std::filesystem::exists(statePath, existsEc) && !existsEc) {
			std::error_code timeEc;
			auto stateMtime = std::filesystem::last_write_time(statePath, timeEc);
			std::ifstream in(statePath, std::ios::binary | std::ios::ate);
			if (in && !timeEc) {
				std::streamoff len = in.tellg();
				if (len > 0) {
					std::string contents(static_cast<size_t>(len), '\0');
					in.seekg(0);
					in.read(contents.data(), len);
					if (in) {
						stateEntry.relativeName = "txn.state";
						stateEntry.sourcePath = statePath;
						stateEntry.byteLimit = static_cast<uint64_t>(len);
						stateEntry.mtime = stateMtime;
						stateEntry.immutable = false;
						stateEntry.inlineContents = std::move(contents);
						hasStateEntry = true;
					}
				}
			}
		}
	}

	// Snapshot the sequence files under dataSetsMutex, copying shared_ptrs so the
	// files outlive the lock (the CoolTransactionLogs pattern). Also capture the
	// current sequence so we can mark rotated (immutable) files.
	std::vector<std::pair<uint32_t, std::shared_ptr<TransactionLogFile>>> files;
	uint32_t current;
	{
		std::lock_guard<std::mutex> lock(this->dataSetsMutex);
		current = this->currentSequenceNumber.load(std::memory_order_relaxed);
		files.reserve(this->sequenceFiles.size());
		for (const auto& [seq, file] : this->sequenceFiles) {
			// Resolve the written extent of a lazily-registered segment, or the
			// `byteLimit == 0` skip below silently omits it from the backup
			// (directory iteration order decides which segments are lazy, so ext4
			// hits those orders and APFS hides them). close() leaves `size` set, so
			// successful opens cost their syscalls once per process. A malformed file
			// remains unresolved and is retried so a repaired segment can rejoin backups.
			try {
				this->ensureExtent(file);
			} catch (const TransactionLogFormatException& e) {
				// A malformed header has no readable entries, so there is nothing to
				// back up. Other open failures propagate: the segment may be healthy,
				// and reporting success would publish an incomplete backup.
				if (!file->malformedBackupWarningEmitted.exchange(true, std::memory_order_relaxed)) {
					std::ostringstream msg;
					msg << "Transaction log segment " << file->path.string()
						<< " could not be opened to measure its extent (" << e.what()
						<< "); it is excluded from this backup.";
					DEBUG_LOG("%p TransactionLogStore::snapshotForBackup WARNING: %s\n", this, msg.str().c_str());
					emitGlobalEvent("log.warn", ListenerData::fromStrings({ msg.str() }));
				}
				continue;
			}
			files.emplace_back(seq, file);
		}
	}

	std::vector<TransactionLogBackupEntry> entries;
	entries.reserve(files.size() + 1);
	for (const auto& [seq, file] : files) {
		// `size` is atomic and append-owned, so [0, byteLimit) is a stable,
		// complete prefix even if a concurrent append is in flight (we simply
		// capture the pre-append extent).
		uint64_t byteLimit = file->size.load(std::memory_order_relaxed);
		if (byteLimit == 0) {
			// A just-created current file whose header has not landed yet — nothing
			// to back up, and a header-less file would confuse recovery on restore.
			// The extent is seeded above, so this no longer also means "registered
			// but never opened".
			continue;
		}
		std::error_code ec;
		auto mtime = std::filesystem::last_write_time(file->path, ec);
		if (ec) {
			// The file was purged out from under us between the snapshot and now;
			// skip it (a rotated file that's gone contributes nothing to restore).
			continue;
		}
		entries.push_back({
			file->path.filename().string(),
			file->path,
			byteLimit,
			mtime,
			// A retired file is frozen but its physical inode includes an orphaned
			// tail beyond byteLimit, so it must take the prefix-copy path.
			seq != current &&
				file->retiredAppendBoundary.load(std::memory_order_relaxed) == 0,
			{}, // read from disk, not inline
		});
	}

	if (hasStateEntry) {
		entries.push_back(std::move(stateEntry));
	}

	return entries;
}

void TransactionLogStore::collectStats(TransactionLogStoreStats& out) {
	// identity
	out.name = this->name;
	out.path = this->path.string();

	// lifetime counters and config are lock-free / immutable
	out.transactionsWritten = this->transactionsWritten.load(std::memory_order_relaxed);
	out.entriesWritten = this->entriesWritten.load(std::memory_order_relaxed);
	out.bytesWritten = this->bytesWritten.load(std::memory_order_relaxed);
	out.rotations = this->rotations.load(std::memory_order_relaxed);
	out.filesPurged = this->filesPurged.load(std::memory_order_relaxed);
	out.bytesPurged = this->bytesPurged.load(std::memory_order_relaxed);
	out.purgeRuns = this->purgeRuns.load(std::memory_order_relaxed);
	out.databaseFlushes = this->databaseFlushes.load(std::memory_order_relaxed);
	out.writeFailures = this->writeFailures.load(std::memory_order_relaxed);
	out.lastPurgeMs = this->lastPurgeMs.load(std::memory_order_relaxed);
	out.maxFileSize = this->maxFileSize;
	out.retentionMs = static_cast<uint64_t>(this->retentionMs.count());
	out.maxAgeThreshold = this->maxAgeThreshold;
	out.pendingTransactions = this->pendingTransactionCount.load(std::memory_order_relaxed);

	// Read the flushed position before taking dataSetsMutex: getLastFlushedPosition()
	// acquires flushedStateMutex, and the required lock ordering is
	// dataSetsMutex → flushedStateMutex, so we must not already hold dataSetsMutex.
	LogPosition flushedPosition = this->getLastFlushedPosition();
	out.lastFlushedPosition = flushedPosition;

	auto now = std::chrono::system_clock::now();
	bool hasOldest = false;
	std::chrono::system_clock::time_point oldestWriteTime;

	std::lock_guard<std::mutex> lock(this->dataSetsMutex);

	out.currentSequenceNumber = this->currentSequenceNumber;
	out.nextLogPosition = this->nextLogPosition;
	out.fileCount = static_cast<uint32_t>(this->sequenceFiles.size());
	out.uncommittedTransactions = static_cast<uint32_t>(this->uncommittedTransactionPositions.size());
	out.oldestSequenceNumber = this->sequenceFiles.empty() ? 0 : this->sequenceFiles.begin()->first;

	if (this->lastCommittedPosition && this->lastCommittedPosition->fullPosition > 0) {
		out.lastCommittedPosition = *this->lastCommittedPosition;
		out.hasLastCommittedPosition = true;
	}

	const bool retentionEnabled = this->retentionMs.count() > 0;
	uint32_t retentionFloorSequence = this->sequenceFiles.empty()
		? 0
		: this->sequenceFiles.rbegin()->first;
	if (this->sequenceFiles.find(flushedPosition.logSequenceNumber) != this->sequenceFiles.end()) {
		retentionFloorSequence = flushedPosition.logSequenceNumber;
	}

	for (const auto& [seq, logFile] : this->sequenceFiles) {
		// A registered-but-never-opened older file still reads 0 here (see
		// snapshotForBackup); collectStats is deliberately syscall-free, so it
		// under-reports totalSizeBytes for those until something opens them.
		uint32_t fileSize = logFile->size.load(std::memory_order_relaxed);
		out.totalSizeBytes += fileSize;
		if (seq == this->currentSequenceNumber) {
			out.currentFileSize = fileSize;
		}

		// memory: the memoryMap shared_ptr is guarded by the per-file fileMutex
		// (not dataSetsMutex), so copy it under that lock. mapSize is immutable
		// after construction, so it is safe to read once the pointer is copied.
		std::shared_ptr<MemoryMap> memoryMap;
		{
			std::lock_guard<std::mutex> fileLock(logFile->fileMutex);
			memoryMap = logFile->memoryMap;
		}
		if (memoryMap) {
			out.mappedBytes += memoryMap->mapSize;
			out.activeMaps++;
		}
#if TRANSACTION_LOG_ENABLE_ANONYMOUS_OVERLAY && defined(PLATFORM_POSIX)
		out.overlayBytes += logFile->lastOverlaySize.load(std::memory_order_relaxed);
#endif

		// replay gap: bytes from the flushed position up to the write head, summed
		// across files. A file entirely before the flushed position contributes 0.
		if (seq >= flushedPosition.logSequenceNumber) {
			uint32_t from = (seq == flushedPosition.logSequenceNumber) ? flushedPosition.positionInLogFile : 0;
			uint32_t to = (seq == this->nextLogPosition.logSequenceNumber) ? this->nextLogPosition.positionInLogFile : fileSize;
			if (to > from) {
				out.replayGapBytes += (to - from);
			}
		}

		// purge / retention gauges — mirror the eligibility logic in doPurge().
		// Uses the in-memory fileLastWriteTime (seeded from the on-disk mtime at
		// registration/open and updated on every write) rather than stat()ing
		// each file, so collectStats stays syscall-free while holding
		// dataSetsMutex — important because stats may be polled frequently and
		// writeBatch contends on this mutex.
		auto mtime = logFile->fileLastWriteTime.load(std::memory_order_relaxed);
		if (!hasOldest || mtime < oldestWriteTime) {
			oldestWriteTime = mtime;
			hasOldest = true;
		}
		if (retentionEnabled) {
			auto fileAgeMs = std::chrono::duration_cast<std::chrono::milliseconds>(now - mtime);
			if (fileAgeMs > this->retentionMs) {
				// old enough to purge by retention; only actually purgeable if the
				// whole file lies before the flushed position (see doPurge()).
				// A lazily-registered file reads `size == 0` here (collectStats is
				// syscall-free, so it does not call ensureExtent()), which can
				// count it as purgeable when doPurge() — which does resolve the
				// extent — would retain it. Gauge-only skew; nothing acts on it.
				bool fullyFlushed = !(seq > flushedPosition.logSequenceNumber ||
					(seq == flushedPosition.logSequenceNumber && fileSize > flushedPosition.positionInLogFile));
				if (fullyFlushed && seq != retentionFloorSequence) {
					out.purgeableFiles++;
				} else if (!fullyFlushed) {
					out.retainedUnflushedFiles++;
				}
			}
		}
	}

	if (hasOldest) {
		out.oldestFileAgeMs = static_cast<double>(
			std::chrono::duration_cast<std::chrono::milliseconds>(now - oldestWriteTime).count());
	}
}

void TransactionLogStore::purge(std::function<void(const std::filesystem::path&, uint32_t entryCount)> visitor, const bool all, const uint64_t before, const bool countEntries) {
	std::lock_guard<std::mutex> lock(this->writeMutex);
	std::lock_guard<std::mutex> dataSetsLock(this->dataSetsMutex);
	this->doPurge(visitor, all, before, countEntries);
}

void TransactionLogStore::doPurge(std::function<void(const std::filesystem::path&, uint32_t entryCount)> visitor, const bool all, const uint64_t before, const bool countEntries) {
	if (this->sequenceFiles.empty()) {
		return;
	}

	// record that a purge scan ran and when (observability only)
	this->purgeRuns.fetch_add(1, std::memory_order_relaxed);
	auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::system_clock::now().time_since_epoch()).count();
	this->lastPurgeMs.store(static_cast<double>(nowMs), std::memory_order_relaxed);

	// collect sequence numbers to remove to avoid modifying map during iteration
	std::vector<uint32_t> sequenceNumbersToRemove;
	auto lastFlushedPosition = this->getLastFlushedPosition();
	uint32_t retentionFloorSequence = this->sequenceFiles.rbegin()->first;
	if (this->sequenceFiles.find(lastFlushedPosition.logSequenceNumber) != this->sequenceFiles.end()) {
		retentionFloorSequence = lastFlushedPosition.logSequenceNumber;
	}

	for (const auto& entry : this->sequenceFiles) {
		auto& sequenceNumber = entry.first;
		auto& logFile = entry.second;
		if (!all && sequenceNumber == retentionFloorSequence) {
			break;
		}
		bool shouldPurge = all;

		if (!shouldPurge && (before > 0 || this->retentionMs.count() > 0)) {
			try {
				auto mtime = logFile->getLastWriteTime();

				if (before > 0) {
					auto mtimeMs = std::chrono::duration_cast<std::chrono::milliseconds>(mtime.time_since_epoch()).count();
					shouldPurge = static_cast<uint64_t>(mtimeMs) < before;
				}
				if (!shouldPurge && this->retentionMs.count() > 0) {
					auto now = std::chrono::system_clock::now();
					auto fileAgeMs = std::chrono::duration_cast<std::chrono::milliseconds>(now - mtime);
					shouldPurge = fileAgeMs > this->retentionMs;
				}
			} catch (const std::filesystem::filesystem_error& e) {
				if (e.code() == std::errc::no_such_file_or_directory) {
					DEBUG_LOG("%p TransactionLogStore::purge Forgetting missing file: %s\n", this, logFile->path.string().c_str());
					sequenceNumbersToRemove.push_back(sequenceNumber);
					continue;
				}
				DEBUG_LOG("%p TransactionLogStore::purge Failed to get last write time for file %s: %s\n", this, logFile->path.string().c_str(), e.what());
				break;
			} catch (const std::exception& e) {
				DEBUG_LOG("%p TransactionLogStore::purge Failed to get last write time for file %s: %s\n", this, logFile->path.string().c_str(), e.what());
				break;
			} catch (...) {
				auto eptr = std::current_exception();
				std::string errorMsg = getExceptionMessage(eptr);
				DEBUG_LOG("%p TransactionLogStore::purge Unknown error getting last write time for file %s: %s\n",
					this, logFile->path.string().c_str(), errorMsg.c_str());
				break;
			}
		}

		if (!shouldPurge) {
			break;
		}

		// only purge files that are entirely before the last flushed position,
		// guaranteeing all their transactions have been committed to RocksDB
		if (sequenceNumber > lastFlushedPosition.logSequenceNumber) {
			if (all) {
				continue;
			}
			break;
		}
		if (sequenceNumber == lastFlushedPosition.logSequenceNumber) {
			// The only comparison here that reads `size`, and the only file whose
			// extent therefore has to be resolved: a lazily-registered segment
			// reads 0, so `0 > positionInLogFile` is false and a file whose tail
			// never reached RocksDB would be deleted as if fully flushed.
			try {
				this->ensureExtent(logFile);
			} catch (const std::exception& e) {
				// Unresolvable extent — refuse to purge rather than delete a
				// segment we cannot prove is flushed.
				DEBUG_LOG("%p TransactionLogStore::purge Failed to resolve extent for file %s: %s\n", this, logFile->path.string().c_str(), e.what());
				if (all) {
					continue;
				}
				break;
			} catch (...) {
				DEBUG_LOG("%p TransactionLogStore::purge Failed to resolve extent for file %s\n", this, logFile->path.string().c_str());
				if (all) {
					continue;
				}
				break;
			}
			if (logFile->size > lastFlushedPosition.positionInLogFile) {
				if (all) {
					continue;
				}
				break;
			}
		}

		// count the entries before removing the file (counting is opt-in extra
		// work; the file is gone by the time the visitor runs)
		uint32_t entryCount = (visitor && countEntries) ? logFile->countEntries() : 0;

		// delete the log file
		uint32_t removedSize = logFile->size.load(std::memory_order_relaxed);
		auto removed = logFile->removeFile();
		if (!removed) {
			if (all) {
				continue;
			}
			break;
		}
		this->filesPurged.fetch_add(1, std::memory_order_relaxed);
		this->bytesPurged.fetch_add(removedSize, std::memory_order_relaxed);
		if (visitor) {
			visitor(logFile->path, entryCount);
		}

		// collect sequence number for removal
		sequenceNumbersToRemove.push_back(sequenceNumber);
	}

	// remove sequence files from the map
	for (uint32_t sequenceNumber : sequenceNumbersToRemove) {
		if (sequenceNumber == this->currentSequenceNumber.load(std::memory_order_relaxed)) {
			// erase only the stale sentinel for the current sequence - the guard
			// above already verified no real uncommitted positions exist
			this->positionErase(this->nextLogPosition);

			// Advance to maintain monotonicity of (sequenceNumber, position)
			// pairs. Existing shared_ptrs to the old memory map remain valid
			// until released.
			DEBUG_LOG("%p TransactionLogStore::purge Advancing sequence number from %u to %u\n", this, this->currentSequenceNumber.load(std::memory_order_relaxed), this->nextSequenceNumber);
			this->advanceSequence();
			this->nextLogPosition = { 0, this->currentSequenceNumber.load(std::memory_order_relaxed) };
			this->positionInsert(this->nextLogPosition);
			LogPosition fullyCommittedPosition = this->uncommittedTransactionPositions.empty()
				? this->nextLogPosition
				: this->uncommittedTransactionPositions.front();
			*this->lastCommittedPosition = fullyCommittedPosition;
		}
		this->sequenceFiles.erase(sequenceNumber);
	}

	// if all log files have been removed, clean up the empty directory
	// only try to remove if we actually removed at least one file from this store
	if (all && this->sequenceFiles.empty() && !sequenceNumbersToRemove.empty()) {
		try {
			if (std::filesystem::exists(this->path)) {
				DEBUG_LOG("%p TransactionLogStore::purge Removing log store directory: %s\n", this, this->path.string().c_str());
				std::filesystem::remove_all(this->path);
				DEBUG_LOG("%p TransactionLogStore::purge Removed log store directory: %s\n", this, this->path.string().c_str());
			}
		} catch (const std::filesystem::filesystem_error& e) {
			DEBUG_LOG("%p TransactionLogStore::purge Failed to remove log store directory %s: %s\n", this, this->path.string().c_str(), e.what());
		} catch (...) {
			auto eptr = std::current_exception();
			std::string errorMsg = getExceptionMessage(eptr);
			DEBUG_LOG("%p TransactionLogStore::purge Unknown error removing log store directory %s: %s\n",
				this, this->path.string().c_str(), errorMsg.c_str());
		}
	}
}

void TransactionLogStore::registerLogFile(const std::filesystem::path& path, const uint32_t sequenceNumber) {
	std::lock_guard<std::mutex> lock(this->dataSetsMutex);

	uint32_t retiredBoundary = readTransactionLogAppendBoundaryMarker(path);
	auto logFile = std::make_shared<TransactionLogFile>(
		path, sequenceNumber, retiredBoundary > 0);
	if (retiredBoundary > 0) {
		logFile->retiredAppendBoundary.store(retiredBoundary, std::memory_order_relaxed);
		logFile->appendBoundaryLost.store(true, std::memory_order_relaxed);
		// Validate the header and seed the authoritative logical extent now. This
		// file must remain frozen even when it is the highest discovered sequence.
		try {
			logFile->open(this->latestTimestamp);
		} catch (const std::exception& e) {
			throw TransactionLogAppendBoundaryException(
				"Failed to open retired transaction log segment " + path.string() + ": " + e.what());
		}
		logFile->close();
	}
	this->sequenceFiles[sequenceNumber] = logFile;

	// Seed the in-memory last-write time from the on-disk mtime so the
	// retention gauges in collectStats() are correct for files that are
	// registered at startup discovery but never opened this session. This is
	// a one-time stat() at registration; collectStats() itself is syscall-free.
	try {
		logFile->fileLastWriteTime.store(
			convertFileTimeToSystemTime(std::filesystem::last_write_time(path)),
			std::memory_order_relaxed);
	} catch (...) {
		// file missing or racing a concurrent remove; keep the constructor's
		// "now" seed
	}

	if (retiredBoundary == 0 &&
		sequenceNumber >= this->currentSequenceNumber.load(std::memory_order_relaxed)) {
		// Record the candidate only; load() opens and marker-enables whichever file
		// is still current once the whole directory has been scanned.
		this->currentSequenceNumber.store(sequenceNumber, std::memory_order_relaxed);
		this->nextLogPosition = { 0, sequenceNumber };
	} else if (retiredBoundary > 0 &&
		sequenceNumber >= this->currentSequenceNumber.load(std::memory_order_relaxed)) {
		uint32_t nextWritableSequence = sequenceNumber + 1;
		this->currentSequenceNumber.store(nextWritableSequence, std::memory_order_relaxed);
		this->nextLogPosition = { 0, nextWritableSequence };
	}

	// update next sequence number to be one higher than the highest existing
	uint32_t sequenceAfterFile = sequenceNumber + (retiredBoundary > 0 ? 2 : 1);
	if (sequenceAfterFile > this->nextSequenceNumber) {
		this->nextSequenceNumber = sequenceAfterFile;
	}

	DEBUG_LOG("%p TransactionLogStore::registerLogFile Added log file: %s (seq=%u)\n",
		this, path.string().c_str(), sequenceNumber);
}

void TransactionLogStore::writeBatch(TransactionLogEntryBatch& batch, LogPosition& logPosition) {
	std::lock_guard<std::mutex> lock(this->writeMutex);

	if (this->isClosing.load(std::memory_order_relaxed)) {
		throw rocksdb_js::DBException("Transaction log store is closed");
	}

	DEBUG_LOG("%p TransactionLogStore::writeBatch Adding batch with %zu entries to store \"%s\" (current=%u, next=%u, timestamp=%llu)\n",
		this, batch.entries.size(), this->name.c_str(), this->currentSequenceNumber.load(std::memory_order_relaxed), this->nextSequenceNumber, batch.timestamp);

	{
		std::lock_guard<std::mutex> logPositionLock(this->dataSetsMutex);
		logPosition = this->nextLogPosition;
		this->positionInsert(logPosition);
	}

	// Judged against the previous append's clock sample (or construction's):
	// at most one batch stale, which the ten-year skew makes irrelevant, and no
	// clock read on this path.
	if (this->raiseLatestTimestamp(batch.timestamp, this->lastAppendMs + MAX_CLOCK_FLOOR_SKEW_MS)) {
		DEBUG_LOG("%p TransactionLogStore::writeBatch Set latest timestamp to batch timestamp: %f\n", this, batch.timestamp);
	}

	// write entries across multiple log files until all are written
	while (!batch.isComplete()) {
		std::shared_ptr<TransactionLogFile> logFile = nullptr;
		bool rotatedAfterOpenFailure = false;

		// get the current log file and rotate if needed
		while (logFile == nullptr) {
			logFile = this->getLogFile(this->currentSequenceNumber.load(std::memory_order_relaxed));

			// we found a log file, check if it's already at max size
			if (this->maxFileSize == 0 || logFile->size < this->maxFileSize) {
				try {
					if (!logFile->isOpen()) {
						logFile->open(this->latestTimestamp);
					}
					break;
				} catch (const std::exception& e) {
					DEBUG_LOG("%p TransactionLogStore::writeBatch Failed to open transaction log file: %s\n", this, e.what());
					this->writeFailures.fetch_add(1, std::memory_order_relaxed);
					if (rotatedAfterOpenFailure) {
						// A corrupt/colliding existing segment may be bypassed once, but a
						// repeated environmental failure must become a bounded caller error.
						throw;
					}
					rotatedAfterOpenFailure = true;
					// move to next sequence number and try again
					logFile = nullptr;
				}
			}

			// rotate to next sequence if file open failed or file is at max size
			// one retry avoids an infinite sequence of files on an environmental failure
			if (logFile == nullptr || this->maxFileSize > 0) {
				DEBUG_LOG("%p TransactionLogStore::writeBatch Advancing sequence number from %u to %u for store \"%s\" (logFile=%p, maxIndexSize=%u)\n",
					this, this->currentSequenceNumber.load(std::memory_order_relaxed), this->nextSequenceNumber, this->name.c_str(), static_cast<void*>(logFile.get()), this->maxFileSize);
				this->rotateToNextSequence(logFile);
				logFile = nullptr;
			}
		}

		if (!logPosition.fullPosition) {
			std::lock_guard<std::mutex> lock(this->dataSetsMutex);
			this->positionErase(logPosition);
			logPosition = this->nextLogPosition;
			this->positionInsert(logPosition);
		}

		// ensure we have a valid log file before writing
		if (!logFile) {
			DEBUG_LOG("%p TransactionLogStore::writeBatch ERROR: Failed to open transaction log file for store \"%s\"\n", this, this->name.c_str());
			throw rocksdb_js::DBException("Failed to open transaction log file for store \"" + this->name + "\"");
		}

		// if the file is older than the retention threshold, rotate to the next file
		if (this->maxAgeThreshold > 0) {
			auto thresholdDuration = std::chrono::duration_cast<std::chrono::milliseconds>(
				this->retentionMs * (1 - this->maxAgeThreshold)
			);
			auto now = std::chrono::system_clock::now();
			auto fileAgeMs = std::chrono::duration_cast<std::chrono::milliseconds>(now - logFile->fileLastWriteTime.load(std::memory_order_relaxed));
			DEBUG_LOG("%p TransactionLogStore::writeBatch Max age threshold:  %f\n", this, this->maxAgeThreshold);
			DEBUG_LOG("%p TransactionLogStore::writeBatch File age:           %lld ms (threshold %lld ms)\n",
				this, fileAgeMs.count(), thresholdDuration.count());
			if (fileAgeMs >= thresholdDuration) {
				DEBUG_LOG("%p TransactionLogStore::writeBatch Log file is older than threshold (%lld ms >= %lld ms), advancing from %u to %u for store \"%s\"\n",
					this, fileAgeMs.count(), thresholdDuration.count(), this->currentSequenceNumber.load(std::memory_order_relaxed), this->nextSequenceNumber, this->name.c_str());
				this->rotateToNextSequence(logFile);
				continue;
			}
		}

		uint32_t sizeBefore = logFile->size;

		DEBUG_LOG("%p TransactionLogStore::writeBatch Writing to log file for store \"%s\" (seq=%u, size=%u, maxIndexSize=%u)\n",
			this, this->name.c_str(), logFile->sequenceNumber, logFile->size.load(std::memory_order_relaxed), this->maxFileSize);

		// write as much as possible to this file
		try {
			logFile->writeEntries(batch, this->maxFileSize);
		} catch (...) {
			this->writeFailures.fetch_add(1, std::memory_order_relaxed);
			if (logFile->appendBoundaryLost.load(std::memory_order_relaxed)) {
				// Persist the logical boundary before the sequence becomes visible as
				// rotated. A restart can then ignore every orphaned physical byte.
				logFile->persistAppendBoundaryRetirement();
				this->rotateToNextSequence(logFile);
			}
			throw;
		}
		const auto appendedAt = std::chrono::system_clock::now();
		logFile->fileLastWriteTime.store(appendedAt, std::memory_order_relaxed);
		this->lastAppendMs = std::chrono::duration<double, std::milli>(appendedAt.time_since_epoch()).count();

		DEBUG_LOG("%p TransactionLogStore::writeBatch Wrote to log file for store \"%s\" (seq=%u, new size=%u)\n",
			this, this->name.c_str(), logFile->sequenceNumber, logFile->size.load(std::memory_order_relaxed));

		// if no progress was made, rotate to the next file to avoid infinite loop
		if (logFile->size == sizeBefore) {
			DEBUG_LOG("%p TransactionLogStore::writeBatch No progress made (size unchanged), advancing from %u to %u for store \"%s\"\n", this, this->currentSequenceNumber.load(std::memory_order_relaxed), this->nextSequenceNumber, this->name.c_str());
			this->rotateToNextSequence(logFile);
		} else if (this->maxFileSize > 0 && logFile->size >= this->maxFileSize) {
			// we've reached or exceeded the max size, rotate to the next file
			DEBUG_LOG("%p TransactionLogStore::writeBatch Log file reached max size, advancing from %u to %u for store \"%s\"\n", this, this->currentSequenceNumber.load(std::memory_order_relaxed), this->nextSequenceNumber, this->name.c_str());
			this->rotateToNextSequence(logFile);
		} else if (!batch.isComplete()) {
			// we've written some entries, but the batch is not complete, rotate to the next file
			DEBUG_LOG("%p TransactionLogStore::writeBatch Batch is not complete, advancing from %u to %u for store \"%s\"\n", this, this->currentSequenceNumber.load(std::memory_order_relaxed), this->nextSequenceNumber, this->name.c_str());
			this->rotateToNextSequence(logFile);
		}

		{
			std::lock_guard<std::mutex> lock(this->dataSetsMutex);
			// rotateToNextSequence() already reset the next position to the new
			// segment. Preserve that boundary; only advance within this file when
			// it is still current.
			if (this->currentSequenceNumber.load(std::memory_order_relaxed) == logFile->sequenceNumber) {
				this->nextLogPosition = { logFile->size, logFile->sequenceNumber };
			}
		}
	}

	{
		std::lock_guard<std::mutex> lock(this->dataSetsMutex);
		this->positionInsert(this->nextLogPosition);
	}

	// Now that nextLogPosition has been advanced past logPosition, it is safe to
	// drop the pending count so that purgeTransactionLogs() can see this
	// transaction as tracked by uncommittedTransactionPositions instead.
	this->pendingTransactionCount--;

	// record lifetime write counters (observability only)
	uint64_t batchBytes = 0;
	for (const auto& entry : batch.entries) {
		batchBytes += entry->size;
	}
	this->transactionsWritten.fetch_add(1, std::memory_order_relaxed);
	this->entriesWritten.fetch_add(batch.entries.size(), std::memory_order_relaxed);
	this->bytesWritten.fetch_add(batchBytes, std::memory_order_relaxed);

	DEBUG_LOG("%p TransactionLogStore::writeBatch Completed writing all entries\n", this);
}

void TransactionLogStore::commitFinished(const LogPosition position, rocksdb::SequenceNumber rocksSequenceNumber) {
	std::lock_guard<std::mutex> lock(this->dataSetsMutex);
	// This written transaction entry is no longer uncommitted, so we can remove it
	this->positionErase(position);
	// we now find the beginning of the earliest uncommitted transaction to mark the end of continuously fully committed transactions
	// If there are no uncommitted transactions, everything up to nextLogPosition is fully committed
	LogPosition fullyCommittedPosition = this->uncommittedTransactionPositions.empty()
		? this->nextLogPosition
		: this->uncommittedTransactionPositions.front();
	// update the current position handle with latest fully committed position
	*this->lastCommittedPosition = fullyCommittedPosition;
	// now setup a sequence position that matches a rocksdb sequence number to our log position
	SequencePosition sequencePosition = { rocksSequenceNumber, fullyCommittedPosition };
	// Now we record this in our array of sequence number + position combinations. However, we don't want to keep a huge
	// array so we keep an array where each n position represents an n^2 frequencies of correlations. We are not keeping
	// an exact map of every pairing, and we don't need to. We don't need to know the exact rocks sequence number, we just
	// need a sequence number that is not greater than the point of the flush. But we want to record enough that we
	// won't lose more than half of what has to be replayed since the last flush.
	unsigned int count = this->nextSequencePositionsCount++;
	int index = 0;
	// iterate through the array breaking once at the first set bit, but don't iterate past the end of the array (hence -1)
	for (; index < RECENTLY_COMMITTED_POSITIONS_SIZE - 1; index++) {
	    if ((count >> index) & 1) break; // will break 50% of the time at each iteration
	}
	// record in the array
	this->recentlyCommittedSequencePositions[index] = sequencePosition;
}

void TransactionLogStore::commitAborted(const LogPosition position) {
	std::lock_guard<std::mutex> lock(this->dataSetsMutex);
	this->positionErase(position);
	LogPosition fullyCommittedPosition = this->uncommittedTransactionPositions.empty()
		? this->nextLogPosition
		: this->uncommittedTransactionPositions.front();
	*this->lastCommittedPosition = fullyCommittedPosition;
}


bool operator>(const LogPosition a, const LogPosition b) {
	// as noted in the header, 64-bit comparison on little-endian machines seems like it would be an optimization
	return a.logSequenceNumber == b.logSequenceNumber ?
		a.positionInLogFile > b.positionInLogFile :
		a.logSequenceNumber > b.logSequenceNumber;
};

/**
 * This method is called when a database flush begins, and it prepares the transaction log store for the flush.
 * It ensures that all log files are flushed to disk before the database flush operation continues.
 * This maintains the consistency that all entries in the database are guaranteed to have a corresponding entry in the
 * log file (until it expires), even after crash.
 */
void TransactionLogStore::databaseFlushBegin(rocksdb::SequenceNumber rocksSequenceNumber) {
	if (this->isClosing.load(std::memory_order_relaxed)) {
		return;
	}

	std::vector<std::shared_ptr<TransactionLogFile>> logFilesToFlush;

	{
		std::lock_guard<std::mutex> lock(this->dataSetsMutex);
		// Copy the sequence files to a vector so we can release the lock
		logFilesToFlush.reserve(this->sequenceFiles.size());
		for (const auto& [sequenceNumber, logFile] : this->sequenceFiles) {
			logFilesToFlush.push_back(logFile);
		}
	}

	// Flush all log files to ensure data is synced to disk (without holding the lock)
	for (const auto& logFile : logFilesToFlush) {
		try {
			logFile->flush();
		} catch (const std::exception& e) {
			DEBUG_LOG("%p TransactionLogStore::databaseFlushBegin ERROR: Failed to flush log file %u: %s\n",
				this, logFile->sequenceNumber, e.what());
			// Continue flushing other files even if one fails
		}
	}
}

/**
 * Called when a database OnFlushComplete event takes place and this will record the last position in the log file that
 * has transactions that are fully flushed to disk in the RocksDB database and consequently do not need to be replayed
 * after restart or crash
 */
void TransactionLogStore::databaseFlushed(rocksdb::SequenceNumber rocksSequenceNumber) {
	if (this->isClosing.load(std::memory_order_relaxed)) {
		return;
	}

	LogPosition latestSequencePosition = { 0, 0 };
	{
		std::lock_guard<std::mutex> lock(this->dataSetsMutex);
		// the latest sequence number that has been flushed according to this flush update
		for (int i = 0; i < RECENTLY_COMMITTED_POSITIONS_SIZE; i++) {
			SequencePosition sequencePosition = this->recentlyCommittedSequencePositions[i];
			if (sequencePosition.rocksSequenceNumber <= rocksSequenceNumber && sequencePosition.position > latestSequencePosition) {
				latestSequencePosition = sequencePosition.position;
			}
		}
	}

	DEBUG_LOG("%p TransactionLogStore::databaseFlushed, flushed up to logId: %u position %u\n",
		this, latestSequencePosition.logSequenceNumber, latestSequencePosition.positionInLogFile);

	// All file I/O and lastWrittenFlushedPosition updates are protected by
	// flushedStateMutex (not dataSetsMutex) so that getLastFlushedPosition()
	// can safely read txn.state from doPurge() without risk of deadlock.
	std::lock_guard<std::mutex> flushedLock(this->flushedStateMutex);

	// Only write if the position has changed
	if (latestSequencePosition.fullPosition == lastWrittenFlushedPosition.fullPosition) {
		return;
	}

	// open the state file if it isn't open yet
	if (!this->flushedStateFile.is_open()) {
		auto flushedStateFilePath = this->path / "txn.state";
		this->flushedStateFile.open(flushedStateFilePath, std::ios::binary | std::ios::out);
	}

	// write the position to the file
	if (this->flushedStateFile.is_open()) {
		this->flushedStateFile.seekp(0);
		this->flushedStateFile.write(reinterpret_cast<const char*>(&latestSequencePosition), sizeof(latestSequencePosition));
		this->flushedStateFile.flush();
		lastWrittenFlushedPosition = latestSequencePosition;
		this->databaseFlushes.fetch_add(1, std::memory_order_relaxed);
	}
}

std::shared_ptr<TransactionLogStore> TransactionLogStore::load(
	const std::filesystem::path& path,
	const uint32_t maxFileSize,
	const std::chrono::milliseconds& retentionMs,
	const float maxAgeThreshold
) {
	auto dirName = path.filename().string();

	// skip directories that start with "."
	if (dirName.empty() || dirName[0] == '.') {
		return nullptr;
	}

	std::shared_ptr<TransactionLogStore> store = std::make_shared<TransactionLogStore>(dirName, path, maxFileSize, retentionMs, maxAgeThreshold);

	bool& clockFloorComplete = store->clockFloorComplete;

	// find `.txnlog` files in the directory
	try {
		for (const auto& fileEntry : std::filesystem::directory_iterator(path)) {
			try {
				if (fileEntry.is_regular_file() && fileEntry.path().extension() == ".txnlog") {
					auto filePath = fileEntry.path();
					auto filename = filePath.filename().string();

					std::string sequenceNumberStr = filename.substr(0, filename.size() - 7);
					uint32_t sequenceNumber = 0;

					sequenceNumber = std::stoul(sequenceNumberStr);

					store->registerLogFile(filePath, sequenceNumber);
				}
			} catch (const TransactionLogAppendBoundaryException&) {
				// The marker is the only authoritative record of a retired file's
				// logical end. Ignoring it could expose orphaned bytes after restart.
				throw;
			} catch (const std::filesystem::filesystem_error& e) {
				clockFloorComplete = false;
				DEBUG_LOG("%p TransactionLogStore::load Failed to process file (filesystem error): %s\n",
					store.get(), e.what());
			} catch (const std::exception& e) {
				clockFloorComplete = false;
				DEBUG_LOG("%p TransactionLogStore::load Failed to load file: %s\n",
					store.get(), e.what());
			} catch (...) {
				clockFloorComplete = false;
				auto eptr = std::current_exception();
				std::string errorMsg = getExceptionMessage(eptr);
				DEBUG_LOG("%p TransactionLogStore::load Unknown error processing file: %s\n",
					store.get(), errorMsg.c_str());
			}
		}
	} catch (const std::filesystem::filesystem_error& e) {
		clockFloorComplete = false;
		DEBUG_LOG("%p TransactionLogStore::load Failed to iterate directory: %s\n",
			store.get(), e.what());
	}

	LogPosition flushedPosition = store->getLastFlushedPosition();

	// Clock-floor seed, part 1: every segment's header word is the store's
	// latestTimestamp when that segment was created, so post-upgrade the newest
	// header bounds every key in the older segments. Part 2 (below) folds in the
	// entry keys of the segments the recovery scans walk anyway, which is where
	// keys above the newest header can still sit.
	// A header beyond the plausible bound is a far-future key stamped by an
	// older version: it says nothing about the segments it summarizes, and a
	// writer that stamped one cannot be trusted to have kept a running maximum
	// in any of its headers, so every segment older than the active file is
	// walked. The cost recurs at each open until retention purges the segment
	// carrying that header; the warning below names the key.
	// One clock sample classifies every candidate of this load, and the final
	// floor raise uses the same one, so none of them can disagree.
	const double plausibleBound = TransactionLogStore::plausibleBoundNow();
	double implausibleKey = 0;
	bool scanOlderSegments = false;
	{
		std::lock_guard<std::mutex> lock(store->dataSetsMutex);
		for (const auto& [sequence, logFile] : store->sequenceFiles) {
			try {
				double header = readTransactionLogFileHeaderTimestamp(logFile->path);
				store->raiseLatestTimestamp(header, plausibleBound);
				if (header > plausibleBound) {
					implausibleKey = std::max(implausibleKey, header);
					scanOlderSegments = true;
				}
			} catch (const std::exception& e) {
				clockFloorComplete = false;
				DEBUG_LOG("%p TransactionLogStore::load Failed to read header of %s: %s\n",
					store.get(), logFile->path.string().c_str(), e.what());
			}
		}
		if (scanOlderSegments) {
			for (const auto& [sequence, logFile] : store->sequenceFiles) {
				if (sequence >= store->currentSequenceNumber.load(std::memory_order_relaxed)) {
					continue;
				}
				const bool openedForScan = !logFile->isOpen();
				try {
					if (openedForScan) {
						logFile->open(store->latestTimestamp);
					}
					logFile->scanForLastCompleteTransactionEnd(plausibleBound);
					store->raiseLatestTimestamp(
						logFile->maxEntryTimestamp.load(std::memory_order_relaxed), plausibleBound);
					implausibleKey = std::max(implausibleKey,
						logFile->maxImplausibleEntryTimestamp.load(std::memory_order_relaxed));
					if (logFile->scanStoppedAtBreak.load(std::memory_order_relaxed)) {
						clockFloorComplete = false;
					}
				} catch (const std::exception& e) {
					clockFloorComplete = false;
					DEBUG_LOG("%p TransactionLogStore::load Failed to scan %s: %s\n",
						store.get(), logFile->path.string().c_str(), e.what());
				}
				if (openedForScan && logFile->isOpen()) {
					logFile->close();
				}
			}
		}
	}

	// Only the active file can carry a torn append; recover it after discovery and
	// refresh the write position if recovery shortened it.
	uint32_t storeCurrentSeq = store->currentSequenceNumber.load(std::memory_order_relaxed);
	if (storeCurrentSeq > 0) {
		auto currentIt = store->sequenceFiles.find(storeCurrentSeq);
		if (currentIt != store->sequenceFiles.end()) {
			auto& currentFile = currentIt->second;
			currentFile->appendBoundaryMarkerEnabled = true;
			bool activated = true;
			try {
				if (!currentFile->isOpen()) {
					currentFile->open(store->latestTimestamp);
				}
			} catch (const TransactionLogAppendBoundaryException&) {
				// The marker is authoritative; a segment we cannot reconcile with it
				// must fail the load rather than expose orphaned bytes.
				throw;
			} catch (const std::exception& e) {
				// A newest segment we cannot open is not fatal: leave it registered
				// (readers still see whatever a later ensureExtent() can resolve) and
				// start appending at a fresh sequence instead of failing the open.
				DEBUG_LOG("%p TransactionLogStore::load Failed to open current log file %s: %s\n",
					store.get(), currentFile->path.string().c_str(), e.what());
				currentFile->appendBoundaryMarkerEnabled = false;
				activated = false;
				clockFloorComplete = false;
			}
			if (activated) {
				uint32_t protectedPosition = flushedPosition.logSequenceNumber == storeCurrentSeq
					? flushedPosition.positionInLogFile
					: 0;
				currentFile->recoverTail(protectedPosition, plausibleBound);
				store->raiseLatestTimestamp(
					currentFile->maxEntryTimestamp.load(std::memory_order_relaxed), plausibleBound);
				implausibleKey = std::max(implausibleKey,
					currentFile->maxImplausibleEntryTimestamp.load(std::memory_order_relaxed));
				if (currentFile->scanStoppedAtBreak.load(std::memory_order_relaxed)) {
					clockFloorComplete = false;
				}
				store->nextLogPosition = { currentFile->size, storeCurrentSeq };
			} else {
				uint32_t nextWritableSequence = storeCurrentSeq + 1;
				store->currentSequenceNumber.store(nextWritableSequence, std::memory_order_relaxed);
				store->nextLogPosition = { 0, nextWritableSequence };
				if (store->nextSequenceNumber <= nextWritableSequence) {
					store->nextSequenceNumber = nextWritableSequence + 1;
				}
				storeCurrentSeq = nextWritableSequence;
			}
		}
	}

	store->positionInsert(store->nextLogPosition);

	// Seed from the last closed transaction, but never behind txn.state: flushed
	// log entries are already durable in RocksDB and are therefore a safe floor.
	// Legacy batches can span any number of rotations, so walk back through their
	// unflagged files until a boundary is found. Once the flushed file has been
	// scanned, older files cannot improve the floor and need not be read.
	LogPosition recoveredPosition = { 0, 0 };
	for (auto it = store->sequenceFiles.rbegin(); it != store->sequenceFiles.rend(); ++it) {
		if (it->first > storeCurrentSeq) {
			continue;
		}
		const bool isCurrent = it->first == storeCurrentSeq;
		if (flushedPosition.logSequenceNumber > 0 && it->first < flushedPosition.logSequenceNumber) {
			break;
		}

		auto& logFile = it->second;
		const bool openedForScan = !logFile->isOpen();
		try {
			if (openedForScan) {
				logFile->open(store->latestTimestamp);
			}
			uint32_t completeEnd = isCurrent
				? logFile->lastCompleteTransactionEnd.load(std::memory_order_relaxed)
				: logFile->scanForLastCompleteTransactionEnd(plausibleBound);
			store->raiseLatestTimestamp(
				logFile->maxEntryTimestamp.load(std::memory_order_relaxed), plausibleBound);
			implausibleKey = std::max(implausibleKey,
				logFile->maxImplausibleEntryTimestamp.load(std::memory_order_relaxed));
			if (logFile->scanStoppedAtBreak.load(std::memory_order_relaxed)) {
				clockFloorComplete = false;
			}
			if (openedForScan) {
				logFile->close();
			}
			if (completeEnd > 0) {
				recoveredPosition = { completeEnd, it->first };
				break;
			}
		} catch (const std::exception& e) {
			clockFloorComplete = false;
			if (openedForScan && logFile->isOpen()) {
				logFile->close();
			}
			DEBUG_LOG("%p TransactionLogStore::load Failed to scan transaction boundary in %s: %s\n",
				store.get(), logFile->path.string().c_str(), e.what());
		} catch (...) {
			clockFloorComplete = false;
			if (openedForScan && logFile->isOpen()) {
				logFile->close();
			}
			DEBUG_LOG("%p TransactionLogStore::load Failed to scan transaction boundary in %s\n",
				store.get(), logFile->path.string().c_str());
		}
		if (flushedPosition.logSequenceNumber > 0 && it->first == flushedPosition.logSequenceNumber) {
			break;
		}
	}
	*store->lastCommittedPosition = recoveredPosition < flushedPosition ? flushedPosition : recoveredPosition;

	// The floor must be in place before any transaction on this database can
	// claim a timestamp; DBDescriptor::open discovers stores before the
	// descriptor reaches a handle, so this runs early enough for every opener.
	if (raiseMonotonicTimestampFloor(store->latestTimestamp, plausibleBound)) {
		DEBUG_LOG("%p TransactionLogStore::load Raised monotonic clock floor to %f from store \"%s\"\n",
			store.get(), store->latestTimestamp, store->name.c_str());
	}
	if (implausibleKey > 0) {
		std::ostringstream msg;
		msg << "Transaction log store " << store->path.string()
			<< " holds a batch key more than " << static_cast<long long>(MAX_CLOCK_FLOOR_SKEW_MS / 86400000.0)
			<< " days ahead of the wall clock (" << std::fixed << implausibleKey
			<< "); not seeding the monotonic clock floor from it.";
		DEBUG_LOG("%p TransactionLogStore::load WARNING: %s\n", store.get(), msg.str().c_str());
		emitGlobalEvent("log.warn", ListenerData::fromStrings({ msg.str() }));
	}
	if (!clockFloorComplete) {
		std::ostringstream msg;
		msg << "Transaction log store " << store->path.string()
			<< " has a segment, or entries past a framing break, that could not be inspected at"
			   " open; the monotonic clock floor may be below a batch key already in this log.";
		DEBUG_LOG("%p TransactionLogStore::load WARNING: %s\n", store.get(), msg.str().c_str());
		emitGlobalEvent("log.warn", ListenerData::fromStrings({ msg.str() }));
	}
	if (retentionMs.count() > 0) {
		try {
			store->purge(nullptr, false, 0, false);
		} catch (const std::exception& e) {
			DEBUG_LOG("%p TransactionLogStore::load Failed to purge expired files: %s\n", store.get(), e.what());
		} catch (...) {
			DEBUG_LOG("%p TransactionLogStore::load Failed to purge expired files\n", store.get());
		}
	}

	return store;
}

} // namespace rocksdb_js
