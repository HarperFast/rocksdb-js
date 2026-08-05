#include <cmath>
#include <fstream>
#include <limits>
#include <sstream>
#include <system_error>
#include <vector>
#include "napi/global_events.h"
#include "transaction_log_entry.h"
#include "transaction_log_file.h"
#include "transaction_log_recovery.h"

// include platform-specific implementation
#ifdef PLATFORM_WINDOWS
	#include "transaction_log_file_windows.cpp"
#else
	#include "transaction_log_file_posix.cpp"
#endif

namespace rocksdb_js {

std::atomic<bool> TransactionLogFile::madvColdUnsupported{false};

std::atomic<int64_t> MemoryMap::liveCount{0};

#ifdef ROCKSDB_JS_NATIVE_TESTS
void TransactionLogFile::resetAdviseColdSupportForTests() {
	madvColdUnsupported.store(false, std::memory_order_relaxed);
}
#endif

TransactionLogFile::~TransactionLogFile() {
	this->close();
}

bool TransactionLogFile::removeFile() {
	std::lock_guard<std::mutex> lock(this->fileMutex);
	return this->removeFileLocked();
}

void TransactionLogFile::downgradeMapToFrozen() {
	std::lock_guard<std::mutex> lock(this->fileMutex);
	if (this->memoryMap) {
		// The file is no longer the current (actively-written) log, so drop the
		// strong reference. Keep a weak handle for handout dedup; the mapping now
		// lives exactly as long as the JS external buffer (if any reader mapped it
		// while it was current) — once that is released, it is unmapped instead of
		// staying pinned for the life of this TransactionLogFile.
		this->frozenMapCache = this->memoryMap;
		this->memoryMap.reset();
	}
}

std::chrono::system_clock::time_point TransactionLogFile::getLastWriteTime() {
	std::lock_guard<std::mutex> fileLock(this->fileMutex);
	// Check if file exists first to avoid exceptions from deleted files
	if (!std::filesystem::exists(this->path)) {
		throw std::filesystem::filesystem_error(
			"File does not exist",
			this->path,
			std::make_error_code(std::errc::no_such_file_or_directory)
		);
	}
	try {
		auto mtime = std::filesystem::last_write_time(this->path);
		return convertFileTimeToSystemTime(mtime);
	} catch (const std::filesystem::filesystem_error&) {
		// Re-throw filesystem errors as-is
		throw;
	} catch (const std::exception& e) {
		// Convert other standard exceptions to filesystem_error
		throw std::filesystem::filesystem_error(
			std::string("Failed to get last write time: ") + e.what(),
			this->path,
			std::make_error_code(std::errc::io_error)
		);
	} catch (...) {
		// Convert any other exception to filesystem_error
		throw std::filesystem::filesystem_error(
			"Unknown error getting last write time",
			this->path,
			std::make_error_code(std::errc::io_error)
		);
	}
}

void TransactionLogFile::open(const double latestTimestamp) {
	std::lock_guard<std::mutex> fileLock(this->fileMutex);
	this->openFile();

	// Cache the file's effective last-write time now, once, so writeBatch can
	// check the maxAgeThreshold without a stat() syscall on every commit.
	if (this->size == 0) {
		this->fileLastWriteTime.store(std::chrono::system_clock::now(), std::memory_order_relaxed);
	} else {
		try {
			this->fileLastWriteTime.store(convertFileTimeToSystemTime(std::filesystem::last_write_time(this->path)), std::memory_order_relaxed);
		} catch (...) {
			this->fileLastWriteTime.store(std::chrono::system_clock::now(), std::memory_order_relaxed);
		}
	}

	// read the file header
	char buffer[TRANSACTION_LOG_FILE_HEADER_SIZE];
	if (this->size == 0) {
		// file is empty, initialize it
		DEBUG_LOG("%p TransactionLogFile::open Initializing empty file: %s (timestamp=%f)\n", this, this->path.string().c_str(), latestTimestamp);
		writeUint32BE(buffer, TRANSACTION_LOG_TOKEN);
		writeUint8(buffer + 4, this->version);
		this->timestamp = latestTimestamp;
		writeDoubleBE(buffer + TRANSACTION_LOG_FILE_TIMESTAMP_POSITION, this->timestamp);

		// One write, checked: three unchecked writes could leave a header that only
		// partly landed while `size` still claimed a full one, framing every later
		// append from the wrong offset. Discard the file if it does land short —
		// a size in (0, HEADER_SIZE) fails the "too small" check below on every
		// future open, and freeing disk space would not heal it.
		int64_t headerBytes = this->writeToFile(buffer, TRANSACTION_LOG_FILE_HEADER_SIZE);
		if (headerBytes != static_cast<int64_t>(TRANSACTION_LOG_FILE_HEADER_SIZE)) {
			DEBUG_LOG("%p TransactionLogFile::open ERROR: Failed to write file header: %s (wrote=%lld)\n",
				this, this->path.string().c_str(), static_cast<long long>(headerBytes));
			this->removeFileLocked();
			throw rocksdb_js::DBException("Failed to write transaction log file header: " + this->path.string());
		}
		this->size = TRANSACTION_LOG_FILE_HEADER_SIZE;
	} else if (this->size < TRANSACTION_LOG_FILE_HEADER_SIZE) {
		DEBUG_LOG("%p TransactionLogFile::open ERROR: File is too small to be a valid transaction log file: %s\n", this, this->path.string().c_str());
		throw rocksdb_js::DBException("File is too small to be a valid transaction log file: " + this->path.string());
	} else {
		// try to read the token and version from the log file
		int64_t result = this->readFromFile(buffer, TRANSACTION_LOG_FILE_HEADER_SIZE, 0);
		if (result < 0) {
			DEBUG_LOG("%p TransactionLogFile::open ERROR: Failed to read version from file: %s\n", this, this->path.string().c_str());
			throw rocksdb_js::DBException("Failed to read version from file: " + this->path.string());
		}

		// token
		uint32_t token = readUint32BE(buffer);
		if (token != TRANSACTION_LOG_TOKEN) {
			DEBUG_LOG("%p TransactionLogFile::open ERROR: Invalid transaction log file: %s\n", this, this->path.string().c_str());
			throw rocksdb_js::DBException("Invalid transaction log file: " + this->path.string());
		}

		// version
		result = this->readFromFile(buffer, 1, 4);
		if (result < 0) {
			DEBUG_LOG("%p TransactionLogFile::open ERROR: Failed to read version from file: %s\n", this, this->path.string().c_str());
			throw rocksdb_js::DBException("Failed to read version from file: " + this->path.string());
		}
		this->version = readUint8(buffer);

		if (this->version != 1) {
			DEBUG_LOG("%p TransactionLogFile::open ERROR: Unsupported transaction log file version: %s\n", this, this->path.string().c_str());
			throw rocksdb_js::DBException("Unsupported transaction log file version: " + std::to_string(this->version));
		}

		// file timestamp
		result = this->readFromFile(buffer, 8, 5);
		if (result < 0) {
			DEBUG_LOG("%p TransactionLogFile::open ERROR: Failed to read file timestamp from file: %s\n", this, this->path.string().c_str());
			throw rocksdb_js::DBException("Failed to read file timestamp from file: " + this->path.string());
		}
		this->timestamp = readDoubleBE(buffer);

		DEBUG_LOG("%p TransactionLogFile::open Opened file %s (size=%zu, version=%u, timestamp=%f)\n",
			this, this->path.string().c_str(), this->size.load(std::memory_order_relaxed), this->version, this->timestamp);
	}
}

uint32_t TransactionLogFile::scanForLastCompleteTransactionEnd() {
	std::lock_guard<std::mutex> fileLock(this->fileMutex);

	if (this->version != 1) {
		return 0;
	}

	uint32_t fileSize = this->size.load(std::memory_order_relaxed);
	if (fileSize <= TRANSACTION_LOG_FILE_HEADER_SIZE) {
		return 0; // header-only or empty: no transactions at all
	}

	std::vector<char> buffer(fileSize);
	int64_t bytesRead = this->readFromFile(buffer.data(), fileSize, 0);
	if (bytesRead < 0 || static_cast<uint32_t>(bytesRead) != fileSize) {
		DEBUG_LOG("%p TransactionLogFile::scanForLastCompleteTransactionEnd Failed to read file: %s (read=%lld, size=%u)\n",
			this, this->path.string().c_str(), static_cast<long long>(bytesRead), fileSize);
		return 0;
	}

	uint32_t end = findLastCompleteTransactionEnd(buffer.data(), fileSize);
	this->lastCompleteTransactionEnd.store(end, std::memory_order_relaxed);
	return end;
}

void TransactionLogFile::recoverTail(uint32_t protectedPosition) {
	std::lock_guard<std::mutex> fileLock(this->fileMutex);

	if (this->version != 1) {
		return;
	}

	uint32_t fileSize = this->size.load(std::memory_order_relaxed);
	if (fileSize <= TRANSACTION_LOG_FILE_HEADER_SIZE) {
		return; // header-only or empty: nothing to recover
	}

	// Read the whole file image for the framing scan. This runs once, at open
	// time, on the active log file only — never on a hot path.
	std::vector<char> buffer(fileSize);
	int64_t bytesRead = this->readFromFile(buffer.data(), fileSize, 0);
	if (bytesRead < 0 || static_cast<uint32_t>(bytesRead) != fileSize) {
		DEBUG_LOG("%p TransactionLogFile::recoverTail Failed to read file for recovery scan: %s (read=%lld, size=%u)\n",
			this, this->path.string().c_str(), static_cast<long long>(bytesRead), fileSize);
		return;
	}

	RecoveryScan scan = scanTransactionLogForRecovery(buffer.data(), fileSize);
	// Publish the complete-transaction boundary from this same scan so the store can
	// seed its committed watermark without re-reading the file.
	this->lastCompleteTransactionEnd.store(scan.lastCompleteTransactionEnd, std::memory_order_relaxed);
	switch (scan.kind) {
		case RecoveryScan::Kind::Clean:
			this->discardUnclosedTransaction(scan, scan.validEnd, protectedPosition);
			return;

		case RecoveryScan::Kind::MidFileCorruption: {
			// Leave the file intact: entries are still framed after the break, so
			// truncating would discard committed/replicated transactions. Surface
			// it so an operator can repair the file; the reader's per-entry bounds
			// checks refuse to return the broken frame.
			DEBUG_LOG("%p TransactionLogFile::recoverTail Mid-file corruption at offset %u in %s (size=%u), leaving intact\n",
				this, scan.validEnd, this->path.string().c_str(), fileSize);

			std::ostringstream msg;
			msg << "Transaction log " << this->path.string()
				<< " has a framing break at offset " << std::hex << scan.validEnd << std::dec
				<< " with " << (fileSize - scan.validEnd)
				<< " byte(s) of further data; leaving it intact to avoid discarding committed entries. "
					"Reads past this point will fail until the file is repaired.";
			DEBUG_LOG("%p TransactionLogFile::recoverTail WARNING: %s\n", this, msg.str().c_str());
			emitGlobalEvent("log.warn", ListenerData::fromStrings({ msg.str() }));

			return;
		}

		case RecoveryScan::Kind::TruncateTail:
			if (scan.validEnd >= fileSize) {
				return;
			}
			if (scan.validEnd < protectedPosition) {
				std::ostringstream msg;
				msg << "Transaction log " << this->path.string()
					<< " has a torn tail before the flushed position " << protectedPosition
					<< "; leaving it intact to preserve durable log history.";
				DEBUG_LOG("%p TransactionLogFile::recoverTail WARNING: %s\n", this, msg.str().c_str());
				emitGlobalEvent("log.warn", ListenerData::fromStrings({ msg.str() }));
				return;
			}
			DEBUG_LOG("%p TransactionLogFile::recoverTail Torn tail in %s: truncating %u -> %u bytes\n",
				this, this->path.string().c_str(), fileSize, scan.validEnd);
			if (this->truncateFile(scan.validEnd)) {
				this->size.store(scan.validEnd, std::memory_order_relaxed);
				if (this->lastFlushedSize > scan.validEnd) {
					this->lastFlushedSize = scan.validEnd;
				}
				this->resetTimestampIndex();

				std::ostringstream msg;
				msg << "Transaction log " << this->path.string()
					<< " had a torn tail; dropped " << (fileSize - scan.validEnd)
					<< " partial byte(s) back to the last valid entry (new size=" << scan.validEnd << ").";
				DEBUG_LOG("%p TransactionLogFile::recoverTail WARNING: %s\n", this, msg.str().c_str());
				emitGlobalEvent("log.warn", ListenerData::fromStrings({ msg.str() }));

				// The partial bytes are gone; whole entries of the same interrupted
				// batch can still precede them.
				this->discardUnclosedTransaction(scan, scan.validEnd, protectedPosition);
			} else {
				DEBUG_LOG("%p TransactionLogFile::recoverTail Truncate failed (or unsupported on this platform) for %s\n",
					this, this->path.string().c_str());
			}
			return;
	}
}

void TransactionLogFile::discardUnclosedTransaction(
	const RecoveryScan& scan, uint32_t entriesEnd, uint32_t protectedPosition) {
	uint32_t boundary = scan.lastCompleteTransactionEnd;
	if (entriesEnd <= boundary) {
		return; // the file ends on a transaction boundary
	}
	if (boundary < protectedPosition) {
		std::ostringstream msg;
		msg << "Transaction log " << this->path.string()
			<< " has an unclosed transaction crossing the flushed position " << protectedPosition
			<< "; leaving it intact to preserve durable log history.";
		DEBUG_LOG("%p TransactionLogFile::discardUnclosedTransaction WARNING: %s\n",
			this, msg.str().c_str());
		emitGlobalEvent("log.warn", ListenerData::fromStrings({ msg.str() }));
		return;
	}

	// A zero boundary means no entry in this file closed a transaction. Either the
	// whole file is the tail of a batch that began in an earlier one, or it predates
	// the last-entry flag entirely — neither is safe to discard from here, so leave
	// the bytes and let the store fall back to seeding its watermark from an older
	// file. A non-zero boundary is the proof that this writer sets the flag.
	if (boundary == 0) {
		DEBUG_LOG("%p TransactionLogFile::discardUnclosedTransaction No closed transaction in %s; leaving %u trailing entries intact\n",
			this, this->path.string().c_str(), scan.unclosedTailEntries);
		return;
	}

	if (!scan.unclosedTailIsOneTransaction) {
		std::ostringstream msg;
		msg << "Transaction log " << this->path.string() << " ends with " << scan.unclosedTailEntries
			<< " unflagged entries spanning more than one timestamp after offset " << boundary
			<< "; leaving them intact rather than discarding what may be complete transactions "
			   "written before the last-entry flag existed.";
		DEBUG_LOG("%p TransactionLogFile::discardUnclosedTransaction WARNING: %s\n", this, msg.str().c_str());
		emitGlobalEvent("log.warn", ListenerData::fromStrings({ msg.str() }));
		return;
	}

	// The trailing run is one transaction whose final entry never reached disk, so
	// its RocksDB commit never ran either: writeBatch() completes before
	// Transaction::Commit() in every commit path, and both lanes of the commit
	// thread preserve dispatch order, so an interrupted log write is always the
	// newest thing in the log and nothing durable depends on it. Dropping it is what
	// keeps the "a log never holds an unclosed transaction" invariant enforceable
	// rather than merely observed by the watermark: the bytes cannot later be
	// swallowed into the next batch's group by that batch's flag.
	if (!this->eraseTail(boundary, entriesEnd)) {
		DEBUG_LOG("%p TransactionLogFile::discardUnclosedTransaction Erase failed (or unsupported on this platform) for %s\n",
			this, this->path.string().c_str());
		return;
	}

	this->size.store(boundary, std::memory_order_relaxed);
	if (this->lastFlushedSize > boundary) {
		this->lastFlushedSize = boundary;
	}
	this->resetTimestampIndex();

	std::ostringstream msg;
	msg << "Transaction log " << this->path.string() << " ended mid-transaction; dropped "
		<< scan.unclosedTailEntries << " entry(s) (" << (entriesEnd - boundary)
		<< " bytes) of a transaction that never closed, back to the last complete transaction (new size="
		<< boundary << ").";
	DEBUG_LOG("%p TransactionLogFile::discardUnclosedTransaction WARNING: %s\n", this, msg.str().c_str());
	emitGlobalEvent("log.warn", ListenerData::fromStrings({ msg.str() }));
}

void TransactionLogFile::resetTimestampIndex() {
	std::lock_guard<std::mutex> indexLock(this->indexMutex);
	this->positionByTimestampIndex.clear();
	this->lastIndexedPosition = TRANSACTION_LOG_FILE_TIMESTAMP_POSITION;
}

uint32_t TransactionLogFile::countEntries() const {
	// Counting must never abort a purge, so swallow every failure (I/O errors and
	// a std::bad_alloc from the whole-file buffer below) and report 0.
	try {
		std::error_code ec;
		auto onDiskSize = std::filesystem::file_size(this->path, ec);
		if (ec || onDiskSize <= TRANSACTION_LOG_FILE_HEADER_SIZE) {
			// missing, empty, or header-only: no entries
			return 0;
		}
		if (onDiskSize > std::numeric_limits<uint32_t>::max()) {
			// transaction log files are bounded well under 4 GiB; refuse an absurd size
			DEBUG_LOG("%p TransactionLogFile::countEntries File too large to count: %s (size=%llu)\n",
				this, this->path.string().c_str(), static_cast<unsigned long long>(onDiskSize));
			return 0;
		}

		auto fileSize = static_cast<uint32_t>(onDiskSize);
		std::vector<char> buffer(fileSize);

		// Read through a fresh handle rather than this->fd: old purgeable files are
		// never opened (only the current sequence file is), and the read must work
		// regardless. POSIX/Windows both share read access, so this is safe even when
		// the file is concurrently open.
		std::ifstream stream(this->path, std::ios::binary);
		if (!stream) {
			DEBUG_LOG("%p TransactionLogFile::countEntries Failed to open file for counting: %s\n",
				this, this->path.string().c_str());
			return 0;
		}
		stream.read(buffer.data(), fileSize);
		auto bytesRead = stream.gcount();
		if (bytesRead < 0) {
			return 0;
		}
		if (static_cast<uint32_t>(bytesRead) != fileSize) {
			// short read (e.g. the file shrank); count only what we actually read
			DEBUG_LOG("%p TransactionLogFile::countEntries Short read while counting: %s (read=%lld, size=%u)\n",
				this, this->path.string().c_str(), static_cast<long long>(bytesRead), fileSize);
			fileSize = static_cast<uint32_t>(bytesRead);
		}

		return countTransactionLogEntries(buffer.data(), fileSize);
	} catch (...) {
		DEBUG_LOG("%p TransactionLogFile::countEntries Failed to count entries: %s\n",
			this, this->path.string().c_str());
		return 0;
	}
}

void TransactionLogFile::writeEntries(TransactionLogEntryBatch& batch, const uint32_t maxFileSize) {
	DEBUG_LOG("%p TransactionLogFile::writeEntries Writing batch with %zu entries, current entry index=%zu (timestamp=%f, maxFileSize=%u, currentSize=%u)\n",
		this, batch.entries.size(), batch.currentEntryIndex, batch.timestamp, maxFileSize, this->size.load(std::memory_order_relaxed));

	// Mark that appends are now occurring on this file (regardless of format version), so a concurrent
	// reader's index build will no longer treat a transiently-zero (not-yet-visible) entry as
	// end-of-file and truncate this->size (see findPositionByTimestamp and hasAppendedSinceOpen). Set
	// before writing so a reader racing this first append observes it (it is read after this->size).
	this->hasAppendedSinceOpen.store(true);

	// branch based on file format version
	if (this->version == 1) {
		this->writeEntriesV1(batch, maxFileSize);
	} else {
		DEBUG_LOG("%p TransactionLogFile::writeEntries Unsupported transaction log file version: %s\n", this, this->path.string().c_str());
		throw rocksdb_js::DBException("Unsupported transaction log file version: " + std::to_string(this->version));
	}
}

void TransactionLogFile::writeEntriesV1(TransactionLogEntryBatch& batch, const uint32_t maxFileSize) {
	std::lock_guard<std::mutex> fileLock(this->fileMutex);
	uint32_t numEntriesToWrite = 0;
	uint32_t totalSizeToWrite = 0;

	if (this->appendBoundaryLost.load(std::memory_order_relaxed)) {
		// Bytes we could not erase sit past `size`; appending here would put valid
		// entries on the far side of them. Write nothing, which the store reads as
		// "no progress" and answers by rotating to the next file — leaving these
		// bytes as a trailing partial that recoverTail() can still truncate.
		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Append boundary lost, deferring to next file: %s\n",
			this, this->path.string().c_str());
		return;
	}

	// check if the file is at or over the max size
	if (maxFileSize > 0) {
		if (this->size >= maxFileSize) {
			DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 File already at max size (%u >= %u), deferring to next file\n",
				this, this->size.load(std::memory_order_relaxed), maxFileSize);
			return;
		}

		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Calculating how many entries we can fit (size=%u, maxFileSize=%u)\n", this, this->size.load(std::memory_order_relaxed), maxFileSize);

		// calculate how many entries we can fit
		auto availableSpace = maxFileSize - this->size;
		for (size_t i = batch.currentEntryIndex; i < batch.entries.size(); ++i) {
			auto& entry = batch.entries[i];
			auto spaceNeeded = totalSizeToWrite + entry->size;
			// always write the first entry
			if ((this->size > TRANSACTION_LOG_FILE_HEADER_SIZE || i > batch.currentEntryIndex) && spaceNeeded > availableSpace) {
				// entry won't fit
				DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Entry %u won't fit (need=%u, available=%u)\n", this, i, spaceNeeded, availableSpace);
				break;
			}
			DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Entry %u fits (need=%u, available=%u)\n", this, i, spaceNeeded, availableSpace);
			++numEntriesToWrite;
			totalSizeToWrite += entry->size;
		}
	} else {
		// unlimited space, write all entries
		numEntriesToWrite = batch.entries.size() - batch.currentEntryIndex;
	}

	if (numEntriesToWrite == 0) {
		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 No entries to write\n", this);
		return;
	}

	DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Writing %u entries to file (%u bytes)\n", this, numEntriesToWrite, totalSizeToWrite);

	// Use a stack buffer for small batches to avoid a heap alloc per commit.
	iovec stackIovecs[8];
	auto heapIovecs = numEntriesToWrite > 8 ? std::make_unique<iovec[]>(numEntriesToWrite) : nullptr;
	iovec* iovecs = heapIovecs ? heapIovecs.get() : stackIovecs;
	size_t iovecsIndex = 0;
	uint32_t startEntryIndex = batch.currentEntryIndex;

	// write the transaction headers and entry data to the iovecs
	for (uint32_t i = 0; i < numEntriesToWrite; ++i) {
		auto& entry = batch.entries[batch.currentEntryIndex];
		auto data = entry->data.get();

		// Write the timestamp into the transaction header
		// Note: the rest of the transaction header is written in the
		// `TransactionLogEntry` constructor
		writeDoubleBE(data, batch.timestamp); // actual timestamp
		if (batch.currentEntryIndex == batch.entries.size() - 1) {
			// Last entry in batch, set the last entry flag
			uint8_t flags = readUint8(data + 12);
			writeUint8(data + 12, flags | TRANSACTION_LOG_ENTRY_LAST_FLAG);
		}

		// add the entry data to the iovecs
		iovecs[iovecsIndex++] = {data, entry->size};

		++batch.currentEntryIndex;
	}

	int64_t bytesLanded = 0;
	int64_t bytesWritten = this->writeBatchToFile(iovecs, static_cast<int>(iovecsIndex), bytesLanded);
	if (bytesWritten < 0) {
		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 ERROR: Failed to write transaction log entries to file: %s (%lld byte(s) landed)\n",
			this, this->path.string().c_str(), static_cast<long long>(bytesLanded));

		// Anything that reached the file is a partial entry nothing acknowledged,
		// and `size` never advanced over it. Left in place it is a permanent
		// framing break: the fd is O_APPEND, so the next successful append lands
		// *after* these bytes, and recoverTail() then refuses to repair the break
		// because valid entries follow it.
		uint32_t committedSize = this->size.load(std::memory_order_relaxed);
		if (bytesLanded > 0 && !this->eraseTail(committedSize, committedSize + static_cast<uint32_t>(bytesLanded))) {
			// Retire the file rather than append past bytes we could not remove.
			this->appendBoundaryLost.store(true, std::memory_order_relaxed);

			std::ostringstream msg;
			msg << "Transaction log " << this->path.string() << " kept " << bytesLanded
				<< " orphaned byte(s) from a failed append at offset " << committedSize
				<< " and could not remove them; no further entries will be written to this file.";
			DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 WARNING: %s\n", this, msg.str().c_str());
			emitGlobalEvent("log.warn", ListenerData::fromStrings({ msg.str() }));
		}

		batch.currentEntryIndex = startEntryIndex;

		throw rocksdb_js::DBException("Failed to write transaction log entries to file: " + this->path.string());
	}

	this->size += static_cast<uint32_t>(bytesWritten);
#if TRANSACTION_LOG_ENABLE_ANONYMOUS_OVERLAY
	this->updateMemoryMapOverlay();
#endif
	DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Wrote %lld bytes to log file (size=%u, batch state: entryIndex=%zu)\n",
		this, bytesWritten, this->size.load(std::memory_order_relaxed), batch.currentEntryIndex);
}

// Public entry point: acquire fileMutex (the guard for memoryMap) and delegate to
// the platform getMemoryMapLocked(). Callers that already hold fileMutex (the open
// path, via findPositionByTimestamp) must call getMemoryMapLocked() directly.
std::shared_ptr<MemoryMap> TransactionLogFile::getMemoryMap(uint32_t fileSize, bool isCurrent) {
	std::lock_guard<std::mutex> fileLock(this->fileMutex);
	return this->getMemoryMapLocked(fileSize, isCurrent);
}

/**
 * Find the start position for a forward scan of all transactions with timestamp >= the given one:
 * the position of the first entry whose timestamp is >= `timestamp`, such that no entry BEFORE it
 * has a timestamp >= `timestamp` (a tight lower bound to begin scanning). This is a running-maxima
 * index, so it gives no UPPER bound — on an out-of-order log, entries with timestamp >= `timestamp`
 * may still appear after the returned position, and an absent `timestamp` scans to end-of-log. The
 * caller filters each entry and is responsible for any stop condition.
 * @param timestamp - the timestamp to find the start position for
 * @param mapSize - the size of the memory map to search in
 * @return the position of the first entry >= timestamp, or zero if it precedes this log file's
 *         header timestamp, or 0xFFFFFFFF if it follows every entry in this log file
 */
uint32_t TransactionLogFile::findPositionByTimestamp(double timestamp, uint32_t mapSize, bool isCurrent, bool fileMutexHeld) {
	DEBUG_LOG("%p TransactionLogFile::findPositionByTimestamp Finding position for timestamp=%f, mapSize=%u\n", this, timestamp, mapSize);

	// getMemoryMapLocked() (re)assigns this->memoryMap and so must run under
	// fileMutex. The open() -> openFile() -> here path already holds it; every
	// other caller does not. Take fileMutex only for the mapping call and release
	// it before the scan: the scan reads through the pinned shared_ptr copy and
	// must stay concurrent with appends (a zero timestamp mid-scan is a not-yet-
	// visible append, not EOF — see hasAppendedSinceOpen; HarperFast/harper#1148).
	// Acquiring fileMutex before indexMutex (and never the reverse) keeps the
	// fileMutex -> indexMutex order; the open path nests them in that same order.
	std::shared_ptr<MemoryMap> memoryMap;
	if (fileMutexHeld) {
		memoryMap = this->getMemoryMapLocked(mapSize, isCurrent);
	} else {
		std::lock_guard<std::mutex> fileLock(this->fileMutex);
		memoryMap = this->getMemoryMapLocked(mapSize, isCurrent);
	}

	// If memory map is null (e.g., empty file with size 0), return 0xFFFFFFFF
	// to indicate the timestamp comes after this logfile
	if (!memoryMap) {
		DEBUG_LOG("%p TransactionLogFile::findPositionByTimestamp memoryMap is null, returning 0xFFFFFFFF\n", this);
		return 0xFFFFFFFF;
	}

	std::lock_guard<std::mutex> indexLock(this->indexMutex);

	// we use our memory maps for fast access to the data
	char* mappedFile = (char*) memoryMap->map;
	// We begin by indexing the file, so we can use fast ordered std::map access O(log n). We only need to index the file
	// that hasn't been indexed yet, so we start at the last indexed position. Note that there may be a slight benefit
	// to using an ordered vector with binary search for faster lookups, but std::map is simpler for now is very close in performance
	// Set when indexing stops early at a committed-but-not-yet-visible tail (a concurrent append we
	// couldn't read this pass); used below to start the scan at lastIndexedPosition rather than EOF.
	bool stoppedAtUnindexedTail = false;
	while (this->lastIndexedPosition < this->size) {
		double entryTimestamp = readDoubleBE(mappedFile + this->lastIndexedPosition);
		// The header's own timestamp slot is not an entry, so a legitimate value of exactly
		// zero there (e.g. an unset/epoch file timestamp) must not be mistaken for the
		// zero-padding end-of-data marker below — that heuristic only applies once we're
		// scanning actual entries past the header. Record it and advance to the first entry
		// unconditionally, before the end-of-data check gets a chance to truncate this->size
		// back into the header itself.
		if (TRANSACTION_LOG_FILE_TIMESTAMP_POSITION == this->lastIndexedPosition) {
			// specifically record the log file timestamp as the first entry with a position of zero
			positionByTimestampIndex.insert({entryTimestamp, 0});
			this->lastIndexedPosition = TRANSACTION_LOG_FILE_HEADER_SIZE; // move to the first transaction entry
			continue;
		}
		if (entryTimestamp == 0) {
			// A zero timestamp marks the end of the written data. Only correct this->size down to the
			// true written extent when no entries have been appended since (re)open — i.e. during
			// startup replay, where the on-disk size can include memory-map zero-padding (Windows
			// extends files to the map size) and there are no concurrent writers. Once appends have
			// begun, a zero here is a transient artifact of this reader's memory-map view lagging a
			// concurrent append (size is bumped only after the bytes are written): mutating the
			// append-owned size would truncate it and freeze the index, intermittently hiding
			// committed entries (HarperFast/harper#1148). Reads during writes are bounded by the
			// committed position, so we just stop indexing here and resume from lastIndexedPosition
			// on a later call once the bytes are visible.
			if (!this->hasAppendedSinceOpen.load()) {
				this->size = this->lastIndexedPosition;
			} else {
				stoppedAtUnindexedTail = true;
			}
			break;
		}
		// check that the timestamp is greater than any previously indexed timestamp,
		// otherwise we don't record it, because we want to start at the first position with a timestamp that
		// is greater than the requested timestamp:
		if (entryTimestamp > positionByTimestampIndex.rbegin()->first) {
			// insert with a hint to go at the end (constant time?)
			positionByTimestampIndex.insert(positionByTimestampIndex.end(), {entryTimestamp, this->lastIndexedPosition});
		}
		// read size of the entry and move on
		this->lastIndexedPosition += TRANSACTION_LOG_ENTRY_HEADER_SIZE + readUint32BE(mappedFile + this->lastIndexedPosition + 8);
	}
	// now do the actual search: just a search for the lower bound
	auto it = this->positionByTimestampIndex.lower_bound(timestamp);
	if (it != this->positionByTimestampIndex.end()) {
		return it->second;
	}
	// The timestamp is past every indexed entry. If indexing stopped early at a committed-but-not-yet-
	// visible tail (a concurrent append), start the scan at lastIndexedPosition so the iterator covers
	// those just-committed entries — bounded by the committed position — rather than reporting the
	// timestamp as past EOF and missing them (HarperFast/harper#1148). Otherwise the timestamp genuinely
	// comes after this log file.
	return stoppedAtUnindexedTail ? this->lastIndexedPosition : 0xFFFFFFFF;
}

} // namespace rocksdb_js
