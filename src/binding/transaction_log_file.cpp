#include <cmath>
#include <system_error>
#include "transaction_log_entry.h"
#include "transaction_log_file.h"

// include platform-specific implementation
#ifdef PLATFORM_WINDOWS
	#include "transaction_log_file_windows.cpp"
#else
	#include "transaction_log_file_posix.cpp"
#endif

namespace rocksdb_js {

TransactionLogFile::~TransactionLogFile() {
	this->close();
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

	// read the file header
	char buffer[TRANSACTION_LOG_FILE_HEADER_SIZE];
	if (this->size == 0) {
		// file is empty, initialize it
		DEBUG_LOG("%p TransactionLogFile::open Initializing empty file: %s (timestamp=%f)\n", this, this->path.string().c_str(), latestTimestamp)
		writeUint32BE(buffer, TRANSACTION_LOG_TOKEN);
		this->writeToFile(buffer, 4);
		writeUint8(buffer, this->version);
		this->writeToFile(buffer, 1);
		this->timestamp = latestTimestamp;
		writeDoubleBE(buffer, this->timestamp);
		this->writeToFile(buffer, 8);
		this->size = TRANSACTION_LOG_FILE_HEADER_SIZE;
	} else if (this->size < TRANSACTION_LOG_FILE_HEADER_SIZE) {
		DEBUG_LOG("%p TransactionLogFile::open ERROR: File is too small to be a valid transaction log file: %s\n", this, this->path.string().c_str())
		throw std::runtime_error("File is too small to be a valid transaction log file: " + this->path.string());
	} else {
		// try to read the token and version from the log file
		int64_t result = this->readFromFile(buffer, TRANSACTION_LOG_FILE_HEADER_SIZE, 0);
		if (result < 0) {
			DEBUG_LOG("%p TransactionLogFile::open ERROR: Failed to read version from file: %s\n", this, this->path.string().c_str())
			throw std::runtime_error("Failed to read version from file: " + this->path.string());
		}

		// token
		uint32_t token = readUint32BE(buffer);
		if (token != TRANSACTION_LOG_TOKEN) {
			DEBUG_LOG("%p TransactionLogFile::open ERROR: Invalid transaction log file: %s\n", this, this->path.string().c_str())
			throw std::runtime_error("Invalid transaction log file: " + this->path.string());
		}

		// version
		result = this->readFromFile(buffer, 1, 4);
		if (result < 0) {
			DEBUG_LOG("%p TransactionLogFile::open ERROR: Failed to read version from file: %s\n", this, this->path.string().c_str())
			throw std::runtime_error("Failed to read version from file: " + this->path.string());
		}
		this->version = readUint8(buffer);

		if (this->version != 1) {
			DEBUG_LOG("%p TransactionLogFile::open ERROR: Unsupported transaction log file version: %s\n", this, this->path.string().c_str())
			throw std::runtime_error("Unsupported transaction log file version: " + std::to_string(this->version));
		}

		// file timestamp
		result = this->readFromFile(buffer, 8, 5);
		if (result < 0) {
			DEBUG_LOG("%p TransactionLogFile::open ERROR: Failed to read file timestamp from file: %s\n", this, this->path.string().c_str())
			throw std::runtime_error("Failed to read file timestamp from file: " + this->path.string());
		}
		this->timestamp = readDoubleBE(buffer);

		DEBUG_LOG("%p TransactionLogFile::open Opened file %s (size=%zu, version=%u, timestamp=%f)\n",
			this, this->path.string().c_str(), this->size, this->version, this->timestamp)
	}

	DEBUG_LOG("%p TransactionLogFile::open Opened file %s (size=%u)\n",
		this, this->path.string().c_str(), this->size)
}

void TransactionLogFile::writeEntries(TransactionLogEntryBatch& batch, const uint32_t maxFileSize) {
	DEBUG_LOG("%p TransactionLogFile::writeEntries Writing batch with %zu entries, current entry index=%zu, bytes written=%zu (timestamp=%f, maxFileSize=%u, currentSize=%u)\n",
		this, batch.entries.size(), batch.currentEntryIndex, batch.currentEntryBytesWritten, batch.timestamp, maxFileSize, this->size)

	// branch based on file format version
	if (this->version == 1) {
		this->writeEntriesV1(batch, maxFileSize);
	} else {
		DEBUG_LOG("%p TransactionLogFile::writeEntries Unsupported transaction log file version: %s\n", this, this->path.string().c_str())
		throw std::runtime_error("Unsupported transaction log file version: " + std::to_string(this->version));
	}
}

void TransactionLogFile::writeEntriesV1(TransactionLogEntryBatch& batch, const uint32_t maxFileSize) {
	std::lock_guard<std::mutex> fileLock(this->fileMutex);
	uint32_t numEntriesToWrite = 0;
	uint32_t totalSizeToWrite = 0;

	// check if the file is at or over the max size
	if (maxFileSize > 0) {
		if (this->size >= maxFileSize) {
			DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 File already at max size (%u >= %u), deferring to next file\n",
				this, this->size, maxFileSize)
			return;
		}

		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Calculating how many entries we can fit (size=%u, maxFileSize=%u)\n", this, this->size, maxFileSize)

		// calculate how many entries we can fit
		auto availableSpace = maxFileSize - this->size;
		for (size_t i = batch.currentEntryIndex; i < batch.entries.size(); ++i) {
			auto& entry = batch.entries[i];
			auto spaceNeeded = totalSizeToWrite + entry->size;
			// always write the first entry
			if ((this->size > TRANSACTION_LOG_FILE_HEADER_SIZE || i > batch.currentEntryIndex) && spaceNeeded > availableSpace) {
				// entry won't fit
				DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Entry %u won't fit (need=%u, available=%u)\n", this, i, spaceNeeded, availableSpace)
				break;
			}
			DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Entry %u fits (need=%u, available=%u)\n", this, i, spaceNeeded, availableSpace)
			++numEntriesToWrite;
			totalSizeToWrite += entry->size;
		}
	} else {
		// unlimited space, write all entries
		numEntriesToWrite = batch.entries.size() - batch.currentEntryIndex;
	}

	if (numEntriesToWrite == 0) {
		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 No entries to write\n", this)
		return;
	}

	DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Writing %u entries to file (%u bytes)\n", this, numEntriesToWrite, totalSizeToWrite)

	// allocate buffers for the transaction headers and iovecs
	auto iovecs = std::make_unique<iovec[]>(numEntriesToWrite);
	size_t iovecsIndex = 0;

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

	int64_t bytesWritten = this->writeBatchToFile(iovecs.get(), static_cast<int>(iovecsIndex));
	if (bytesWritten < 0) {
		DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 ERROR: Failed to write transaction log entries to file: %s\n", this, this->path.string().c_str())
		throw std::runtime_error("Failed to write transaction log entries to file: " + this->path.string());
	}

	batch.currentEntryBytesWritten += static_cast<uint32_t>(bytesWritten);
	this->size += static_cast<uint32_t>(bytesWritten);
	DEBUG_LOG("%p TransactionLogFile::writeEntriesV1 Wrote %lld bytes to log file (size=%u, batch state: entryIndex=%zu, bytesWritten=%zu)\n",
		this, bytesWritten, this->size, batch.currentEntryIndex, batch.currentEntryBytesWritten)
}

/**
 * Find the position of a timestamp, specifically the position where there are no transactions with an earlier timestamp
 * @param timestamp - the timestamp to find the position of
 * @param mapSize - the size of the memory map to search in
 * @return the position of the timestamp, or zero if comes before this logfile, or 0xFFFFFFFF if it comes after this logfile
 */
uint32_t TransactionLogFile::findPositionByTimestamp(double timestamp, uint32_t mapSize) {
	std::lock_guard<std::mutex> indexLock(this->indexMutex);
	auto memoryMap = this->getMemoryMap(mapSize);

	// we use our memory maps for fast access to the data
	char* mappedFile = (char*) memoryMap->map;
	// We begin by indexing the file, so we can use fast ordered std::map access O(log n). We only need to index the file
	// that hasn't been indexed yet, so we start at the last indexed position. Note that there may be a slight benefit
	// to using an ordered vector with binary search for faster lookups, but std::map is simpler for now is very close in performance
	while (this->lastIndexedPosition < this->size) {
		double entryTimestamp = readDoubleBE(mappedFile + this->lastIndexedPosition);
		if (entryTimestamp == 0) {
			// this means we have reached the end of zero-padded file (usually Windows), adjust size and break out of the loop
			this->size = this->lastIndexedPosition;
			break;
		}
		// for the first iteration, we insert the log file timestamp at the beginning of the index
		if (TRANSACTION_LOG_FILE_TIMESTAMP_POSITION == this->lastIndexedPosition) {
			// specifically record the log file timestamp as the first entry with a position of zero
			positionByTimestampIndex.insert({entryTimestamp, 0});
			this->lastIndexedPosition = TRANSACTION_LOG_FILE_HEADER_SIZE; // move to the first transaction entry
			continue;
			// else check that the timestamp is greater than any previously indexed timestamp,
			// otherwise we don't record it, because we want to start at the first position with a timestamp that
			// is greater than the requested timestamp:
		} else if (entryTimestamp > positionByTimestampIndex.rbegin()->first) {
			// insert with a hint to go at the end (constant time?)
			positionByTimestampIndex.insert(positionByTimestampIndex.end(), {entryTimestamp, this->lastIndexedPosition});
		}
		// read size of the entry and move on
		this->lastIndexedPosition += TRANSACTION_LOG_ENTRY_HEADER_SIZE + readUint32BE(mappedFile + this->lastIndexedPosition + 8);
	}
	// now do the actual search: just a search for the lower bound
	auto it = this->positionByTimestampIndex.lower_bound(timestamp);
	return it == this->positionByTimestampIndex.end() ? 0xFFFFFFFF : it->second;
}

} // namespace rocksdb_js
