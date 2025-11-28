#include <cmath>
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

	if (!std::filesystem::exists(this->path)) {
		DEBUG_LOG("%p TransactionLogFile::getLastWriteTime ERROR: File does not exist: %s\n", this, this->path.string().c_str())
		throw std::filesystem::filesystem_error(
			"File does not exist",
			this->path,
			std::make_error_code(std::errc::no_such_file_or_directory)
		);
	}

	auto mtime = std::filesystem::last_write_time(this->path);
	return convertFileTimeToSystemTime(mtime);
}

void TransactionLogFile::open() {
	std::lock_guard<std::mutex> fileLock(this->fileMutex);
	this->openFile();

	// read the file header
	char buffer[TRANSACTION_LOG_FILE_HEADER_SIZE];
	if (this->size == 0) {
		// file is empty, initialize it
		DEBUG_LOG("%p TransactionLogFile::open Initializing empty file: %s\n", this, this->path.string().c_str())
		writeUint32BE(buffer, TRANSACTION_LOG_TOKEN);
		this->writeToFile(buffer, 4);
		writeUint8(buffer, this->version);
		this->writeToFile(buffer, 1);
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

		uint32_t token = readUint32BE(buffer);
		if (token != TRANSACTION_LOG_TOKEN) {
			DEBUG_LOG("%p TransactionLogFile::open ERROR: Invalid transaction log file: %s\n", this, this->path.string().c_str())
			throw std::runtime_error("Invalid transaction log file: " + this->path.string());
		}

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
	}

	DEBUG_LOG("%p TransactionLogFile::open Opened file %s (size=%zu)\n",
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
			DEBUG_LOG("%p TransactionLogFile::writeEntries File already at max size (%u >= %u), deferring to next file\n",
				this, this->size, maxFileSize)
			return;
		}

		// calculate how many entries we can fit
		for (size_t i = batch.currentEntryIndex; i < batch.entries.size(); ++i) {
			auto& entry = batch.entries[i];
			if (this->size > TRANSACTION_LOG_FILE_HEADER_SIZE && this->size + totalSizeToWrite + TRANSACTION_LOG_ENTRY_HEADER_SIZE + entry->size > maxFileSize) {
				// entry won't fit
				break;
			}
			numEntriesToWrite++;
			totalSizeToWrite += TRANSACTION_LOG_ENTRY_HEADER_SIZE + entry->size;
		}
	} else {
		// unlimited space, write all entries
		numEntriesToWrite = batch.entries.size() - batch.currentEntryIndex;
	}

	if (numEntriesToWrite == 0) {
		DEBUG_LOG("%p TransactionLogFile::writeEntries No entries to write\n", this)
		return;
	}

	// allocate buffers for the transaction headers and iovecs
	auto txnHeaders = std::make_unique<char[]>(numEntriesToWrite * TRANSACTION_LOG_ENTRY_HEADER_SIZE);
	auto numIovecs = numEntriesToWrite * 2;
	auto iovecs = std::make_unique<iovec[]>(numIovecs);
	size_t iovecsIndex = 0;

	// write the transaction headers and entry data to the iovecs
	for (; batch.currentEntryIndex < numEntriesToWrite; ++batch.currentEntryIndex) {
		auto& entry = batch.entries[batch.currentEntryIndex];

		// write the transaction header
		auto txnHeader = txnHeaders.get() + (batch.currentEntryIndex * TRANSACTION_LOG_ENTRY_HEADER_SIZE);
		writeDoubleBE(txnHeader, batch.timestamp); // actual timestamp
		writeUint32BE(txnHeader + 8, static_cast<uint32_t>(entry->size)); // data length
		writeUint8(txnHeader + 12, 0); // flags

		// add the transaction header to the iovecs
		iovecs[iovecsIndex++] = {txnHeader, TRANSACTION_LOG_ENTRY_HEADER_SIZE};

		// add the entry data to the iovecs
		iovecs[iovecsIndex++] = {entry->data.get(), entry->size};
	}

	int64_t bytesWritten = this->writeBatchToFile(iovecs.get(), static_cast<int>(iovecsIndex));
	if (bytesWritten < 0) {
		DEBUG_LOG("%p TransactionLogFile::writeEntries ERROR: Failed to write transaction log entries to file: %s\n", this, this->path.string().c_str())
		throw std::runtime_error("Failed to write transaction log entries to file: " + this->path.string());
	}

	DEBUG_LOG("%p TransactionLogFile::writeEntries Wrote %lld bytes to log file, batch state: entryIndex=%zu, bytesWritten=%zu\n",
		this, bytesWritten, batch.currentEntryIndex, batch.currentEntryBytesWritten)
	batch.currentEntryBytesWritten += static_cast<uint32_t>(bytesWritten);
	this->size += static_cast<uint32_t>(bytesWritten);
}

} // namespace rocksdb_js
