#include <chrono>
#include <sstream>
#include <vector>
#include "macros.h"
#include "transaction_log_store.h"
#include "util.h"

namespace rocksdb_js {

TransactionLogStore::TransactionLogStore(
	const std::string& name,
	const std::filesystem::path& logsDirectory,
	const uint32_t maxSize,
	const std::chrono::milliseconds& retentionMs
) :
	name(name),
	logsDirectory(logsDirectory),
	maxSize(maxSize),
	retentionMs(retentionMs),
	currentSequenceNumber(1),
	nextSequenceNumber(2),
	isClosing(false)
{
	DEBUG_LOG("%p TransactionLogStore::TransactionLogStore New transaction log store: %s (maxSize=%u)\n", this, name.c_str(), maxSize)
}

/**
 * Adds an entry to the transaction log store.
 */
void TransactionLogStore::addEntry(const uint64_t timestamp, const char* data, const size_t size) {
	// TODO: if this `addEntry()` call is a transaction, then buffer the entry
	// until `commit()` is called

	std::unique_lock<std::mutex> lock(this->storeMutex);

	// get the current log file and rotate if needed
	auto logFile = this->openLogFile(this->currentSequenceNumber);
	if (logFile->size >= this->maxSize) {
		DEBUG_LOG("%p TransactionLogStore::addEntry Store is full, rotating to next sequence number: %u\n",
			this, this->nextSequenceNumber)
		this->currentSequenceNumber = this->nextSequenceNumber++;
		logFile = this->openLogFile(this->currentSequenceNumber);
	}

	DEBUG_LOG("%p TransactionLogStore::addEntry Adding entry to store: %s (seq=%u, size=%zu, maxSize=%u)\n",
		this, this->name.c_str(), this->currentSequenceNumber, logFile->size.load(), this->maxSize)

	lock.unlock();
	logFile->writeToFile(data, size);
}

/**
 * Closes the transaction log store and all associated log files.
 * This method waits for all active operations to complete before closing.
 */
void TransactionLogStore::close() {
	// set the closing flag to prevent concurrent closes
	bool expected = false;
	if (!this->isClosing.compare_exchange_strong(expected, true)) {
		// already closing, return early
		DEBUG_LOG("%p TransactionLogStore::close Already closing, skipping: %s\n", this, this->name.c_str())
		return;
	}

	std::unique_lock<std::mutex> lock(this->storeMutex);

	DEBUG_LOG("%p TransactionLogStore::close Closing transaction log store: %s\n", this, this->name.c_str())

	for (const auto& [sequenceNumber, logFile] : this->sequenceFiles) {
		DEBUG_LOG("%p TransactionLogStore::close Closing log file: %s\n", this, logFile->path.string().c_str())
		logFile->close();
	}

	lock.unlock();
	this->purge();
}

/**
 * Opens a log file for the given sequence number. If the log file does not
 * exist, it will be created.
 *
 * NOTE: This method must be called with storeMutex already locked.
 *
 * @param sequenceNumber The sequence number of the log file to open.
 * @returns The log file.
 */
TransactionLogFile* TransactionLogStore::openLogFile(const uint32_t sequenceNumber) {
	auto it = this->sequenceFiles.find(sequenceNumber);
	auto logFile = it != this->sequenceFiles.end() ? it->second.get() : nullptr;

	if (!logFile) {
		// ensure the directory exists before creating the file
		std::filesystem::create_directories(this->logsDirectory);

		std::ostringstream oss;
		oss << this->logsDirectory.string() << "/" << this->name << "." << sequenceNumber << ".txnlog";
		logFile = new TransactionLogFile(std::filesystem::path(oss.str()), sequenceNumber);
		this->sequenceFiles[sequenceNumber] = std::unique_ptr<TransactionLogFile>(logFile);
	}

	logFile->open();
	return logFile;
}

/**
 * Queries the transaction log store.
 */
void TransactionLogStore::query() {
	DEBUG_LOG("%p TransactionLogStore::query Querying transaction log store: %s\n", this, this->name.c_str())

	// TODO: Implement
	// 1. Determine files to iterate over
	// 2. Open each file with a memory map
	// 3. Iterate over the data in the memory map and copy to the supplied buffer
	// 4. Close the memory mapped files
}

/**
 * Purges transaction logs.
 */
void TransactionLogStore::purge(std::function<void(const std::filesystem::path&)> visitor, const bool all) {
	std::unique_lock<std::mutex> lock(this->storeMutex);

	DEBUG_LOG("%p TransactionLogStore::purge Purging transaction log store: %s (files=%u)\n", this, this->name.c_str(), this->sequenceFiles.size())

	// collect sequence numbers to remove to avoid modifying map during iteration
	std::vector<uint32_t> sequenceNumbersToRemove;

	for (const auto& entry : this->sequenceFiles) {
		auto& logFile = entry.second;

		bool shouldPurge = all;
		if (!shouldPurge && this->retentionMs.count() > 0) {
			try {
				auto mtime = logFile->getLastWriteTime();
				auto now = std::chrono::system_clock::now();
				auto fileAge = now - mtime;
				auto fileAgeMs = std::chrono::duration_cast<std::chrono::milliseconds>(fileAge);

				if (fileAgeMs <= this->retentionMs) {
					continue; // file is too new, don't purge
				}
			} catch (const std::filesystem::filesystem_error& e) {
				// file was deleted or doesn't exist
				DEBUG_LOG("%p TransactionLogStore::purge File no longer exists: %s\n", this, logFile->path.string().c_str())
				continue;
			}
		}

		DEBUG_LOG("%p TransactionLogStore::purge Purging log file: %s\n", this, logFile->path.string().c_str())

		// delete the log file
		logFile->close();
		std::filesystem::remove(logFile->path);

		// call the visitor
		if (visitor) {
			visitor(logFile->path);
		}

		// collect sequence number for removal
		sequenceNumbersToRemove.push_back(entry.first);
	}

	// remove sequence files from the map
	for (uint32_t sequenceNumber : sequenceNumbersToRemove) {
		this->sequenceFiles.erase(sequenceNumber);
	}

	// if all log files have been removed, clean up the empty directory
	if (this->sequenceFiles.empty() && std::filesystem::exists(this->logsDirectory)) {
		try {
			std::filesystem::remove(this->logsDirectory);
			DEBUG_LOG("%p TransactionLogStore::purge Removed empty log directory: %s\n", this, this->logsDirectory.string().c_str())
		} catch (const std::filesystem::filesystem_error& e) {
			DEBUG_LOG("%p TransactionLogStore::purge Failed to remove log directory %s: %s\n", this, this->logsDirectory.string().c_str(), e.what())
		}
	}
}

/**
 * Registers a log file for the given sequence number.
 *
 * @param path The path to the log file to register.
 * @param sequenceNumber The sequence number of the log file to register.
 */
void TransactionLogStore::registerLogFile(const std::filesystem::path& path, const uint32_t sequenceNumber) {
	std::unique_lock<std::mutex> lock(this->storeMutex);

	auto logFile = std::make_unique<TransactionLogFile>(path, sequenceNumber);
	this->sequenceFiles[sequenceNumber] = std::move(logFile);

	if (sequenceNumber > this->currentSequenceNumber) {
		this->currentSequenceNumber = sequenceNumber;
	}

	// update next sequence number to be one higher than the highest existing
	if (sequenceNumber > this->nextSequenceNumber) {
		this->nextSequenceNumber = sequenceNumber + 1;
	}

	DEBUG_LOG("%p TransactionLogStore::registerLogFile Added log file: %s (seq=%u)\n",
		this, path.string().c_str(), sequenceNumber)
}

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
std::shared_ptr<TransactionLogStore> TransactionLogStore::load(
	const std::filesystem::path& path,
	const uint32_t maxSize,
	const std::chrono::milliseconds& retentionMs
) {
	auto dirName = path.filename().string();

	// skip directories that start with "."
	if (dirName.empty() || dirName[0] == '.') {
		return nullptr;
	}

	std::shared_ptr<TransactionLogStore> store = std::make_shared<TransactionLogStore>(dirName, path, maxSize, retentionMs);

	// find `.txnlog` files in the directory
	for (const auto& fileEntry : std::filesystem::directory_iterator(path)) {
		if (fileEntry.is_regular_file() && fileEntry.path().extension() == ".txnlog") {
			auto filePath = fileEntry.path();
			auto filename = filePath.filename().string();

			// parse sequence number: {dirName}.{sequenceNumber}.txnlog
			size_t lastDot = filename.find_last_of('.');
			if (lastDot == std::string::npos) {
				// this should never happen since we know the file is a `.txnlog` file
				continue;
			}

			size_t secondLastDot = filename.find_last_of('.', lastDot - 1);
			if (secondLastDot == std::string::npos) {
				// no sequence number
				continue;
			}

			if (filename.substr(0, secondLastDot) != dirName) {
				// filename doesn't match the directory name
				continue;
			}

			std::string sequenceNumberStr = filename.substr(secondLastDot + 1, lastDot - secondLastDot - 1);
			uint32_t sequenceNumber = 0;

			try {
				sequenceNumber = std::stoul(sequenceNumberStr);
			} catch (const std::exception& e) {
				DEBUG_LOG(
					"DBDescriptor::discoverTransactionLogStores Invalid sequence number in file: %s\n",
					filename.c_str()
				)
			}

			// check if the file is too old
			if (store->retentionMs.count() > 0) {
				auto mtime = std::filesystem::last_write_time(filePath);
				auto now = std::chrono::system_clock::now();

				auto mtime_sys = std::chrono::system_clock::time_point(
					std::chrono::duration_cast<std::chrono::system_clock::duration>(
						mtime.time_since_epoch()));

				auto fileAge = now - mtime_sys;
				auto fileAgeMs = std::chrono::duration_cast<std::chrono::milliseconds>(fileAge);
				DEBUG_LOG("TransactionLogStore::load File age: %s %lld ms\n", filePath.string().c_str(), fileAgeMs.count())
				if (fileAgeMs > retentionMs) {
					// file is too old, remove it
					DEBUG_LOG("TransactionLogStore::load Purging old log file: %s\n", filePath.string().c_str())
					std::filesystem::remove(filePath);
					continue;
				}
			}

			store->registerLogFile(filePath, sequenceNumber);
		}
	}

	return store;
}

} // namespace rocksdb_js
