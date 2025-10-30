#include <chrono>
#include <sstream>
#include <vector>
#include "macros.h"
#include "transaction_log_store.h"
#include "util.h"

namespace rocksdb_js {

TransactionLogStore::TransactionLogStore(
	const std::string& name,
	const std::filesystem::path& path,
	const uint32_t maxSize,
	const std::chrono::milliseconds& retentionMs
) :
	name(name),
	path(path),
	maxSize(maxSize),
	retentionMs(retentionMs)
{}

TransactionLogStore::~TransactionLogStore() {
	DEBUG_LOG("%p TransactionLogStore::~TransactionLogStore Closing transaction log store \"%s\"\n", this, this->name.c_str())
	this->close();
}

void TransactionLogStore::close() {
	// set the closing flag to prevent concurrent closes
	bool expected = false;
	if (!this->isClosing.compare_exchange_strong(expected, true)) {
		// already closing, return early
		DEBUG_LOG("%p TransactionLogStore::close Already closing, skipping \"%s\"\n", this, this->name.c_str())
		return;
	}

	std::unique_lock<std::mutex> lock(this->storeMutex);
	DEBUG_LOG("%p TransactionLogStore::close Closing transaction log store \"%s\"\n", this, this->name.c_str())
	for (const auto& [sequenceNumber, logFile] : this->sequenceFiles) {
		DEBUG_LOG("%p TransactionLogStore::close Closing log file \"%s\"\n", this, logFile->path.string().c_str())
		logFile->close();
	}

	lock.unlock();
	this->purge();
}

void TransactionLogStore::commit(const uint64_t timestamp, const std::vector<std::unique_ptr<TransactionLogEntry>>& entries) {
	DEBUG_LOG("%p TransactionLogStore::commit Adding batch with %zu entries to store \"%s\" (timestamp=%llu)\n",
		this, entries.size(), this->name.c_str(), timestamp)

	std::lock_guard<std::mutex> lock(this->storeMutex);
	TransactionLogFile* logFile = nullptr;

	// get the current log file and rotate if needed
	while (logFile == nullptr && this->currentSequenceNumber) {
		logFile = this->getLogFile(this->currentSequenceNumber);

		// we found a log file, check the size
		if (logFile->size < this->maxSize) {
			try {
				logFile->open();
				break;
			} catch (const std::exception& e) {
				DEBUG_LOG("%p TransactionLogStore::commit Failed to open transaction log file: %s\n", this, e.what())
			}
		}

		this->currentSequenceNumber = this->nextSequenceNumber++;
	}

	// delegate to the log file to write the entries
	logFile->writeEntries(timestamp, entries);
}

TransactionLogFile* TransactionLogStore::getLogFile(const uint32_t sequenceNumber) {
	auto it = this->sequenceFiles.find(sequenceNumber);
	auto logFile = it != this->sequenceFiles.end() ? it->second.get() : nullptr;

	if (!logFile) {
		DEBUG_LOG("%p TransactionLogStore::getLogFile Store path \"%s\" (seq=%u) no log file found, creating\n",
			this, this->path.string().c_str(), sequenceNumber)

		// ensure the directory exists before creating the file (should already exist)
		std::filesystem::create_directories(this->path);

		std::ostringstream oss;
		oss << this->name << "." << sequenceNumber << ".txnlog";
		auto logFilePath = this->path / oss.str();
		logFile = new TransactionLogFile(logFilePath, sequenceNumber);
		this->sequenceFiles[sequenceNumber] = std::unique_ptr<TransactionLogFile>(logFile);
	}

	return logFile;
}

void TransactionLogStore::query() {
	DEBUG_LOG("%p TransactionLogStore::query Querying transaction log store \"%s\"\n", this, this->name.c_str())

	// TODO: Implement
	// 1. Determine files to iterate over
	// 2. Open each file with a memory map
	// 3. Iterate over the data in the memory map and copy to the supplied buffer
	// 4. Close the memory mapped files
}

void TransactionLogStore::purge(std::function<void(const std::filesystem::path&)> visitor, const bool all) {
	std::lock_guard<std::mutex> lock(this->storeMutex);

	DEBUG_LOG("%p TransactionLogStore::purge Purging transaction log store \"%s\" (files=%u)\n", this, this->name.c_str(), this->sequenceFiles.size())

	// collect sequence numbers to remove to avoid modifying map during iteration
	std::vector<uint32_t> sequenceNumbersToRemove;

	for (const auto& entry : this->sequenceFiles) {
		auto& logFile = entry.second;

		bool shouldPurge = all;
		if (!shouldPurge && this->retentionMs.count() > 0) {
			try {
				auto mtime = logFile->getLastWriteTime();
				auto now = std::chrono::system_clock::now();
				auto fileAgeMs = std::chrono::duration_cast<std::chrono::milliseconds>(now - mtime);

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
	if (this->sequenceFiles.empty() && std::filesystem::exists(this->path)) {
		try {
			std::filesystem::remove(this->path);
			DEBUG_LOG("%p TransactionLogStore::purge Removed empty log directory: %s\n", this, this->path.string().c_str())
		} catch (const std::filesystem::filesystem_error& e) {
			DEBUG_LOG("%p TransactionLogStore::purge Failed to remove log directory %s: %s\n", this, this->path.string().c_str(), e.what())
		}
	}
}

void TransactionLogStore::registerLogFile(const std::filesystem::path& path, const uint32_t sequenceNumber) {
	std::lock_guard<std::mutex> lock(this->storeMutex);

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
			if (retentionMs.count() > 0) {
				auto mtime = std::filesystem::last_write_time(filePath);
				auto mtime_sys = convertFileTimeToSystemTime(mtime);
				auto now = std::chrono::system_clock::now();
				auto fileAgeMs = std::chrono::duration_cast<std::chrono::milliseconds>(now - mtime_sys);
				auto delta = fileAgeMs - retentionMs;

				if (delta.count() > 0) {
					// file is too old, remove it
					DEBUG_LOG("%p TransactionLogStore::load File \"%s\" age=%lldms, expired %lldms ago, purging\n",
						store.get(), filePath.filename().string().c_str(), fileAgeMs.count(), delta.count())
					std::filesystem::remove(filePath);
					continue;
				} else {
					DEBUG_LOG("%p TransactionLogStore::load File \"%s\" age=%lldms, not expired, %lldms left\n",
						store.get(), filePath.filename().string().c_str(), fileAgeMs.count(), delta.count() * -1)
				}
			}

			store->registerLogFile(filePath, sequenceNumber);
		}
	}

	return store;
}

} // namespace rocksdb_js
