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
{
	DEBUG_LOG("%p TransactionLogStore::TransactionLogStore Opening transaction log store \"%s\"\n", this, this->name.c_str());
	positionHandle = new PositionHandle();
	for (int i = 0; i < 20; i++) { // initialize recent commits to not match until values are entered
		recentlyCommittedSequencePositions[i].position = 0;
		recentlyCommittedSequencePositions[i].rocksSequenceNumber = 0x7FFFFFFFFFFFFFFF; // maximum int64, won't match commit
	}
}

TransactionLogStore::~TransactionLogStore() {
	DEBUG_LOG("%p TransactionLogStore::~TransactionLogStore Closing transaction log store \"%s\"\n", this, this->name.c_str())
	if (--positionHandle->refCount == 0) {
		DEBUG_LOG("%p TransactionLogStore::~TransactionLogStore delete positionHandle\n", this);
		delete positionHandle;
	}
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
		this->nextSequencePosition = ((uint64_t) sequenceNumber << 32);
		if (this->uncommittedTransactionPositions.empty()) {
			this->uncommittedTransactionPositions.insert(this->nextSequencePosition);
		}
	}

	return logFile;
}

MemoryMap* TransactionLogStore::getMemoryMap(uint32_t logSequenceNumber) {
	std::lock_guard<std::mutex> lock(this->storeMutex);
	auto it = this->sequenceFiles.find(logSequenceNumber);
	auto logFile = it != this->sequenceFiles.end() ? it->second.get() : nullptr;
	if (!logFile) {
		return nullptr;
	}
	logFile->open();
	return logFile->getMemoryMap(maxSize);
}

uint32_t TransactionLogStore::getLogFileSize(uint32_t logSequenceNumber) {
	std::lock_guard<std::mutex> lock(this->storeMutex);
	auto it = this->sequenceFiles.find(logSequenceNumber);
	auto logFile = it != this->sequenceFiles.end() ? it->second.get() : nullptr;
	if (!logFile) {
		return 0;
	}
	logFile->open();
	return logFile->size;
}

PositionHandle* TransactionLogStore::getLastCommittedPosition() {
	std::lock_guard<std::mutex> lock(this->storeMutex);
	return this->positionHandle;
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
		bool removed = std::filesystem::remove(logFile->path);
		if (visitor && removed) {
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
	// only try to remove if we actually removed at least one file from this store
	if (this->sequenceFiles.empty() && !sequenceNumbersToRemove.empty()) {
		try {
			if (std::filesystem::exists(this->path)) {
				std::filesystem::remove(this->path);
				DEBUG_LOG("%p TransactionLogStore::purge Removed empty log directory: %s\n", this, this->path.string().c_str())
			}
		} catch (const std::filesystem::filesystem_error& e) {
			DEBUG_LOG("%p TransactionLogStore::purge Failed to remove log directory %s: %s\n", this, this->path.string().c_str(), e.what())
		} catch (...) {
			DEBUG_LOG("%p TransactionLogStore::purge Unknown error removing log directory %s\n", this, this->path.string().c_str())
		}
	}
}

void TransactionLogStore::registerLogFile(const std::filesystem::path& path, const uint32_t sequenceNumber) {
	std::lock_guard<std::mutex> lock(this->storeMutex);

	auto logFile = std::make_unique<TransactionLogFile>(path, sequenceNumber);

	if (sequenceNumber >= this->currentSequenceNumber) {
		logFile->open();
		this->currentSequenceNumber = sequenceNumber;
		nextSequencePosition = ((uint64_t) sequenceNumber << 32) | logFile->size;
	}
	this->sequenceFiles[sequenceNumber] = std::move(logFile);

	// update next sequence number to be one higher than the highest existing
	if (sequenceNumber > this->nextSequenceNumber) {
		this->nextSequenceNumber = sequenceNumber + 1;
	}

	DEBUG_LOG("%p TransactionLogStore::registerLogFile Added log file: %s (seq=%u)\n",
		this, path.string().c_str(), sequenceNumber)
}

uint64_t TransactionLogStore::writeBatch(TransactionLogEntryBatch& batch) {
	DEBUG_LOG("%p TransactionLogStore::commit Adding batch with %zu entries to store \"%s\" (timestamp=%llu)\n",
		this, batch.entries.size(), this->name.c_str(), batch.timestamp)

	std::lock_guard<std::mutex> lock(this->storeMutex);

	uint64_t sequencePosition = this->nextSequencePosition;
	// write entries across multiple log files until all are written
	while (!batch.isComplete()) {
		TransactionLogFile* logFile = nullptr;

		// get the current log file and rotate if needed
		while (logFile == nullptr && this->currentSequenceNumber) {
			logFile = this->getLogFile(this->currentSequenceNumber);

			// we found a log file, check if it's already at max size
			if (this->maxSize == 0 || logFile->size < this->maxSize) {
				try {
					logFile->open();
					break;
				} catch (const std::exception& e) {
					DEBUG_LOG("%p TransactionLogStore::commit Failed to open transaction log file: %s\n", this, e.what())
					// move to next sequence number and try again
					logFile = nullptr;
				}
			}

			// rotate to next sequence if file open failed or file is at max size
			// this prevents infinite loops when file open fails (even with maxSize=0)
			if (logFile == nullptr || this->maxSize > 0) {
				DEBUG_LOG("%p TransactionLogStore::commit Rotating to next sequence for store \"%s\" (logFile=%p, maxSize=%u)\n",
					this, this->name.c_str(), static_cast<void*>(logFile), this->maxSize)
				this->currentSequenceNumber = this->nextSequenceNumber++;
				logFile = nullptr;
			}
		}

		if (!sequencePosition) {
			// if this wasn't initialized, we do so now
			sequencePosition = this->nextSequencePosition;
		}

		// ensure we have a valid log file before writing
		if (!logFile) {
			DEBUG_LOG("%p TransactionLogStore::commit ERROR: Failed to open transaction log file for store \"%s\"\n", this, this->name.c_str())
			throw std::runtime_error("Failed to open transaction log file for store \"" + this->name + "\"");
		}

		uint32_t sizeBefore = logFile->size;

		DEBUG_LOG("%p TransactionLogStore::commit Writing to log file for store \"%s\" (seq=%u, size=%u, maxSize=%u)\n",
			this, this->name.c_str(), logFile->sequenceNumber, logFile->size, this->maxSize)

		// write as much as possible to this file
		logFile->writeEntries(batch, this->maxSize);

		DEBUG_LOG("%p TransactionLogStore::commit Wrote to log file for store \"%s\" (seq=%u, new size=%u)\n",
			this, this->name.c_str(), logFile->sequenceNumber, logFile->size)

		// if no progress was made, rotate to the next file to avoid infinite loop
		if (logFile->size == sizeBefore) {
			DEBUG_LOG("%p TransactionLogStore::commit No progress made (size unchanged), rotating to next file for store \"%s\"\n", this, this->name.c_str())
			this->currentSequenceNumber = this->nextSequenceNumber++;
		}
		// if we've reached or exceeded the max size, rotate to the next file
		else if (this->maxSize > 0 && logFile->size >= this->maxSize) {
			DEBUG_LOG("%p TransactionLogStore::commit Log file reached max size, rotating to next file for store \"%s\"\n", this, this->name.c_str())
			this->currentSequenceNumber = this->nextSequenceNumber++;
		}
		this->nextSequencePosition = ((uint64_t) this->currentSequenceNumber << 32) | logFile->size;
	}
	uncommittedTransactionPositions.insert(this->nextSequencePosition);

	DEBUG_LOG("%p TransactionLogStore::commit Completed writing all entries\n", this)
	return sequencePosition;
}

void TransactionLogStore::commitFinished(const uint64_t position, rocksdb::SequenceNumber rocksSequenceNumber) {
	// This written transaction entry is no longer uncommitted, so we can remove it
	uncommittedTransactionPositions.erase(position);
	// we now find the beginning of the earliest uncommitted transaction to mark the end of continuously fully committed transactions
	uint64_t fullyCommittedPosition = *(uncommittedTransactionPositions.begin());
	// update the current position handle with latest fully committed position
	positionHandle->position = fullyCommittedPosition;
	// now setup a sequence position that matches a rocksdb sequence number to our log position
	SequencePosition sequencePosition;
	sequencePosition.position = fullyCommittedPosition;
	sequencePosition.rocksSequenceNumber = rocksSequenceNumber;
	// Now we record this in our array of sequence number + position combinations. However, we don't want to keep a huge
	// array so we keep an array where each n position represents an n^2 frequencies of correlations. We are not keeping
	// an exact map of every pairing, but enough that we won't lose more than half of what has to be replayed since the last flush
	unsigned int count = nextSequencePositionsCount++;
	int index = 0;
	for (; index < 19; index++) {
	    if ((count >> index) & 1) break;
	}
	// record in the array (is there possibly some concern about concurrent writes causing memory tearing?)
	recentlyCommittedSequencePositions[index] = sequencePosition;
}

void TransactionLogStore::databaseFlushed(rocksdb::SequenceNumber rocksSequenceNumber) {
	uint64_t latestFlushedPosition = 0;
	// the latest sequence number that has been flushed according to this flush update
	for (int i = 0; i < 20; i++) {
		SequencePosition sequencePosition = recentlyCommittedSequencePositions[i];
		if (sequencePosition.rocksSequenceNumber <= rocksSequenceNumber && sequencePosition.position > latestFlushedPosition) {
			latestFlushedPosition = sequencePosition.position;
		}
	}
	// Open the flushed tracker file if it isn't open yet. We are using the TransactionLogFile; it is not technically
	// a "log" file, but the API provides all the functionality we need to just write a single word
	if (!flushedTrackerFile) {
		std::ostringstream oss;
		oss << this->path << ".txnstate";
		flushedTrackerFile = new TransactionLogFile(oss.str(), 0);
		flushedTrackerFile->open();
	}
	// save the position of fully flushed transaction logs (future replay will start from here)
	flushedTrackerFile->writeToFile(&latestFlushedPosition, 8, 0);
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
					try {
						std::filesystem::remove(filePath);
					} catch (const std::filesystem::filesystem_error& e) {
						DEBUG_LOG("%p TransactionLogStore::load Failed to remove expired file %s: %s\n",
							store.get(), filePath.string().c_str(), e.what())
					}
					continue;
				} else {
					DEBUG_LOG("%p TransactionLogStore::load File \"%s\" age=%lldms, not expired, %lldms left\n",
						store.get(), filePath.filename().string().c_str(), fileAgeMs.count(), delta.count() * -1)
				}
			}

			store->registerLogFile(filePath, sequenceNumber);
		}
	}
	store->uncommittedTransactionPositions.insert(store->nextSequencePosition);

	return store;
}

} // namespace rocksdb_js
