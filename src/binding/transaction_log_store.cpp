#include <chrono>
#include <vector>
#include "macros.h"
#include "transaction_log_store.h"
#include "util.h"

namespace rocksdb_js {

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
	for (int i = 0; i < RECENTLY_COMMITTED_POSITIONS_SIZE; i++) { // initialize recent commits to not match until values are entered
		recentlyCommittedSequencePositions[i].position = { { 0, 0 } };
		recentlyCommittedSequencePositions[i].rocksSequenceNumber = 0x7FFFFFFFFFFFFFFF; // maximum int64, won't match any commit
	}
}

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

	std::unique_lock<std::mutex> lock(this->writeMutex);
	std::unique_lock<std::mutex> dataSetsLock(this->dataSetsMutex);
	DEBUG_LOG("%p TransactionLogStore::close Closing transaction log store \"%s\"\n", this, this->name.c_str())
	for (const auto& [sequenceNumber, logFile] : this->sequenceFiles) {
		DEBUG_LOG("%p TransactionLogStore::close Closing log file \"%s\"\n", this, logFile->path.string().c_str())
		logFile->close();
	}

	lock.unlock();
	dataSetsLock.unlock();
	this->purge();
}

std::shared_ptr<TransactionLogFile> TransactionLogStore::getLogFile(const uint32_t sequenceNumber) {
	std::unique_lock<std::mutex> lock(this->dataSetsMutex);
	auto it = this->sequenceFiles.find(sequenceNumber);
	std::shared_ptr<TransactionLogFile> logFile = it != this->sequenceFiles.end() ? it->second : nullptr;

	if (!logFile) {
		DEBUG_LOG("%p TransactionLogStore::getLogFile Store path \"%s\" (seq=%u) no log file found, creating\n",
			this, this->path.string().c_str(), sequenceNumber)

		// ensure the directory exists before creating the file (should already exist)
		std::filesystem::create_directories(this->path);

		std::string filename = std::to_string(sequenceNumber) + ".txnlog";
		auto logFilePath = this->path / filename;
		logFile = std::make_shared<TransactionLogFile>(logFilePath, sequenceNumber);
		this->sequenceFiles[sequenceNumber] = logFile;
		this->nextLogPosition = { { 0, sequenceNumber } };
		if (this->uncommittedTransactionPositions.empty()) {
			// initialize with the first position in the log file
			this->uncommittedTransactionPositions.insert(this->nextLogPosition);
		}
	}

	return logFile;
}

std::weak_ptr<MemoryMap> TransactionLogStore::getMemoryMap(uint32_t logSequenceNumber) {
	std::lock_guard<std::mutex> lock(this->dataSetsMutex);
	auto it = this->sequenceFiles.find(logSequenceNumber);
	auto logFile = it != this->sequenceFiles.end() ? it->second.get() : nullptr;
	if (!logFile) {
		return std::weak_ptr<MemoryMap>(); // nullptr
	}
	logFile->open(this->latestTimestamp);
	return logFile->getMemoryMap(this->currentSequenceNumber == logSequenceNumber ?
		maxFileSize : // if it is the most current log, it will be growing so we need to allocate the max size
		logFile->size); // otherwise it is frozen, use the file size
}

uint64_t TransactionLogStore::getLogFileSize(uint32_t logSequenceNumber) {
	std::lock_guard<std::mutex> lock(this->dataSetsMutex);
	if (logSequenceNumber == 0) {
		// get the total size of all log files
		uint64_t size = 0;
		for (auto& [key, value] : this->sequenceFiles) {
			value->open(this->latestTimestamp);
			size += value->size;
		}
		return size;
	} else
	{
		auto it = this->sequenceFiles.find(logSequenceNumber);
		auto logFile = it != this->sequenceFiles.end() ? it->second.get() : nullptr;
		if (!logFile) {
			return 0;
		}
		logFile->open(this->latestTimestamp);
		return logFile->size;
	}
}

std::weak_ptr<LogPosition> TransactionLogStore::getLastCommittedPosition() {
	return this->lastCommittedPosition;
}

LogPosition TransactionLogStore::findPositionByTimestamp(double timestamp) {
	std::lock_guard<std::mutex> lock(this->dataSetsMutex);
	uint32_t sequenceNumber = this->currentSequenceNumber;
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
		positionInLogFile = logFile->findPositionByTimestamp(timestamp, isCurrent ? maxFileSize : logFile->size);
		// a position of zero means that the timestamp is before the log file header's timestamp, greater than that,
		// we are in the correct log file to start searching
		if (positionInLogFile > 0) {
			if (positionInLogFile == 0xFFFFFFFF && sequenceNumber < this->currentSequenceNumber) {
				// beyond the end of this log file, revert to next one (because it exists)
				break;
			}
			// found a valid position in the log file
			return { { positionInLogFile, sequenceNumber } };
		}
		isCurrent = false;
		it = this->sequenceFiles.find(--sequenceNumber);
	};
	// we iterated too far, return to the beginning position in the current log file
	return { { TRANSACTION_LOG_FILE_HEADER_SIZE, sequenceNumber + 1 } };
}

void TransactionLogStore::purge(std::function<void(const std::filesystem::path&)> visitor, const bool all) {
	std::lock_guard<std::mutex> lock(this->writeMutex);
	std::lock_guard<std::mutex> dataSetsLock(this->dataSetsMutex);

	if (this->sequenceFiles.empty()) {
		return;
	}

	DEBUG_LOG("%p TransactionLogStore::purge Purging transaction log store \"%s\" (# files=%u)\n", this, this->name.c_str(), this->sequenceFiles.size())

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
		auto removed = logFile->removeFile();
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
	std::lock_guard<std::mutex> lock(this->dataSetsMutex);

	auto logFile = std::make_shared<TransactionLogFile>(path, sequenceNumber);
	this->sequenceFiles[sequenceNumber] = logFile;

	if (sequenceNumber >= this->currentSequenceNumber) {
		logFile->open(this->latestTimestamp);
		this->currentSequenceNumber = sequenceNumber;
		nextLogPosition = { { logFile->size, sequenceNumber } };
	}

	// update next sequence number to be one higher than the highest existing
	if (sequenceNumber > this->nextSequenceNumber) {
		this->nextSequenceNumber = sequenceNumber + 1;
	}

	DEBUG_LOG("%p TransactionLogStore::registerLogFile Added log file: %s (seq=%u)\n",
		this, path.string().c_str(), sequenceNumber)
}

LogPosition TransactionLogStore::writeBatch(TransactionLogEntryBatch& batch) {
	DEBUG_LOG("%p TransactionLogStore::commit Adding batch with %zu entries to store \"%s\" (timestamp=%llu)\n",
		this, batch.entries.size(), this->name.c_str(), batch.timestamp)

	std::lock_guard<std::mutex> lock(this->writeMutex);

	LogPosition logPosition = this->nextLogPosition;

	if (batch.timestamp > this->latestTimestamp) {
		DEBUG_LOG("%p TransactionLogStore::commit Setting latest timestamp to batch timestamp: %f > %f\n", this, batch.timestamp, this->latestTimestamp)
		this->latestTimestamp = batch.timestamp;
	}

	// write entries across multiple log files until all are written
	while (!batch.isComplete()) {
		std::shared_ptr<TransactionLogFile> logFile = nullptr;

		// get the current log file and rotate if needed
		while (logFile == nullptr && this->currentSequenceNumber) {
			logFile = this->getLogFile(this->currentSequenceNumber);

			// we found a log file, check if it's already at max size
			if (this->maxFileSize == 0 || logFile->size < this->maxFileSize) {
				try {
					logFile->open(this->latestTimestamp);
					break;
				} catch (const std::exception& e) {
					DEBUG_LOG("%p TransactionLogStore::commit Failed to open transaction log file: %s\n", this, e.what())
					// move to next sequence number and try again
					logFile = nullptr;
				}
			}

			// rotate to next sequence if file open failed or file is at max size
			// this prevents infinite loops when file open fails (even with maxIndexSize=0)
			if (logFile == nullptr || this->maxFileSize > 0) {
				DEBUG_LOG("%p TransactionLogStore::commit Rotating to next sequence for store \"%s\" (logFile=%p, maxIndexSize=%u)\n",
					this, this->name.c_str(), static_cast<void*>(logFile), this->maxFileSize)
				this->currentSequenceNumber = this->nextSequenceNumber++;
				logFile = nullptr;
			}
		}

		if (!logPosition.fullPosition) {
			// if this wasn't initialized, we do so now
			logPosition = this->nextLogPosition;
		}

		// ensure we have a valid log file before writing
		if (!logFile) {
			DEBUG_LOG("%p TransactionLogStore::commit ERROR: Failed to open transaction log file for store \"%s\"\n", this, this->name.c_str())
			throw std::runtime_error("Failed to open transaction log file for store \"" + this->name + "\"");
		}

		// if the file is older than the retention threshold, rotate to the next file
		DEBUG_LOG("%p TransactionLogStore::commit Checking if log file is older than threshold (%f) for store \"%s\"\n",
			this, this->maxAgeThreshold, this->name.c_str())
		if (this->maxAgeThreshold > 0) {
			try {
				auto thresholdDuration = std::chrono::duration_cast<std::chrono::milliseconds>(
					this->retentionMs * (1 - this->maxAgeThreshold)
				);
				auto lastWriteTime = logFile->getLastWriteTime();
				auto now = std::chrono::system_clock::now();
				auto fileAgeMs = std::chrono::duration_cast<std::chrono::milliseconds>(now - lastWriteTime);
				DEBUG_LOG("%p TransactionLogStore::commit Max age threshold:        %f\n",
					this, this->maxAgeThreshold)
				DEBUG_LOG("%p TransactionLogStore::commit Retention duration:       %lld ms\n",
					this, this->retentionMs.count())
				DEBUG_LOG("%p TransactionLogStore::commit Threshold duration:       %lld ms\n",
					this, thresholdDuration.count())
				DEBUG_LOG("%p TransactionLogStore::commit Log file last write time: %lld ms\n",
					this, std::chrono::duration_cast<std::chrono::milliseconds>(lastWriteTime.time_since_epoch()).count())
				DEBUG_LOG("%p TransactionLogStore::commit Now:                      %lld ms\n",
					this, std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count())
				DEBUG_LOG("%p TransactionLogStore::commit File age:                 %lld ms\n",
					this, fileAgeMs.count())
				if (fileAgeMs >= thresholdDuration) {
					DEBUG_LOG("%p TransactionLogStore::commit Log file is older than threshold (%lld ms >= %lld ms), rotating to next file for store \"%s\"\n",
						this, fileAgeMs.count(), thresholdDuration.count(), this->name.c_str())
					this->currentSequenceNumber = this->nextSequenceNumber++;
					continue;
				}
			} catch (const std::filesystem::filesystem_error& e) {
				// file doesn't exist
			}
		}

		uint32_t sizeBefore = logFile->size;

		DEBUG_LOG("%p TransactionLogStore::commit Writing to log file for store \"%s\" (seq=%u, size=%u, maxIndexSize=%u)\n",
			this, this->name.c_str(), logFile->sequenceNumber, logFile->size, this->maxFileSize)

		// write as much as possible to this file
		logFile->writeEntries(batch, this->maxFileSize);

		DEBUG_LOG("%p TransactionLogStore::commit Wrote to log file for store \"%s\" (seq=%u, new size=%u)\n",
			this, this->name.c_str(), logFile->sequenceNumber, logFile->size)

		// if no progress was made, rotate to the next file to avoid infinite loop
		if (logFile->size == sizeBefore) {
			DEBUG_LOG("%p TransactionLogStore::commit No progress made (size unchanged), rotating to next file for store \"%s\"\n", this, this->name.c_str())
			this->currentSequenceNumber = this->nextSequenceNumber++;
		} else if (this->maxFileSize > 0 && logFile->size >= this->maxFileSize) {
			// we've reached or exceeded the max size, rotate to the next file
			DEBUG_LOG("%p TransactionLogStore::commit Log file reached max size, rotating to next file for store \"%s\"\n", this, this->name.c_str())
			this->currentSequenceNumber = this->nextSequenceNumber++;
		} else if (!batch.isComplete()) {
			// we've written some entries, but the batch is not complete, rotate to the next file
			DEBUG_LOG("%p TransactionLogStore::commit Batch is not complete, rotating to next file for store \"%s\"\n", this, this->name.c_str())
			this->currentSequenceNumber = this->nextSequenceNumber++;
		}
		this->nextLogPosition = { { logFile->size, this->currentSequenceNumber } };
	}
	std::lock_guard<std::mutex> dataSetsLock(this->dataSetsMutex);
	uncommittedTransactionPositions.insert(this->nextLogPosition);

	DEBUG_LOG("%p TransactionLogStore::commit Completed writing all entries\n", this)
	return logPosition;
}

void TransactionLogStore::commitFinished(const LogPosition position, rocksdb::SequenceNumber rocksSequenceNumber) {
	std::lock_guard<std::mutex> lock(this->dataSetsMutex);
	// This written transaction entry is no longer uncommitted, so we can remove it
	uncommittedTransactionPositions.erase(position);
	// we now find the beginning of the earliest uncommitted transaction to mark the end of continuously fully committed transactions
	LogPosition fullyCommittedPosition = *(uncommittedTransactionPositions.begin());
	// update the current position handle with latest fully committed position
	*this->lastCommittedPosition = fullyCommittedPosition;
	// now setup a sequence position that matches a rocksdb sequence number to our log position
	SequencePosition sequencePosition;
	sequencePosition.position = fullyCommittedPosition;
	sequencePosition.rocksSequenceNumber = rocksSequenceNumber;
	// Now we record this in our array of sequence number + position combinations. However, we don't want to keep a huge
	// array so we keep an array where each n position represents an n^2 frequencies of correlations. We are not keeping
	// an exact map of every pairing, and we don't need to. We don't need to know the exact rocks sequence number, we just
	// need a sequence number that is not greater than the point of the flush. But we want to record enough that we
	// won't lose more than half of what has to be replayed since the last flush.
	unsigned int count = nextSequencePositionsCount++;
	int index = 0;
	// iterate through the array breaking once at the first set bit, but don't iterate past the end of the array (hence -1)
	for (; index < RECENTLY_COMMITTED_POSITIONS_SIZE - 1; index++) {
	    if ((count >> index) & 1) break; // will break 50% of the time at each iteration
	}
	// record in the array
	recentlyCommittedSequencePositions[index] = sequencePosition;
}

bool operator>( const LogPosition a, const LogPosition b ) {
	// as noted in the header, 64-bit comparison on little-endian machines seems like it would be an optimization
	return a.logSequenceNumber == b.logSequenceNumber ?
		a.positionInLogFile > b.positionInLogFile :
		a.logSequenceNumber > b.logSequenceNumber;
};

void TransactionLogStore::databaseFlushed(rocksdb::SequenceNumber rocksSequenceNumber) {
	std::lock_guard<std::mutex> lock(this->dataSetsMutex);
	LogPosition latestFlushedPosition = { { 0, 0 } };
	// the latest sequence number that has been flushed according to this flush update
	for (int i = 0; i < RECENTLY_COMMITTED_POSITIONS_SIZE; i++) {
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
		flushedTrackerFile->open(this->latestTimestamp);
	}
	// save the position of fully flushed transaction logs (future replay will start from here)
	flushedTrackerFile->writeToFile(&latestFlushedPosition, 8, 0);
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

	// find `.txnlog` files in the directory
	for (const auto& fileEntry : std::filesystem::directory_iterator(path)) {
		if (fileEntry.is_regular_file() && fileEntry.path().extension() == ".txnlog") {
			auto filePath = fileEntry.path();
			auto filename = filePath.filename().string();

			std::string sequenceNumberStr = filename.substr(0, filename.size() - 7);
			uint32_t sequenceNumber = 0;

			try {
				sequenceNumber = std::stoul(sequenceNumberStr);

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
			} catch (const std::filesystem::filesystem_error& e) {
				DEBUG_LOG("%p TransactionLogStore::load Failed to get last write time for file %s: %s\n",
					store.get(), filePath.string().c_str(), e.what())
			} catch (const std::exception& e) {
				DEBUG_LOG("%p TransactionLogStore::load Failed to load file %s: %s\n",
					store.get(), filePath.string().c_str(), e.what())
			} catch (...) {
				DEBUG_LOG(
					"DBDescriptor::discoverTransactionLogStores Invalid sequence number in file: %s\n",
					filename.c_str()
				)
			}
		}
	}
	store->uncommittedTransactionPositions.insert(store->nextLogPosition);

	return store;
}

} // namespace rocksdb_js
