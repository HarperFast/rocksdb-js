#include "transaction_log_store.h"
#include "macros.h"
#include "util.h"

namespace rocksdb_js {

TransactionLogStore::TransactionLogStore(
	const std::string& name,
	const std::filesystem::path& logsDirectory,
	const uint32_t maxSize
) :
	name(name),
	logsDirectory(logsDirectory),
	maxSize(maxSize),
	currentSequenceNumber(1),
	nextSequenceNumber(2)
{}

void TransactionLogStore::addEntry(uint64_t timestamp, char* logEntry, size_t logEntryLength) {
	DEBUG_LOG("%p TransactionLogStore::addEntry Adding entry to store: %s (seq=%u)\n",
			 this, this->name.c_str(), this->currentSequenceNumber)

	// TODO:
	// 1. Determine if we a store for the current sequence number exists
	// 2. If not, create a new store
	// 3. If the store is full, rotate to the next sequence number
	// 4. Add the entry to the store
}

void TransactionLogStore::registerLogFile(uint32_t sequenceNumber, const std::filesystem::path& path) {
	auto sequenceFile = std::make_unique<TransactionLogFile>(path, sequenceNumber);
	this->sequenceFiles[sequenceNumber] = std::move(sequenceFile);

	if (sequenceNumber > this->currentSequenceNumber) {
		this->currentSequenceNumber = sequenceNumber;
	}

	// update next sequence number to be one higher than the highest existing
	if (sequenceNumber > this->nextSequenceNumber) {
		this->nextSequenceNumber = sequenceNumber + 1;
	}

	DEBUG_LOG("%p TransactionLogStore::registerSequenceFile Added sequence file: %s (seq=%u)\n",
		this, path.string().c_str(), sequenceNumber)
}

TransactionLogFile* TransactionLogStore::getLogFile(uint32_t sequenceNumber) {
	auto it = this->sequenceFiles.find(sequenceNumber);
	return (it != this->sequenceFiles.end()) ? it->second.get() : nullptr;
}

std::shared_ptr<TransactionLogStore> TransactionLogStore::load(const std::filesystem::path& path, const uint32_t maxSize) {
	auto dirName = path.filename().string();

	// skip directories that start with "."
	if (dirName.empty() || dirName[0] == '.') {
		return nullptr;
	}

	std::shared_ptr<TransactionLogStore> store = std::make_shared<TransactionLogStore>(dirName, path, maxSize);

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

			try {
				std::string sequenceNumberStr = filename.substr(secondLastDot + 1, lastDot - secondLastDot - 1);
				uint32_t sequenceNumber = std::stoul(sequenceNumberStr);
				store->registerLogFile(sequenceNumber, filePath);
			} catch (const std::exception& e) {
				DEBUG_LOG(
					"DBDescriptor::discoverTransactionLogStores Invalid sequence number in file: %s\n",
					filename.c_str()
				)
			}
		}
	}

	return store;
}

// SequenceFile* TransactionLogHandle::getCurrentSequenceFile() {
//     if (this->sequenceFiles.empty()) {
//         return nullptr;
//     }

//     // Return the sequence file with the highest sequence number
//     auto it = this->sequenceFiles.rbegin();
//     return it->second.get();
// }

/*
void TransactionLogHandle::openSequenceFile(uint32_t sequenceNumber) {
	auto* sequenceFile = getSequenceFile(sequenceNumber);
	if (!sequenceFile || sequenceFile->isOpen) {
		return;
	}

	DEBUG_LOG("%p TransactionLogHandle::openSequenceFile Opening %s\n", this, sequenceFile->path.string().c_str())
	sequenceFile->file = std::fopen(sequenceFile->path.string().c_str(), "ab"); // append mode for existing files
	if (!sequenceFile->file) {
		throw std::runtime_error("Failed to open transaction log sequence file: " + sequenceFile->path.string());
	}

	sequenceFile->isOpen = true;
}

void TransactionLogHandle::closeSequenceFile(uint32_t sequenceNumber) {
	auto* sequenceFile = getSequenceFile(sequenceNumber);
	if (!sequenceFile || !sequenceFile->isOpen) {
		return;
	}

	DEBUG_LOG("%p TransactionLogHandle::closeSequenceFile Closing %s\n", this, sequenceFile->path.string().c_str())
	std::fclose(sequenceFile->file);
	sequenceFile->file = nullptr;
	sequenceFile->isOpen = false;
}

void TransactionLogHandle::closeAllSequenceFiles() {
	for (const auto& [sequenceNumber, sequenceFile] : this->sequenceFiles) {
		if (sequenceFile->isOpen) {
			closeSequenceFile(sequenceNumber);
		}
	}
}

void TransactionLogHandle::open() {
	// For now, just ensure we have discovered all sequence files
	// Individual sequence files will be opened on-demand
	DEBUG_LOG("%p TransactionLogHandle::open Opening transaction log: %s\n", this, this->name.c_str())
}

void TransactionLogHandle::close() {
	closeAllSequenceFiles();
	DEBUG_LOG("%p TransactionLogHandle::close Closed all sequence files for: %s\n", this, this->name.c_str())
}

*/

// void TransactionLogStore::mapSequenceFileForReading(uint32_t sequenceNumber) {
//     auto* sequenceFile = getSequenceFile(sequenceNumber);
//     if (!sequenceFile || sequenceFile->isMapped) {
//         return;
//     }

//     // Open file for reading
//     sequenceFile->fd = open(sequenceFile->path.c_str(), O_RDONLY);
//     if (sequenceFile->fd < 0) {
//         throw std::runtime_error("Failed to open sequence file for reading: " + sequenceFile->path.string());
//     }

//     // Get file size
//     struct stat st;
//     if (fstat(sequenceFile->fd, &st) < 0) {
//         ::close(sequenceFile->fd);
//         sequenceFile->fd = -1;
//         throw std::runtime_error("Failed to get file size: " + sequenceFile->path.string());
//     }

//     sequenceFile->mappedSize = st.st_size;

//     if (sequenceFile->mappedSize > 0) {
//         // Map file into memory
//         sequenceFile->mappedData = mmap(nullptr, sequenceFile->mappedSize,
//                                       PROT_READ, MAP_SHARED, sequenceFile->fd, 0);

//         if (sequenceFile->mappedData == MAP_FAILED) {
//             ::close(sequenceFile->fd);
//             sequenceFile->fd = -1;
//             throw std::runtime_error("Failed to mmap sequence file: " + sequenceFile->path.string());
//         }

//         // Advise OS about access pattern
//         madvise(sequenceFile->mappedData, sequenceFile->mappedSize, MADV_SEQUENTIAL);
//     }

//     sequenceFile->isMapped = true;
//     DEBUG_LOG("%p TransactionLogStore::mapSequenceFileForReading Mapped %s (%zu bytes)\n",
//              this, sequenceFile->path.string().c_str(), sequenceFile->mappedSize);
// }

// void TransactionLogStore::openCurrentFileForWriting() {
//     auto* currentFile = getCurrentSequenceFile();
//     if (!currentFile || currentFile->isOpenForWrite) {
//         return;
//     }

//     currentFile->writeFile = std::fopen(currentFile->path.c_str(), "ab");
//     if (!currentFile->writeFile) {
//         throw std::runtime_error("Failed to open current sequence file for writing: " +
//                                currentFile->path.string());
//     }

//     currentFile->isOpenForWrite = true;
//     DEBUG_LOG("%p TransactionLogStore::openCurrentFileForWriting Opened %s for writing\n",
//              this, currentFile->path.string().c_str());
// }

} // namespace rocksdb_js
