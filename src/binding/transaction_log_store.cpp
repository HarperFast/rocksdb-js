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
{
	//
}

void TransactionLogStore::addEntry(uint64_t timestamp, char* logEntry, size_t logEntryLength) {
	// TODO:
    // 1. Determine if we a store for the current sequence number exists
    // 2. If not, create a new store
    // 3. If the store is full, rotate to the next sequence number
    // 4. Add the entry to the store
}

void TransactionLogStore::addSequenceFile(uint32_t sequenceNumber, const std::filesystem::path& path) {
    auto sequenceFile = std::make_unique<SequenceFile>(path, sequenceNumber);
    this->sequenceFiles[sequenceNumber] = std::move(sequenceFile);

    if (sequenceNumber > this->currentSequenceNumber) {
        this->currentSequenceNumber = sequenceNumber;
    }

    // Update next sequence number to be one higher than the highest existing
    if (sequenceNumber > this->nextSequenceNumber) {
        this->nextSequenceNumber = sequenceNumber + 1;
    }

    DEBUG_LOG("%p TransactionLogStore::addSequenceFile Added sequence file: %s (seq=%u)\n",
             this, path.string().c_str(), sequenceNumber)
}

/*
void TransactionLogHandle::discoverSequenceFiles() {
    for (const auto& entry : std::filesystem::directory_iterator(this->logsDirectory)) {
        auto path = entry.path();

        if (entry.is_regular_file() && path.extension() == ".txnlog") {
            std::string filename = path.filename().string();

            // Check if this file belongs to our log name
            std::string expectedPrefix = this->name + ".";
            if (filename.substr(0, expectedPrefix.length()) != expectedPrefix) {
                continue;
            }

            // Parse sequence number
            size_t lastDot = filename.find_last_of('.');
            size_t secondLastDot = filename.find_last_of('.', lastDot - 1);

            if (secondLastDot != std::string::npos) {
                std::string sequenceNumberStr = filename.substr(secondLastDot + 1, lastDot - secondLastDot - 1);
                try {
                    uint32_t sequenceNumber = std::stoul(sequenceNumberStr);
                    addSequenceFile(sequenceNumber, path);
                } catch (const std::exception& e) {
                    DEBUG_LOG("%p TransactionLogHandle::discoverSequenceFiles Invalid sequence number in file: %s\n",
                             this, filename.c_str())
                }
            }
        }
    }
}

SequenceFile* TransactionLogHandle::getCurrentSequenceFile() {
    if (this->sequenceFiles.empty()) {
        return nullptr;
    }

    // Return the sequence file with the highest sequence number
    auto it = this->sequenceFiles.rbegin();
    return it->second.get();
}

SequenceFile* TransactionLogHandle::getSequenceFile(uint32_t sequenceNumber) {
    auto it = this->sequenceFiles.find(sequenceNumber);
    return (it != this->sequenceFiles.end()) ? it->second.get() : nullptr;
}

std::vector<uint32_t> TransactionLogHandle::getSequenceNumbers() const {
    std::vector<uint32_t> sequences;
    sequences.reserve(this->sequenceFiles.size());

    for (const auto& [sequenceNumber, _] : this->sequenceFiles) {
        sequences.push_back(sequenceNumber);
    }

    return sequences;
}

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

} // namespace rocksdb_js
