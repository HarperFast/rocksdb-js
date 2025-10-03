#ifndef __TRANSACTION_LOG_STORE_H__
#define __TRANSACTION_LOG_STORE_H__

#include <string>
#include <filesystem>
#include <map>
#include <vector>

namespace rocksdb_js {

struct SequenceFile final {
    std::filesystem::path path;
    uint32_t sequenceNumber;
    FILE* file;
    bool isOpen;

    SequenceFile(const std::filesystem::path& p, uint32_t seq)
        : path(p), sequenceNumber(seq), file(nullptr), isOpen(false) {}
};

struct TransactionLogStore final {
	std::string name;
    std::filesystem::path logsDirectory;
    uint32_t maxSize;
    uint32_t nextSequenceNumber;
    std::map<uint32_t, std::unique_ptr<SequenceFile>> sequenceFiles;

    TransactionLogStore(
        const std::string& name,
        const std::filesystem::path& logsDirectory,
        const uint32_t maxSize
    );

    void addSequenceFile(uint32_t sequenceNumber, const std::filesystem::path& path);

    // void discoverSequenceFiles();
    // void addSequenceFile(uint32_t sequenceNumber, const std::filesystem::path& path);
    // void openSequenceFile(uint32_t sequenceNumber);
    // void closeSequenceFile(uint32_t sequenceNumber);
    // void closeAllSequenceFiles();

    // // Get the current active sequence file (highest sequence number)
    // SequenceFile* getCurrentSequenceFile();

    // // Get a specific sequence file
    // SequenceFile* getSequenceFile(uint32_t sequenceNumber);

    // // Get all sequence numbers in order
    // std::vector<uint32_t> getSequenceNumbers() const;

    // void open();
    // void close();
};

} // namespace rocksdb_js

#endif