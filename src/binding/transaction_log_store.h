#ifndef __TRANSACTION_LOG_STORE_H__
#define __TRANSACTION_LOG_STORE_H__

#include <string>
#include <filesystem>
#include <map>
#include <vector>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "transaction_log_file.h"

namespace rocksdb_js {

struct TransactionLogStore final {
	std::string name;
    std::filesystem::path logsDirectory;
    uint32_t maxSize;
    uint32_t currentSequenceNumber;
    uint32_t nextSequenceNumber;
    std::map<uint32_t, std::unique_ptr<TransactionLogFile>> sequenceFiles;

    TransactionLogStore(
        const std::string& name,
        const std::filesystem::path& logsDirectory,
        const uint32_t maxSize
    );

    void addEntry(uint64_t timestamp, char* logEntry, size_t logEntryLength);
    void registerLogFile(uint32_t sequenceNumber, const std::filesystem::path& path);
    TransactionLogFile* getLogFile(uint32_t sequenceNumber);

    static std::shared_ptr<TransactionLogStore> load(const std::filesystem::path& path, const uint32_t maxSize);
};

} // namespace rocksdb_js

#endif