#ifndef __TRANSACTION_LOG_STORE_H__
#define __TRANSACTION_LOG_STORE_H__

#include <string>
#include <filesystem>
#include <map>
#include <functional>
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
    void close();
    TransactionLogFile* getLogFile(uint32_t sequenceNumber);
    TransactionLogFile* openLogFile(uint32_t sequenceNumber);
    void query();
    void purge(std::function<void(const std::filesystem::path&)> visitor, const bool all = false, const std::chrono::milliseconds& retentionMs = std::chrono::milliseconds(0));
    void registerLogFile(const std::filesystem::path& path, const uint32_t sequenceNumber);

    static std::shared_ptr<TransactionLogStore> load(const std::filesystem::path& path, const uint32_t maxSize);
};

} // namespace rocksdb_js

#endif
