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
	std::chrono::milliseconds retentionMs;
	uint32_t currentSequenceNumber;
	uint32_t nextSequenceNumber;
	std::map<uint32_t, std::unique_ptr<TransactionLogFile>> sequenceFiles;

	TransactionLogStore(
		const std::string& name,
		const std::filesystem::path& logsDirectory,
		const uint32_t maxSize,
		const std::chrono::milliseconds& retentionMs
	);

	void addEntry(const uint64_t timestamp, const char* data, const size_t size);
	void close();
	TransactionLogFile* getLogFile(const uint32_t sequenceNumber);
	TransactionLogFile* openLogFile(const uint32_t sequenceNumber);
	void query();
	void purge(
		std::function<void(const std::filesystem::path&)> visitor = nullptr,
		const bool all = false
	);
	void registerLogFile(const std::filesystem::path& path, const uint32_t sequenceNumber);

	static std::shared_ptr<TransactionLogStore> load(
		const std::filesystem::path& path,
		const uint32_t maxSize,
		const std::chrono::milliseconds& retentionMs
	);
};

} // namespace rocksdb_js

#endif
