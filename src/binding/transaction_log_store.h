#ifndef __TRANSACTION_LOG_STORE_H__
#define __TRANSACTION_LOG_STORE_H__

#include <string>
#include <filesystem>
#include <map>
#include <mutex>
#include <atomic>
#include <functional>
#include "transaction_log_file.h"

namespace rocksdb_js {

struct TransactionLogStore final {
	/**
	 * The name of the transaction log store.
	 */
	std::string name;

	/**
	 * The directory containing the transaction store's sequence log files.
	 */
	std::filesystem::path path;

	/**
	 * The maximum size of a transaction log file in bytes.
	 */
	uint32_t maxSize;

	/**
	 * The retention period for transaction logs in milliseconds.
	 */
	std::chrono::milliseconds retentionMs;

	/**
	 * The current sequence number of the transaction log file.
	 */
	uint32_t currentSequenceNumber;

	/**
	 * The next sequence number to use for the transaction log file.
	 */
	uint32_t nextSequenceNumber;

	/**
	 * The map of sequence numbers to transaction log files.
	 */
	std::map<uint32_t, std::unique_ptr<TransactionLogFile>> sequenceFiles;

	/**
	 * The mutex to protect the transaction log store.
	 */
	std::mutex storeMutex;

	/**
	 * The flag indicating if the transaction log store is closing. Once a store
	 * is closed, it cannot be reopened.
	 *
	 * This is used to prevent concurrent closes and to ensure that no new
	 * operations are added to the store after it is closed.
	 */
	std::atomic<bool> isClosing;

	TransactionLogStore(
		const std::string& name,
		const std::filesystem::path& path,
		const uint32_t maxSize,
		const std::chrono::milliseconds& retentionMs
	);

	~TransactionLogStore();

	void addEntry(const uint64_t timestamp, const char* data, const size_t size);
	void close();
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

private:
	TransactionLogFile* getLogFile(const uint32_t sequenceNumber);
};

} // namespace rocksdb_js

#endif
