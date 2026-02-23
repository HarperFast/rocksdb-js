#ifndef __DB_OPTIONS_H__
#define __DB_OPTIONS_H__

#include <string>
#include <thread>

namespace rocksdb_js {

/**
 * The RocksDB database mode.
 */
enum class DBMode {
	Optimistic,
	Pessimistic,
};

/**
 * Options for opening a RocksDB database. It holds the processed napi argument
 * values passed in from public `open()` method.
 */
struct DBOptions final {
	bool disableWAL = false;
	bool enableStats = false;
	DBMode mode = DBMode::Optimistic;
	std::string name;
	bool noBlockCache = false;
	uint32_t parallelismThreads = std::max<uint32_t>(1, std::thread::hardware_concurrency() / 2);
	uint8_t statsLevel = rocksdb::StatsLevel::kExceptDetailedTimers;
	float transactionLogMaxAgeThreshold = 0.75f;
	uint32_t transactionLogMaxSize = 16 * 1024 * 1024; // 16MB
	uint32_t transactionLogRetentionMs = 3 * 24 * 60 * 60 * 1000; // 3 days
	std::string transactionLogsPath;
};

} // namespace rocksdb_js

#endif
