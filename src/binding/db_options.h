#ifndef __DB_OPTIONS_H__
#define __DB_OPTIONS_H__

#include <string>

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
	bool disableWAL;
	DBMode mode;
	std::string name;
	bool noBlockCache;
	uint32_t parallelismThreads;
	uint32_t transactionLogMaxSize;
	uint32_t transactionLogRetentionMs;
	float transactionLogMaxAgeThreshold;
	std::string transactionLogsPath;
};

} // namespace rocksdb_js

#endif
