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
	uint32_t blockCacheSize;
	DBMode mode;
	std::string name;
	int parallelismThreads;
};

} // namespace rocksdb_js

#endif
