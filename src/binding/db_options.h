#ifndef __DB_OPTIONS_H__
#define __DB_OPTIONS_H__

#include <string>

namespace rocksdb_js {

enum class DBMode {
	Default,
	Optimistic,
	Pessimistic,
};

/**
 * Options for opening a RocksDB database. It holds the processed napi argument
 * values passed in from public `open()` method.
 */
struct DBOptions final {
	DBMode mode;
	std::string name;
	int parallelism;
};

} // namespace rocksdb_js

#endif
