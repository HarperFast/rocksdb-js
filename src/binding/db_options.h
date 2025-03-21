#ifndef __DB_OPTIONS_H__
#define __DB_OPTIONS_H__

#include <string>

namespace rocksdb_js {

/**
 * Options for opening a RocksDB database.
 */
struct DBOptions final {
	std::string name;
	int parallelism;
};

} // namespace rocksdb_js

#endif
