#ifndef __REGISTRY_H__
#define __REGISTRY_H__

#include "dbi.h"
#include <node_api.h>
#include <mutex>

namespace rocksdb_js {

class Registry final {
private:
	Registry() = default;

	std::map<std::string, std::shared_ptr<RocksDBHandle>> databases;

	static std::unique_ptr<Registry> instance;
	static std::mutex mutex;

public:
	~Registry() = default;

	static Registry* getInstance() {
		if (!instance) {
			instance = std::unique_ptr<Registry>(new Registry());
		}
		return instance.get();
	}

	static void cleanup() {
		instance.reset();
	}

	static DBI* open(const std::string& path, const std::string& name = "");
};

} // namespace rocksdb_js

#endif