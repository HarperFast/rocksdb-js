#ifndef __REGISTRY_H__
#define __REGISTRY_H__

#include "database.h"
#include "store.h"
#include <node_api.h>
#include <mutex>

namespace rocksdb_js {

class Registry final {
private:
	Registry() = default;

	struct DatabaseKey {
		std::string path;
		std::string name; // column family name
		bool operator<(const DatabaseKey& other) const {
			if (path != other.path) return path < other.path;
			return name < other.name;
		}
	};

	std::map<std::string, std::shared_ptr<Database>> databases;
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

	static std::shared_ptr<Database> getDatabase(const std::string& path);
};

} // namespace rocksdb_js

#endif