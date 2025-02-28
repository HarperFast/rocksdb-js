#ifndef __REGISTRY_H__
#define __REGISTRY_H__

#include "database.h"
#include <node_api.h>
#include <mutex>

namespace rocksdb_js {

class Registry final {
private:
	Registry() = default;

	struct DatabaseKey {
		std::string path;
		std::string name;
		bool operator<(const DatabaseKey& other) const {
			if (path != other.path) return path < other.path;
			return name < other.name;
		}
	};

	std::map<DatabaseKey, napi_value> databases;
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

	static napi_value getDatabaseStore(napi_env env, napi_value argv[], size_t argc);
};

} // namespace rocksdb_js

#endif