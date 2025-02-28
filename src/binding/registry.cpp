#include "registry.h"
#include "macros.h"
#include "util.h"

namespace rocksdb_js {

// Initialize the static instance
std::unique_ptr<Registry> Registry::instance;
std::mutex Registry::mutex;

// Update the databases map type in the Registry class
std::map<std::pair<std::string, std::string>, napi_value> databases;

napi_value Registry::getDatabaseStore(napi_env env, napi_value argv[], size_t argc) {
	Registry* registry = Registry::getInstance();

	std::string path;
	rocksdb_js::getString(env, argv[0], path);

	std::string name;
	rocksdb_js::getProperty(env, argv[1], "name", name);

	DatabaseKey key{path, name};
	
	{
		std::lock_guard<std::mutex> lock(mutex);
		
		if (registry->databases.find(key) != registry->databases.end()) {
			return registry->databases[key];
		}

		// create a new database instance
		napi_value database_instance;
		napi_value constructor;
		NAPI_STATUS_THROWS(::napi_get_reference_value(env, Database::constructor_ref, &constructor));
		NAPI_STATUS_THROWS(::napi_new_instance(env, constructor, 1, argv, &database_instance));

		registry->databases[key] = database_instance;

		napi_value open_method;
		NAPI_STATUS_THROWS(::napi_get_named_property(env, database_instance, "open", &open_method));

		napi_value open_result;
		NAPI_STATUS_THROWS(::napi_call_function(env, database_instance, open_method, 1, argv + 1, &open_result));

		return database_instance;
	}
}

} // namespace rocksdb_js
