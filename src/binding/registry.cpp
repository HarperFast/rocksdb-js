#include "registry.h"
#include "macros.h"
#include "util.h"

namespace rocksdb_js {

// Initialize the static instance
std::unique_ptr<Registry> Registry::instance;
std::mutex Registry::mutex;

// Update the databases map type in the Registry class
std::map<std::string, std::shared_ptr<Database>> databases;

std::shared_ptr<Database> Registry::getDatabase(const std::string& path) {
	Registry* registry = Registry::getInstance();

	std::lock_guard<std::mutex> lock(mutex);
	
	auto [it, inserted] = registry->databases.insert({path, nullptr});
	if (inserted) {
		// only create new Database if we actually inserted
		it->second = std::make_shared<Database>(path);
	}
	return it->second;
}

} // namespace rocksdb_js
