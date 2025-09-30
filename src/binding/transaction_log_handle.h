#ifndef __TRANSACTION_LOG_HANDLE_H__
#define __TRANSACTION_LOG_HANDLE_H__

#include <string>
#include <filesystem>

namespace rocksdb_js {

struct TransactionLogHandle final {
	TransactionLogHandle(const std::filesystem::path& path);
	~TransactionLogHandle();

	std::string path;
	std::string name;
};

} // namespace rocksdb_js

#endif
