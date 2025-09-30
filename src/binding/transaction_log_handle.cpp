#include "transaction_log_handle.h"

namespace rocksdb_js {

TransactionLogHandle::TransactionLogHandle(const std::filesystem::path& path) :
	path(path.string())
{
	this->name = path.stem().string();
}

TransactionLogHandle::~TransactionLogHandle() {
}

} // namespace rocksdb_js
