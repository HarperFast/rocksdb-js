#include "db_descriptor.h"
#include "macros.h"
#include "transaction_log_handle.h"
#include "util.h"

namespace rocksdb_js {

TransactionLogHandle::TransactionLogHandle(
	const std::shared_ptr<DBHandle>& dbHandle,
	const std::string& logName
): dbHandle(dbHandle) {
    DEBUG_LOG("%p TransactionLogHandle::TransactionLogHandle Creating TransactionLogHandle for: %s\n", this, logName.c_str())
    this->store = dbHandle->descriptor->resolveTransactionLogStore(logName);
}

TransactionLogHandle::~TransactionLogHandle() {
    //
}

} // namespace rocksdb_js
