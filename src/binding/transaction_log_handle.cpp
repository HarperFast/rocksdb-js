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
	DEBUG_LOG("%p TransactionLogHandle::~TransactionLogHandle Closing TransactionLogHandle for: %s\n", this, this->store->name.c_str())
	this->close();
}

void TransactionLogHandle::close() {
	// remove this handle from the `DBHandle`
	DEBUG_LOG("%p TransactionLogHandle::close Closing TransactionLogHandle for: %s\n", this, this->store->name.c_str())
	this->dbHandle->unrefLog(this->store->name);
}

} // namespace rocksdb_js
