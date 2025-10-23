#include "db_descriptor.h"
#include "macros.h"
#include "transaction_log_handle.h"
#include "util.h"

namespace rocksdb_js {

TransactionLogHandle::TransactionLogHandle(
	const std::shared_ptr<DBHandle>& dbHandle,
	const std::string& logName
): dbHandle(dbHandle), logName(logName) {
	DEBUG_LOG("%p TransactionLogHandle::TransactionLogHandle Creating TransactionLogHandle \"%s\"\n", this, logName.c_str())
	this->store = dbHandle->descriptor->resolveTransactionLogStore(logName);
}

TransactionLogHandle::~TransactionLogHandle() {
	DEBUG_LOG("%p TransactionLogHandle::~TransactionLogHandle Closing TransactionLogHandle \"%s\"\n", this, this->logName.c_str())
	this->close();
}

void TransactionLogHandle::addEntry(uint64_t timestamp, char* data, size_t size) {
	auto store = this->store.lock();
	if (!store) {
		// store was closed/destroyed, try to get or create a new one
		auto dbHandle = this->dbHandle.lock();
		if (!dbHandle) {
			throw std::runtime_error("Database has been closed");
		}
		store = dbHandle->descriptor->resolveTransactionLogStore(this->logName);
		this->store = store; // update weak_ptr to point to new store
	}
	store->addEntry(timestamp, data, size);
}

void TransactionLogHandle::close() {
	// remove this handle from the `DBHandle`
	DEBUG_LOG("%p TransactionLogHandle::close Closing TransactionLogHandle \"%s\"\n", this, this->logName.c_str())

	auto dbHandle = this->dbHandle.lock();
	if (dbHandle) {
		dbHandle->unrefLog(this->logName);
	}
}

void TransactionLogHandle::query() {
	auto store = this->store.lock();
	if (!store) {
		// store was closed/destroyed, try to get or create a new one
		auto dbHandle = this->dbHandle.lock();
		if (!dbHandle) {
			throw std::runtime_error("Database has been closed");
		}
		store = dbHandle->descriptor->resolveTransactionLogStore(this->logName);
		this->store = store; // update weak_ptr to point to new store
	}
	store->query();
}

} // namespace rocksdb_js
