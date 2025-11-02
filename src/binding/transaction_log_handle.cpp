#include "db_descriptor.h"
#include "macros.h"
#include "transaction_log_handle.h"
#include "util.h"

namespace rocksdb_js {

TransactionLogHandle::TransactionLogHandle(
	const std::shared_ptr<DBHandle>& dbHandle,
	const std::string& logName
): dbHandle(dbHandle), logName(logName), transactionId(0) {
	DEBUG_LOG("%p TransactionLogHandle::TransactionLogHandle Creating TransactionLogHandle \"%s\"\n", this, logName.c_str())
	this->store = dbHandle->descriptor->resolveTransactionLogStore(logName);
}

TransactionLogHandle::~TransactionLogHandle() {
	DEBUG_LOG("%p TransactionLogHandle::~TransactionLogHandle Closing TransactionLogHandle \"%s\"\n", this, this->logName.c_str())
	this->close();
}

void TransactionLogHandle::addEntry(
	uint32_t transactionId,
	std::unique_ptr<char[]> data,
	size_t size
) {
	auto dbHandle = this->dbHandle.lock();
	if (!dbHandle) {
		throw std::runtime_error("Database has been closed");
	}

	auto txnHandle = dbHandle->descriptor->transactionGet(transactionId);
	if (!txnHandle) {
		DEBUG_LOG("%p TransactionLogHandle::addEntry Transaction id %u not found\n", this, transactionId)
		throw std::runtime_error("Transaction id " + std::to_string(transactionId) + " not found");
	}

	auto store = this->store.lock();
	if (!store) {
		// store was closed/destroyed, try to get or create a new one
		DEBUG_LOG("%p TransactionLogHandle::addEntry Store was destroyed, re-resolving \"%s\"\n", this, this->logName.c_str())
		store = dbHandle->descriptor->resolveTransactionLogStore(this->logName);
		this->store = store; // update weak_ptr to point to new store
	}

	auto entry = std::make_unique<TransactionLogEntry>(store, std::move(data), size);
	txnHandle->addLogEntry(std::move(entry));
}

void TransactionLogHandle::addEntry(
	uint32_t transactionId,
	char* data,
	size_t size,
	napi_env env,
	napi_ref bufferRef
) {
	auto dbHandle = this->dbHandle.lock();
	if (!dbHandle) {
		throw std::runtime_error("Database has been closed");
	}

	auto txnHandle = dbHandle->descriptor->transactionGet(transactionId);
	if (!txnHandle) {
		DEBUG_LOG("%p TransactionLogHandle::addEntry Transaction id %u not found\n", this, transactionId)
		throw std::runtime_error("Transaction id " + std::to_string(transactionId) + " not found");
	}

	auto store = this->store.lock();
	if (!store) {
		// store was closed/destroyed, try to get or create a new one
		DEBUG_LOG("%p TransactionLogHandle::addEntry Store was destroyed, re-resolving \"%s\"\n", this, this->logName.c_str())
		store = dbHandle->descriptor->resolveTransactionLogStore(this->logName);
		this->store = store; // update weak_ptr to point to new store
	}

	auto entry = std::make_unique<TransactionLogEntry>(store, data, size, env, bufferRef);
	txnHandle->addLogEntry(std::move(entry));
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
