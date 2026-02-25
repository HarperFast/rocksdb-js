#include "db_descriptor.h"
#include "macros.h"
#include "transaction_log_handle.h"
#include "util.h"

namespace rocksdb_js {

TransactionLogHandle::TransactionLogHandle(
	const std::shared_ptr<DBHandle>& dbHandle,
	const std::string& logName
): dbHandle(dbHandle), logName(logName), transactionId(0) {
	DEBUG_LOG("%p TransactionLogHandle::TransactionLogHandle Creating TransactionLogHandle \"%s\"\n", this, logName.c_str());
	this->store = dbHandle->descriptor->resolveTransactionLogStore(logName);
}

TransactionLogHandle::~TransactionLogHandle() {
	DEBUG_LOG("%p TransactionLogHandle::~TransactionLogHandle Closing TransactionLogHandle \"%s\"\n", this, this->logName.c_str());
	this->close();
}

void TransactionLogHandle::addEntry(
	uint32_t transactionId,
	char* data,
	uint32_t size
) {
	auto dbHandle = this->dbHandle.lock();
	if (!dbHandle) {
		throw std::runtime_error("Database has been closed");
	}

	auto txnHandle = dbHandle->descriptor->transactionGet(transactionId);
	if (!txnHandle) {
		DEBUG_LOG("%p TransactionLogHandle::addEntry ERROR: Transaction id %u not found\n", this, transactionId);
		throw std::runtime_error("Transaction id " + std::to_string(transactionId) + " not found");
	}

	auto store = this->store.lock();
	if (!store) {
		// store was closed/destroyed, try to get or create a new one
		DEBUG_LOG("%p TransactionLogHandle::addEntry Store was destroyed, re-resolving \"%s\"\n", this, this->logName.c_str());
		store = dbHandle->descriptor->resolveTransactionLogStore(this->logName);
		this->store = store; // update shared_ptr to point to new store
	}

	// check if transaction is already bound to a different log store
	auto boundStore = txnHandle->boundLogStore.lock();
	if (boundStore && boundStore.get() != store.get()) {
		throw std::runtime_error("Log already bound to a transaction");
	}

	auto entry = std::make_unique<TransactionLogEntry>(store, data, size);
	txnHandle->addLogEntry(std::move(entry));
}

void TransactionLogHandle::close() {
	// remove this handle from the `DBHandle`
	DEBUG_LOG("%p TransactionLogHandle::close Closing TransactionLogHandle \"%s\"\n", this, this->logName.c_str());

	auto dbHandle = this->dbHandle.lock();
	if (dbHandle) {
		dbHandle->unrefLog(this->logName);
	}
}

uint64_t TransactionLogHandle::getLogFileSize(uint32_t sequenceNumber) {
	auto store = this->store.lock();
	if (store) return store->getLogFileSize(sequenceNumber);
	return 0;
}

std::weak_ptr<MemoryMap> TransactionLogHandle::getMemoryMap(uint32_t sequenceNumber) {
	auto store = this->store.lock();
	if (store) return store->getMemoryMap(sequenceNumber);
	return std::weak_ptr<MemoryMap>(); // nullptr
}

LogPosition TransactionLogHandle::findPosition(double timestamp) {
	auto store = this->store.lock();
	if (store) return store->findPositionByTimestamp(timestamp);
	return { 0, 0 };
}

LogPosition TransactionLogHandle::getLastFlushed() {
	auto store = this->store.lock();
	if (store) return store->getLastFlushedPosition();
	return { 0, 0 };
}

std::weak_ptr<LogPosition> TransactionLogHandle::getLastCommittedPosition() {
	auto store = this->store.lock();
	if (store) return store->getLastCommittedPosition();
	return std::weak_ptr<LogPosition>(); // nullptr
}

} // namespace rocksdb_js
