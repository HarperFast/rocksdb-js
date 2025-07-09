#include "db_descriptor.h"

namespace rocksdb_js {

/**
 * Creates a new database descriptor.
 */
DBDescriptor::DBDescriptor(
	std::string path,
	DBMode mode,
	std::shared_ptr<rocksdb::DB> db,
	std::unordered_map<std::string, std::shared_ptr<rocksdb::ColumnFamilyHandle>> columns
):
	path(path),
	mode(mode),
	db(db)
{
	for (auto& column : columns) {
		this->columns[column.first] = column.second;
	}
}

/**
 * Destroy the database descriptor and any resources associated to it
 * (transactions, iterators, etc).
 */	
DBDescriptor::~DBDescriptor() {
	DEBUG_LOG("%p DBDescriptor::~DBDescriptor() %ld closables\n", this, this->closables.size())

	if (this->closables.size()) {
		while (!this->closables.empty()) {
			Closable* handle = *this->closables.begin();
			DEBUG_LOG("%p DBDescriptor::~DBDescriptor() closing closable %p\n", this, handle)
			this->closables.erase(handle);
			handle->close();
		}
	}

	// Clear everything else after all closables are done
	{
		std::lock_guard<std::mutex> lock(this->mutex);
		this->transactions.clear();
		this->columns.clear();
		this->db.reset();
	}
}

/**
 * Registers a database resource to be closed when the descriptor is closed.
 */
void DBDescriptor::attach(Closable* closable) {
	std::lock_guard<std::mutex> lock(this->mutex);
	this->closables.insert(closable);
}

/**
 * Unregisters a database resource from being closed when the descriptor is
 * closed.
 */
void DBDescriptor::detach(Closable* closable) {
	std::lock_guard<std::mutex> lock(this->mutex);
	this->closables.erase(closable);
}

/**
 * Adds a transaction to the registry.
 */
void DBDescriptor::transactionAdd(std::shared_ptr<TransactionHandle> txnHandle) {
	uint32_t id = txnHandle->id;
	std::lock_guard<std::mutex> lock(this->mutex);
	this->transactions[id] = txnHandle;
	this->closables.insert(txnHandle.get());
}

/**
 * Retrieves a transaction from the registry.
 */
std::shared_ptr<TransactionHandle> DBDescriptor::transactionGet(uint32_t id) {
	std::lock_guard<std::mutex> lock(this->mutex);
	return this->transactions[id];
}

/**
 * Removes a transaction from the registry.
 */
void DBDescriptor::transactionRemove(uint32_t id) {
	std::lock_guard<std::mutex> lock(this->mutex);
	this->transactions.erase(id);
}

} // namespace rocksdb_js
