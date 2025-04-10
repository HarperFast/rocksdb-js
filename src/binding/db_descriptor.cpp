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
 * Destroys the database descriptor.
 */
DBDescriptor::~DBDescriptor() {
	std::lock_guard<std::mutex> lock(txnMutex);
	for (auto& txn : this->transactions) {
		txn.second.reset();
	}
	this->transactions.clear();
	this->columns.clear();
	this->db.reset();
}

/**
 * Adds a transaction to the registry.
 */
void DBDescriptor::addTransaction(std::shared_ptr<TransactionHandle> txnHandle) {
	uint32_t id = txnHandle->id;
	std::lock_guard<std::mutex> lock(txnMutex);
	transactions[id] = txnHandle;
}

/**
 * Retrieves a transaction from the registry.
 */
std::shared_ptr<TransactionHandle> DBDescriptor::getTransaction(uint32_t id) {
	std::lock_guard<std::mutex> lock(txnMutex);
	return transactions[id];
}

/**
 * Removes a transaction from the registry.
 */
void DBDescriptor::removeTransaction(std::shared_ptr<TransactionHandle> txnHandle) {
	uint32_t id = txnHandle->id;
	std::lock_guard<std::mutex> lock(txnMutex);
	transactions.erase(id);
}

} // namespace rocksdb_js
