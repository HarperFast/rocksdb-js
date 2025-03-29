#ifndef __TXN_REGISTRY_H__
#define __TXN_REGISTRY_H__

#include <memory>
#include <mutex>
#include <string>
#include "transaction.h"

namespace rocksdb_js {

/**
 * A singleton registry that tracks all active transactions.
 */
class TxnRegistry {
private:
	TxnRegistry() = default;

	std::unordered_map<uint32_t, TransactionHandle*> transactions;

	static std::unique_ptr<TxnRegistry> instance;
	std::mutex mutex;

public:
	static TxnRegistry* getInstance() {
		if (!instance) {
			instance = std::unique_ptr<TxnRegistry>(new TxnRegistry());
		}
		return instance.get();
	}

	/**
	 * Adds a transaction to the registry.
	 */
	void addTransaction(TransactionHandle* txnHandle) {
		uint32_t id = txnHandle->id;
		std::lock_guard<std::mutex> lock(mutex);
		transactions[id] = txnHandle;
	}

	/**
	 * Retrieves a transaction from the registry.
	 */
	TransactionHandle* getTransaction(uint32_t id) {
		std::lock_guard<std::mutex> lock(mutex);
		return transactions[id];
	}

	/**
	 * Removes a transaction from the registry.
	 */
	void removeTransaction(TransactionHandle* txnHandle) {
		uint32_t id = txnHandle->id;
		std::lock_guard<std::mutex> lock(mutex);
		transactions.erase(id);
	}
};

} // namespace rocksdb_js

#endif
