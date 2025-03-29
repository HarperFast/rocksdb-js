#ifndef __TXN_REGISTRY_H__
#define __TXN_REGISTRY_H__

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include "transaction.h"

namespace rocksdb_js {

struct PairHash {
    template <class T1, class T2>
    std::size_t operator()(const std::pair<T1, T2>& p) const {
        auto h1 = std::hash<T1>{}(p.first);
        auto h2 = std::hash<T2>{}(p.second);
        return h1 ^ (h2 << 1);
    }
};

/**
 * A singleton registry that tracks all active transactions.
 */
class TxnRegistry {
private:
	TxnRegistry() = default;

	std::unordered_map<
		std::pair<std::string, uint32_t>,
		TransactionHandle*,
		PairHash
	> transactions;
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
	void addTransaction(std::string& dbPath, TransactionHandle* txnHandle) {
		uint32_t id = txnHandle->id;
		std::lock_guard<std::mutex> lock(mutex);
		transactions[std::make_pair(dbPath, id)] = txnHandle;
	}

	/**
	 * Retrieves a transaction from the registry.
	 */
	TransactionHandle* getTransaction(std::string& dbPath, uint32_t id) {
		std::lock_guard<std::mutex> lock(mutex);
		return transactions[std::make_pair(dbPath, id)];
	}

	/**
	 * Removes a transaction from the registry.
	 */
	void removeTransaction(std::string& dbPath, TransactionHandle* txnHandle) {
		uint32_t id = txnHandle->id;
		std::lock_guard<std::mutex> lock(mutex);
		transactions.erase(std::make_pair(dbPath, id));
	}
};

} // namespace rocksdb_js

#endif
