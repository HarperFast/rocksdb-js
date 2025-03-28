#ifndef __TXN_REGISTRY_H__
#define __TXN_REGISTRY_H__

#include <memory>
#include <mutex>
#include <string>
#include "transaction.h"

namespace rocksdb_js {

class TxnRegistry {
private:
	TxnRegistry() = default;

	std::map<std::string, std::unique_ptr<TransactionHandle>> transactions;

	static std::unique_ptr<TxnRegistry> instance;
	std::mutex mutex;
};

} // namespace rocksdb_js

#endif
