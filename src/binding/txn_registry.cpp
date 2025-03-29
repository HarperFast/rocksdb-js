#include "txn_registry.h"

namespace rocksdb_js {

// Initialize the static instance
std::unique_ptr<TxnRegistry> TxnRegistry::instance;

} // namespace rocksdb_js
