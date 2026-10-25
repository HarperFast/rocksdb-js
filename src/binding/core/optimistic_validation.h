#ifndef __CORE_OPTIMISTIC_VALIDATION_H__
#define __CORE_OPTIMISTIC_VALIDATION_H__

#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/transaction.h"

namespace rocksdb_js {

/**
 * RocksDB's own optimistic conflict check for `txn`'s tracked keys, run ahead of `txn->Commit()`
 * (taken from rocksdb-js#936). `txn` must come from an `OptimisticTransactionDB`.
 */
rocksdb::Status validateOptimisticTransaction(rocksdb::Transaction* txn, rocksdb::DB* db);

} // namespace rocksdb_js

#endif
