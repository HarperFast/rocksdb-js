#ifndef DB_TRANSACTION_H
#define DB_TRANSACTION_H

// #include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"

namespace rocksdb_js {

class TxnHandle {

};

class Txn {
private:
	rocksdb::Transaction* txn;
};

} // namespace rocksdb_js

#endif