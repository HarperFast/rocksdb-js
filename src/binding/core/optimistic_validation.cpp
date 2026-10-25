#include "core/optimistic_validation.h"

namespace rocksdb {
class OptimisticTransaction;
}

namespace rocksdb_js {
rocksdb::Status checkTransactionForConflicts(rocksdb::OptimisticTransaction* txn, rocksdb::DB* db);
}

namespace rocksdb {

// Names the private member `OptimisticTransaction::Commit` runs before it writes (v11.8.1). Relies on
// `OptimisticTransaction` deriving from `Transaction` through a single non-virtual chain.
class OptimisticTransaction {
private:
	Status CheckTransactionForConflicts(DB* db);
	friend rocksdb::Status rocksdb_js::checkTransactionForConflicts(
		rocksdb::OptimisticTransaction* txn, rocksdb::DB* db);
};

} // namespace rocksdb

namespace rocksdb_js {

rocksdb::Status checkTransactionForConflicts(rocksdb::OptimisticTransaction* txn, rocksdb::DB* db) {
	return txn->CheckTransactionForConflicts(db);
}

rocksdb::Status validateOptimisticTransaction(rocksdb::Transaction* txn, rocksdb::DB* db) {
	return checkTransactionForConflicts(reinterpret_cast<rocksdb::OptimisticTransaction*>(txn), db->GetRootDB());
}

} // namespace rocksdb_js
