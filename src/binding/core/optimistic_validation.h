#ifndef __CORE_OPTIMISTIC_VALIDATION_H__
#define __CORE_OPTIMISTIC_VALIDATION_H__

#include <cstdint>
#include <span>
#include <vector>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/write_batch.h"

namespace rocksdb_js {

/** One entry of a transaction's write batch; `key` points into the batch. */
struct StagedKey final {
	uint32_t columnFamilyId = 0;
	rocksdb::Slice key;
};

/** Walks `batch` in insertion order. */
void collectStagedKeys(const rocksdb::WriteBatch& batch, std::vector<StagedKey>& out);

/** Reservation hashes for `keys`, sorted and deduplicated. */
std::vector<uint64_t> commitKeyHashes(std::span<const StagedKey> keys);

/**
 * RocksDB's own optimistic conflict check for `txn`'s tracked keys, run ahead
 * of `txn->Commit()` so a logged transaction can be refused before its log
 * batch is written. Returns what `Commit()` would return for the same state:
 * OK, `Busy` on a conflicting write, `TryAgain` when the memtable history no
 * longer covers a tracked sequence. `txn` must come from an
 * `OptimisticTransactionDB` and `db` must be that database or its base.
 */
rocksdb::Status validateOptimisticTransaction(rocksdb::Transaction* txn, rocksdb::DB* db);

} // namespace rocksdb_js

#endif
