#ifndef __CORE_OPTIMISTIC_VALIDATION_H__
#define __CORE_OPTIMISTIC_VALIDATION_H__

#include <cstdint>
#include <vector>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/write_batch.h"

namespace rocksdb_js {

uint64_t commitKeyHash(uint32_t columnFamilyId, const rocksdb::Slice& key);

/**
 * Reservation hashes for every key `batch` names, sorted and deduplicated.
 * Fails closed on a batch record kind the reservation cannot cover.
 */
rocksdb::Status collectCommitKeyHashes(const rocksdb::WriteBatch& batch, std::vector<uint64_t>& out);

/**
 * RocksDB's own optimistic conflict check for `txn`'s tracked keys, run ahead
 * of `txn->Commit()` so a logged transaction can be refused before its log
 * batch is written. Returns what `Commit()` would return for the same state:
 * OK, `Busy` on a conflicting write, `TryAgain` when the memtable history no
 * longer covers a tracked sequence. `txn` must come from an
 * `OptimisticTransactionDB`; callers gate on the descriptor's mode.
 */
rocksdb::Status validateOptimisticTransaction(rocksdb::Transaction* txn, rocksdb::DB* db);

} // namespace rocksdb_js

#endif
