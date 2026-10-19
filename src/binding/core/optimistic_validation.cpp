#include "core/optimistic_validation.h"
#include <algorithm>
#include "core/key_hash.h"

namespace rocksdb {

/**
 * The check `OptimisticTransaction::Commit` runs over the transaction's own
 * tracked keys before it writes. It is not in RocksDB's public headers; this
 * declaration names only that member, and the call goes through the
 * transaction's own pointer (`OptimisticTransaction` derives from
 * `Transaction` through a single non-virtual chain, so the pointers are
 * equal). A signature change on a RocksDB upgrade fails the link rather than
 * the verdict; `test/native/optimistic_validation_test.cc` pins the verdict
 * against `Commit()`.
 */
class OptimisticTransaction {
public:
	Status CheckTransactionForConflicts(DB* db);
};

} // namespace rocksdb

namespace rocksdb_js {

namespace {

struct StagedKeyCollector final : rocksdb::WriteBatch::Handler {
	std::vector<StagedKey>& out;

	explicit StagedKeyCollector(std::vector<StagedKey>& out) : out(out) {}

	rocksdb::Status PutCF(uint32_t columnFamilyId, const rocksdb::Slice& key, const rocksdb::Slice&) override {
		this->out.push_back({ columnFamilyId, key });
		return rocksdb::Status::OK();
	}

	rocksdb::Status DeleteCF(uint32_t columnFamilyId, const rocksdb::Slice& key) override {
		this->out.push_back({ columnFamilyId, key });
		return rocksdb::Status::OK();
	}

	rocksdb::Status SingleDeleteCF(uint32_t columnFamilyId, const rocksdb::Slice& key) override {
		this->out.push_back({ columnFamilyId, key });
		return rocksdb::Status::OK();
	}

	rocksdb::Status MergeCF(uint32_t columnFamilyId, const rocksdb::Slice& key, const rocksdb::Slice&) override {
		this->out.push_back({ columnFamilyId, key });
		return rocksdb::Status::OK();
	}
};

} // namespace

void collectStagedKeys(const rocksdb::WriteBatch& batch, std::vector<StagedKey>& out) {
	out.clear();
	out.reserve(static_cast<size_t>(batch.Count()));
	StagedKeyCollector collector(out);
	batch.Iterate(&collector);
}

std::vector<uint64_t> commitKeyHashes(std::span<const StagedKey> keys) {
	std::vector<uint64_t> hashes;
	hashes.reserve(keys.size());
	for (const StagedKey& staged : keys) {
		const uint64_t seed = mix64(static_cast<uint64_t>(staged.columnFamilyId) + 0x9e3779b97f4a7c15ULL);
		hashes.push_back(hashKeyBytes(
			reinterpret_cast<const uint8_t*>(staged.key.data()), staged.key.size(), seed));
	}
	std::sort(hashes.begin(), hashes.end());
	hashes.erase(std::unique(hashes.begin(), hashes.end()), hashes.end());
	return hashes;
}

rocksdb::Status validateOptimisticTransaction(rocksdb::Transaction* txn, rocksdb::DB* db) {
	return reinterpret_cast<rocksdb::OptimisticTransaction*>(txn)->CheckTransactionForConflicts(db->GetRootDB());
}

} // namespace rocksdb_js
