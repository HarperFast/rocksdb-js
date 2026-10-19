#include "core/optimistic_validation.h"
#include <algorithm>
#include "core/key_hash.h"

namespace rocksdb {
class OptimisticTransaction;
}

namespace rocksdb_js {
rocksdb::Status checkTransactionForConflicts(rocksdb::OptimisticTransaction* txn, rocksdb::DB* db);
}

namespace rocksdb {

/**
 * Names the one member `OptimisticTransaction::Commit` runs over the
 * transaction's own tracked keys before it writes (private in v11.8.1, hence
 * the friend: MSVC mangles the access level into the symbol). RocksDB ships no
 * internal headers in the prebuild, so this declaration stands in for the real
 * class and declares no layout. The call relies on `OptimisticTransaction`
 * deriving from `Transaction` through a single non-virtual chain, so the two
 * pointers are equal (the addon is built without RTTI, so `dynamic_cast` cannot
 * recover the complete object). A signature change on an upgrade fails the
 * link; `test/native/optimistic_validation_test.cc` fails if the verdict stops
 * matching `Commit()`, which a wrong `this` cannot survive.
 */
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

namespace {

struct CommitKeyHasher final : rocksdb::WriteBatch::Handler {
	std::vector<uint64_t>& out;

	explicit CommitKeyHasher(std::vector<uint64_t>& out) : out(out) {}

	rocksdb::Status PutCF(uint32_t columnFamilyId, const rocksdb::Slice& key, const rocksdb::Slice&) override {
		this->out.push_back(commitKeyHash(columnFamilyId, key));
		return rocksdb::Status::OK();
	}

	rocksdb::Status DeleteCF(uint32_t columnFamilyId, const rocksdb::Slice& key) override {
		this->out.push_back(commitKeyHash(columnFamilyId, key));
		return rocksdb::Status::OK();
	}

	rocksdb::Status SingleDeleteCF(uint32_t columnFamilyId, const rocksdb::Slice& key) override {
		this->out.push_back(commitKeyHash(columnFamilyId, key));
		return rocksdb::Status::OK();
	}

	rocksdb::Status MergeCF(uint32_t columnFamilyId, const rocksdb::Slice& key, const rocksdb::Slice&) override {
		this->out.push_back(commitKeyHash(columnFamilyId, key));
		return rocksdb::Status::OK();
	}

	rocksdb::Status DeleteRangeCF(uint32_t, const rocksdb::Slice&, const rocksdb::Slice&) override {
		return rocksdb::Status::NotSupported("A range delete cannot be reserved per key");
	}
};

} // namespace

uint64_t commitKeyHash(uint32_t columnFamilyId, const rocksdb::Slice& key) {
	const uint64_t seed = mix64(static_cast<uint64_t>(columnFamilyId) + 0x9e3779b97f4a7c15ULL);
	return hashKeyBytes(reinterpret_cast<const uint8_t*>(key.data()), key.size(), seed);
}

rocksdb::Status collectCommitKeyHashes(const rocksdb::WriteBatch& batch, std::vector<uint64_t>& out) {
	out.clear();
	out.reserve(static_cast<size_t>(batch.Count()));
	CommitKeyHasher hasher(out);
	rocksdb::Status status = batch.Iterate(&hasher);
	if (!status.ok()) {
		return status;
	}
	std::sort(out.begin(), out.end());
	out.erase(std::unique(out.begin(), out.end()), out.end());
	return rocksdb::Status::OK();
}

rocksdb::Status validateOptimisticTransaction(rocksdb::Transaction* txn, rocksdb::DB* db) {
	return checkTransactionForConflicts(reinterpret_cast<rocksdb::OptimisticTransaction*>(txn), db->GetRootDB());
}

} // namespace rocksdb_js
