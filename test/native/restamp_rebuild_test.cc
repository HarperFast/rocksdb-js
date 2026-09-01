#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <string>
#include <unordered_map>
#include <utility>

#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/write_batch.h"
#include "core/encoding.h"

// Acceptance gate for the commit-time re-stamp path
// (docs/design/local-mutation-stamping.md §3.3), pinning the verified RocksDB
// 11.8.1 semantics of Transaction::RebuildFromWriteBatch:
//   1. it APPENDS the source batch's operations (it does not clear) — which is
//      still terminal-state-exact because the appended FULL-ORDER copy ends
//      every key at the same op the patched original would (a Put→Delete pair
//      stays deleted; the rejected puts-only re-put would resurrect it), at
//      the cost of ~2x batch bytes on the re-stamp path only;
//   2. per-key terminal state after the rebuild is the patched one
//      (read-your-writes and committed state both);
//   3. conflict-detection key tracking survives at the ORIGINAL sequence
//      numbers (a conflicting write between the original put and the rebuild
//      still fails the commit with Busy).
// If any of these change on a RocksDB upgrade, the re-stamp path must not
// ship on that version without re-verification.

namespace {

namespace fs = std::filesystem;

struct PatchingRebuilder final : rocksdb::WriteBatch::Handler {
	rocksdb::WriteBatch copy;
	double stamp;
	// cf id -> (handle, stamped)
	std::unordered_map<uint32_t, std::pair<rocksdb::ColumnFamilyHandle*, bool>> cfs;
	rocksdb::Status failure;

	rocksdb::ColumnFamilyHandle* handleFor(uint32_t cfId) {
		auto it = this->cfs.find(cfId);
		return it == this->cfs.end() ? nullptr : it->second.first;
	}

	bool stamped(uint32_t cfId) {
		auto it = this->cfs.find(cfId);
		return it != this->cfs.end() && it->second.second;
	}

	rocksdb::Status PutCF(uint32_t cfId, const rocksdb::Slice& key, const rocksdb::Slice& value) override {
		auto* handle = this->handleFor(cfId);
		if (!handle) return rocksdb::Status::InvalidArgument("unknown column family");
		if (this->stamped(cfId) && value.size() >= 8) {
			char prefix[8];
			rocksdb_js::writeDoubleBE(prefix, this->stamp);
			rocksdb::Slice keyPart = key;
			rocksdb::Slice valueParts[2] = {
				rocksdb::Slice(prefix, sizeof prefix),
				rocksdb::Slice(value.data() + 8, value.size() - 8),
			};
			return this->copy.Put(handle, rocksdb::SliceParts(&keyPart, 1), rocksdb::SliceParts(valueParts, 2));
		}
		return this->copy.Put(handle, key, value);
	}

	rocksdb::Status DeleteCF(uint32_t cfId, const rocksdb::Slice& key) override {
		auto* handle = this->handleFor(cfId);
		if (!handle) return rocksdb::Status::InvalidArgument("unknown column family");
		return this->copy.Delete(handle, key);
	}

	rocksdb::Status SingleDeleteCF(uint32_t cfId, const rocksdb::Slice& key) override {
		auto* handle = this->handleFor(cfId);
		if (!handle) return rocksdb::Status::InvalidArgument("unknown column family");
		return this->copy.SingleDelete(handle, key);
	}

	rocksdb::Status MergeCF(uint32_t cfId, const rocksdb::Slice& key, const rocksdb::Slice& value) override {
		auto* handle = this->handleFor(cfId);
		if (!handle) return rocksdb::Status::InvalidArgument("unknown column family");
		return this->copy.Merge(handle, key, value);
	}

	void LogData(const rocksdb::Slice& blob) override { this->copy.PutLogData(blob); }
};

struct RebuildFixture : ::testing::Test {
	fs::path dir;
	rocksdb::OptimisticTransactionDB* db = nullptr;

	void SetUp() override {
		this->dir = fs::temp_directory_path() /
			("rocksdbjs-restamp-" + std::to_string(::getpid()) + "-" +
			 std::to_string(reinterpret_cast<uintptr_t>(this)));
		fs::remove_all(this->dir);
		rocksdb::Options options;
		options.create_if_missing = true;
		ASSERT_TRUE(rocksdb::OptimisticTransactionDB::Open(options, this->dir.string(), &this->db).ok());
	}

	void TearDown() override {
		delete this->db;
		fs::remove_all(this->dir);
	}

	std::string stampedValue(double stamp, const std::string& payload) {
		std::string value(8, '\0');
		rocksdb_js::writeDoubleBE(value.data(), stamp);
		return value + payload;
	}

	PatchingRebuilder rebuilderFor(rocksdb::Transaction* txn, double stamp) {
		PatchingRebuilder rebuilder;
		rebuilder.stamp = stamp;
		auto* defaultCf = this->db->DefaultColumnFamily();
		rebuilder.cfs.emplace(defaultCf->GetID(), std::make_pair(defaultCf, true));
		return rebuilder;
	}
};

TEST_F(RebuildFixture, RebuildReplacesAndPatchesPreservingOrder) {
	std::unique_ptr<rocksdb::Transaction> txn(this->db->BeginTransaction(rocksdb::WriteOptions()));
	ASSERT_TRUE(txn->Put("a", this->stampedValue(100.0, "alpha")).ok());
	ASSERT_TRUE(txn->Put("b", this->stampedValue(100.0, "beta")).ok());
	// Put then Delete: the key must stay deleted after the rebuild (an
	// append-style "re-put" would resurrect it — the rejected shortcut).
	ASSERT_TRUE(txn->Put("gone", this->stampedValue(100.0, "temp")).ok());
	ASSERT_TRUE(txn->Delete("gone").ok());
	// Repeated key: last value wins, patched.
	ASSERT_TRUE(txn->Put("a", this->stampedValue(100.0, "alpha2")).ok());

	const int originalCount = txn->GetWriteBatch()->GetWriteBatch()->Count();

	auto rebuilder = this->rebuilderFor(txn.get(), 777.0);
	ASSERT_TRUE(txn->GetWriteBatch()->GetWriteBatch()->Iterate(&rebuilder).ok());
	ASSERT_TRUE(txn->RebuildFromWriteBatch(&rebuilder.copy).ok());

	// Verified 11.8.1 semantics: append of the full-order copy (2x count). A
	// count that stops matching this on an upgrade means the semantics changed
	// and the re-stamp path needs re-verification.
	EXPECT_EQ(txn->GetWriteBatch()->GetWriteBatch()->Count(), 2 * originalCount);

	// Read-your-writes reflects the patch and the preserved delete.
	std::string value;
	ASSERT_TRUE(txn->Get(rocksdb::ReadOptions(), "a", &value).ok());
	EXPECT_EQ(rocksdb_js::readDoubleBE(value.data()), 777.0);
	EXPECT_EQ(value.substr(8), "alpha2");
	EXPECT_TRUE(txn->Get(rocksdb::ReadOptions(), "gone", &value).IsNotFound());

	ASSERT_TRUE(txn->Commit().ok());

	ASSERT_TRUE(this->db->Get(rocksdb::ReadOptions(), "a", &value).ok());
	EXPECT_EQ(rocksdb_js::readDoubleBE(value.data()), 777.0);
	ASSERT_TRUE(this->db->Get(rocksdb::ReadOptions(), "b", &value).ok());
	EXPECT_EQ(rocksdb_js::readDoubleBE(value.data()), 777.0);
	EXPECT_EQ(value.substr(8), "beta");
	EXPECT_TRUE(this->db->Get(rocksdb::ReadOptions(), "gone", &value).IsNotFound());
}

TEST_F(RebuildFixture, RebuildPreservesConflictTracking) {
	std::unique_ptr<rocksdb::Transaction> txn(this->db->BeginTransaction(rocksdb::WriteOptions()));
	ASSERT_TRUE(txn->Put("k", this->stampedValue(100.0, "mine")).ok());

	// A conflicting committed write lands AFTER the transaction's put but
	// BEFORE the rebuild. If the rebuild re-tracked the key at rebuild-time
	// sequence numbers, the conflict window would shrink and this commit would
	// wrongly succeed.
	ASSERT_TRUE(this->db->Put(rocksdb::WriteOptions(), "k", "theirs").ok());

	auto rebuilder = this->rebuilderFor(txn.get(), 777.0);
	ASSERT_TRUE(txn->GetWriteBatch()->GetWriteBatch()->Iterate(&rebuilder).ok());
	ASSERT_TRUE(txn->RebuildFromWriteBatch(&rebuilder.copy).ok());

	rocksdb::Status status = txn->Commit();
	EXPECT_TRUE(status.IsBusy()) << "conflict tracking lost across rebuild: " << status.ToString();
}

} // namespace
