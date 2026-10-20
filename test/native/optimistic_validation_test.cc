// Pins the pre-log conflict validator to RocksDB's own commit verdict. The
// validator links `OptimisticTransaction::CheckTransactionForConflicts`, which
// is not public API: a RocksDB upgrade that changes its signature fails the
// link, one that changes its semantics fails here. Every case asserts
// agreement with `Commit()` on the same transaction, not a fixed verdict.
#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include "core/optimistic_validation.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction.h"
#include <algorithm>

using rocksdb_js::collectCommitKeyHashes;
using rocksdb_js::commitKeyHash;
using rocksdb_js::validateOptimisticTransaction;

namespace {

struct Fixture {
	std::filesystem::path path;
	rocksdb::OptimisticTransactionDB* odb = nullptr;
	rocksdb::DB* base = nullptr;
	rocksdb::ColumnFamilyHandle* other = nullptr;

	explicit Fixture(rocksdb::OccValidationPolicy policy = rocksdb::OccValidationPolicy::kValidateParallel) {
		this->path = std::filesystem::temp_directory_path() / "rocksdb-js-optimistic-validation";
		std::filesystem::remove_all(this->path);
		rocksdb::Options options;
		options.create_if_missing = true;
		rocksdb::OptimisticTransactionDBOptions dbOptions;
		dbOptions.validate_policy = policy;
		std::vector<rocksdb::ColumnFamilyDescriptor> families = {
			{ rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions() },
		};
		std::vector<rocksdb::ColumnFamilyHandle*> handles;
		rocksdb::Status status = rocksdb::OptimisticTransactionDB::Open(
			options, dbOptions, this->path.string(), families, &handles, &this->odb);
		if (!status.ok()) {
			throw std::runtime_error(status.ToString());
		}
		for (auto* handle : handles) {
			this->odb->DestroyColumnFamilyHandle(handle);
		}
		this->base = this->odb->GetBaseDB();
		status = this->odb->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "other", &this->other);
		if (!status.ok()) {
			throw std::runtime_error(status.ToString());
		}
	}

	~Fixture() {
		if (this->odb) {
			this->odb->DestroyColumnFamilyHandle(this->other);
			delete this->odb;
		}
		std::filesystem::remove_all(this->path);
	}

	rocksdb::Transaction* begin() {
		return this->odb->BeginTransaction(rocksdb::WriteOptions());
	}

	rocksdb::Status validate(rocksdb::Transaction* txn) {
		return validateOptimisticTransaction(txn, this->odb);
	}

	void expectAgreement(rocksdb::Transaction* txn, const char* label) {
		rocksdb::Status pre = this->validate(txn);
		rocksdb::Status commit = txn->Commit();
		EXPECT_EQ(pre.code(), commit.code()) << label << ": validator=" << pre.ToString()
			<< " commit=" << commit.ToString();
	}
};

} // namespace

TEST(OptimisticValidation, NoConflictAgreesWithCommit) {
	Fixture fixture;
	fixture.base->Put(rocksdb::WriteOptions(), "k", "v0");
	std::unique_ptr<rocksdb::Transaction> txn(fixture.begin());
	txn->SetSnapshot();
	ASSERT_TRUE(txn->Put("k", "v1").ok());
	EXPECT_TRUE(fixture.validate(txn.get()).ok());
	fixture.expectAgreement(txn.get(), "no conflict");
	std::string value;
	EXPECT_TRUE(fixture.base->Get(rocksdb::ReadOptions(), "k", &value).ok());
	EXPECT_EQ(value, "v1");
}

TEST(OptimisticValidation, PutAfterSnapshotIsBusyLikeCommit) {
	Fixture fixture;
	fixture.base->Put(rocksdb::WriteOptions(), "k", "v0");
	std::unique_ptr<rocksdb::Transaction> txn(fixture.begin());
	txn->SetSnapshot();
	ASSERT_TRUE(txn->Put("k", "v1").ok());
	EXPECT_TRUE(fixture.validate(txn.get()).ok());
	fixture.base->Put(rocksdb::WriteOptions(), "k", "other");
	EXPECT_TRUE(fixture.validate(txn.get()).IsBusy());
	fixture.expectAgreement(txn.get(), "put conflict");
}

TEST(OptimisticValidation, DeleteAfterSnapshotIsBusyLikeCommit) {
	Fixture fixture;
	fixture.base->Put(rocksdb::WriteOptions(), "k", "v0");
	std::unique_ptr<rocksdb::Transaction> txn(fixture.begin());
	txn->SetSnapshot();
	ASSERT_TRUE(txn->Put("k", "v1").ok());
	fixture.base->Delete(rocksdb::WriteOptions(), "k");
	EXPECT_TRUE(fixture.validate(txn.get()).IsBusy());
	fixture.expectAgreement(txn.get(), "delete conflict");
}

TEST(OptimisticValidation, BlindWriteWithoutSnapshotTracksLatestSequence) {
	Fixture fixture;
	fixture.base->Put(rocksdb::WriteOptions(), "k", "v0");
	std::unique_ptr<rocksdb::Transaction> txn(fixture.begin());
	ASSERT_TRUE(txn->Put("k", "v1").ok());
	EXPECT_TRUE(fixture.validate(txn.get()).ok());
	fixture.base->Put(rocksdb::WriteOptions(), "k", "other");
	EXPECT_TRUE(fixture.validate(txn.get()).IsBusy());
	fixture.expectAgreement(txn.get(), "blind write conflict");
}

TEST(OptimisticValidation, DuplicateKeyKeepsTheEarlierTrackedSequence) {
	Fixture fixture;
	fixture.base->Put(rocksdb::WriteOptions(), "k", "v0");
	std::unique_ptr<rocksdb::Transaction> txn(fixture.begin());
	ASSERT_TRUE(txn->Put("k", "v1").ok());
	fixture.base->Put(rocksdb::WriteOptions(), "k", "between");
	ASSERT_TRUE(txn->Put("k", "v2").ok());
	EXPECT_TRUE(fixture.validate(txn.get()).IsBusy());
	fixture.expectAgreement(txn.get(), "duplicate key");
}

TEST(OptimisticValidation, SecondColumnFamilyIsCheckedAgainstItsOwnHistory) {
	Fixture fixture;
	fixture.base->Put(rocksdb::WriteOptions(), fixture.other, "k", "v0");
	std::unique_ptr<rocksdb::Transaction> txn(fixture.begin());
	txn->SetSnapshot();
	ASSERT_TRUE(txn->Put("k", "default").ok());
	ASSERT_TRUE(txn->Delete(fixture.other, "k").ok());
	EXPECT_TRUE(fixture.validate(txn.get()).ok());
	fixture.base->Put(rocksdb::WriteOptions(), fixture.other, "k", "other");
	EXPECT_TRUE(fixture.validate(txn.get()).IsBusy());
	fixture.expectAgreement(txn.get(), "second column family");
}

TEST(OptimisticValidation, UnrelatedKeyNeverConflicts) {
	Fixture fixture;
	std::unique_ptr<rocksdb::Transaction> txn(fixture.begin());
	txn->SetSnapshot();
	ASSERT_TRUE(txn->Put("a", "1").ok());
	fixture.base->Put(rocksdb::WriteOptions(), "b", "2");
	EXPECT_TRUE(fixture.validate(txn.get()).ok());
	fixture.expectAgreement(txn.get(), "unrelated key");
}

TEST(OptimisticValidation, FlushedHistoryAgreesWithCommit) {
	Fixture fixture;
	fixture.base->Put(rocksdb::WriteOptions(), "k", "v0");
	std::unique_ptr<rocksdb::Transaction> txn(fixture.begin());
	txn->SetSnapshot();
	ASSERT_TRUE(txn->Put("k", "v1").ok());
	fixture.base->Put(rocksdb::WriteOptions(), "z", "zz");
	ASSERT_TRUE(fixture.base->Flush(rocksdb::FlushOptions()).ok());
	fixture.expectAgreement(txn.get(), "after flush");
}

TEST(OptimisticValidation, SerialPolicyAgreesWithCommit) {
	Fixture fixture(rocksdb::OccValidationPolicy::kValidateSerial);
	fixture.base->Put(rocksdb::WriteOptions(), "k", "v0");
	std::unique_ptr<rocksdb::Transaction> txn(fixture.begin());
	txn->SetSnapshot();
	ASSERT_TRUE(txn->Put("k", "v1").ok());
	fixture.base->Put(rocksdb::WriteOptions(), "k", "other");
	EXPECT_TRUE(fixture.validate(txn.get()).IsBusy());
	fixture.expectAgreement(txn.get(), "serial policy");
}

TEST(OptimisticValidation, ValidationLeavesTheTransactionUsable) {
	Fixture fixture;
	std::unique_ptr<rocksdb::Transaction> txn(fixture.begin());
	ASSERT_TRUE(txn->Put("k", "v1").ok());
	EXPECT_TRUE(fixture.validate(txn.get()).ok());
	EXPECT_EQ(txn->GetNumPuts(), 1u);
	EXPECT_TRUE(txn->Commit().ok());
	std::string value;
	EXPECT_TRUE(fixture.base->Get(rocksdb::ReadOptions(), "k", &value).ok());
	EXPECT_EQ(value, "v1");
}

// A logged optimistic handle lands its batch with a plain `Write()` instead of `Commit()`
// (transaction.cpp `landTransaction`). That rests on four RocksDB facts, pinned here under both
// OCC policies so an upgrade that changes one fails this test rather than a production commit.
class DirectWriteLanding : public ::testing::TestWithParam<rocksdb::OccValidationPolicy> {};

TEST_P(DirectWriteLanding, LandsABatchThatCommitRefusesAndLeavesNothingBehind) {
	Fixture fixture(GetParam());
	fixture.base->Put(rocksdb::WriteOptions(), "k", "v0");
	std::unique_ptr<rocksdb::Transaction> txn(fixture.begin());
	txn->SetSnapshot();
	ASSERT_TRUE(txn->Put("k", "mine").ok());
	ASSERT_TRUE(txn->Put(fixture.other, "o", "mine-other").ok());
	fixture.base->Put(rocksdb::WriteOptions(), "k", "theirs");

	// A failed Commit() leaves the batch intact: nothing landed and nothing was cleared.
	EXPECT_TRUE(txn->Commit().IsBusy());
	EXPECT_EQ(txn->GetNumPuts(), 2u);
	std::string value;
	ASSERT_TRUE(fixture.base->Get(rocksdb::ReadOptions(), "k", &value).ok());
	EXPECT_EQ(value, "theirs");

	// The plain write has no tracker to validate against, so it lands every record in both families.
	rocksdb::Status written =
		fixture.odb->Write(*txn->GetWriteOptions(), txn->GetWriteBatch()->GetWriteBatch());
	ASSERT_TRUE(written.ok()) << written.ToString();
	ASSERT_TRUE(txn->Rollback().ok());
	ASSERT_TRUE(fixture.base->Get(rocksdb::ReadOptions(), "k", &value).ok());
	EXPECT_EQ(value, "mine");
	ASSERT_TRUE(fixture.base->Get(rocksdb::ReadOptions(), fixture.other, "o", &value).ok());
	EXPECT_EQ(value, "mine-other");
	EXPECT_EQ(txn->GetNumPuts(), 0u);
}

TEST_P(DirectWriteLanding, RefusesARangeDeletionBeforeAnythingLands) {
	Fixture fixture(GetParam());
	std::unique_ptr<rocksdb::Transaction> txn(fixture.begin());
	ASSERT_TRUE(txn->Put("k", "v1").ok());
	rocksdb::WriteBatch batch;
	ASSERT_TRUE(batch.Put("k", "v2").ok());
	ASSERT_TRUE(batch.DeleteRange("a", "z").ok());
	EXPECT_TRUE(fixture.odb->Write(rocksdb::WriteOptions(), &batch).IsNotSupported());
	std::string value;
	EXPECT_TRUE(fixture.base->Get(rocksdb::ReadOptions(), "k", &value).IsNotFound());
}

INSTANTIATE_TEST_SUITE_P(
	Policies,
	DirectWriteLanding,
	::testing::Values(rocksdb::OccValidationPolicy::kValidateParallel, rocksdb::OccValidationPolicy::kValidateSerial)
);

TEST(OptimisticValidation, CommitKeyHashesCoverEveryFamilyOnceAndFailClosedOnRanges) {
	Fixture fixture;
	std::unique_ptr<rocksdb::Transaction> txn(fixture.begin());
	ASSERT_TRUE(txn->Put("k", "1").ok());
	ASSERT_TRUE(txn->Delete(fixture.other, "k").ok());
	ASSERT_TRUE(txn->Put("k", "2").ok());
	std::vector<uint64_t> hashes;
	ASSERT_TRUE(collectCommitKeyHashes(*txn->GetWriteBatch()->GetWriteBatch(), hashes).ok());
	EXPECT_EQ(hashes.size(), 2u) << "same key in two families reserves two entries; a duplicate reserves one";
	EXPECT_NE(commitKeyHash(0, "k"), commitKeyHash(fixture.other->GetID(), "k"));
	EXPECT_TRUE(std::is_sorted(hashes.begin(), hashes.end()));

	rocksdb::WriteBatch ranged;
	ASSERT_TRUE(ranged.DeleteRange("a", "z").ok());
	EXPECT_TRUE(collectCommitKeyHashes(ranged, hashes).IsNotSupported());
}

