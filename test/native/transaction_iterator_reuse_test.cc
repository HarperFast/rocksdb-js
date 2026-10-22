// Pins the RocksDB behavior that transaction iterator reuse depends on: an
// iterator from Transaction::GetIterator (a BaseDeltaIterator over the
// transaction's WriteBatchWithIndex) observes writes staged in the transaction
// AFTER the iterator was created, once it is re-positioned with a Seek. The
// pooled iterators in TransactionHandle rely on this, so a fresh iterator and a
// reused one return the same rows. Must stay green on every RocksDB upgrade.
#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"

namespace {

std::filesystem::path testRoot(const char* name) {
	auto root = std::filesystem::temp_directory_path() / "rocksdb-js-txn-iterator-reuse" / name;
	std::filesystem::remove_all(root);
	std::filesystem::create_directories(root);
	return root;
}

std::vector<std::string> scan(rocksdb::Iterator* it, const std::string& start, const std::string& end) {
	std::vector<std::string> rows;
	for (it->Seek(start); it->Valid() && it->key().compare(end) < 0; it->Next()) {
		rows.push_back(it->key().ToString() + "=" + it->value().ToString());
	}
	EXPECT_TRUE(it->status().ok()) << it->status().ToString();
	return rows;
}

std::vector<std::string> scanReverse(rocksdb::Iterator* it, const std::string& start, const std::string& end) {
	std::vector<std::string> rows;
	it->SeekForPrev(end);
	if (it->Valid() && it->key().compare(end) == 0) {
		it->Prev();
	}
	for (; it->Valid() && it->key().compare(start) >= 0; it->Prev()) {
		rows.push_back(it->key().ToString() + "=" + it->value().ToString());
	}
	EXPECT_TRUE(it->status().ok()) << it->status().ToString();
	return rows;
}

// Stages writes after the iterator exists and checks each reseek against a
// freshly created iterator over the same transaction and snapshot.
void checkLaterWritesVisible(rocksdb::DB* db, rocksdb::Transaction* txn) {
	txn->SetSnapshot();
	rocksdb::ReadOptions readOptions;
	readOptions.snapshot = txn->GetSnapshot();
	// Committed after the snapshot: neither iterator may see it.
	ASSERT_TRUE(db->Put({}, "b2", "late-commit").ok());

	std::unique_ptr<rocksdb::Iterator> pooled(txn->GetIterator(readOptions));
	EXPECT_EQ(scan(pooled.get(), "a", "z"),
		(std::vector<std::string>{ "b1=committed", "c1=committed" }));

	auto expectSameAsFresh = [&](std::vector<std::string> expected) {
		std::unique_ptr<rocksdb::Iterator> fresh(txn->GetIterator(readOptions));
		EXPECT_EQ(scan(fresh.get(), "a", "z"), expected);
		EXPECT_EQ(scan(pooled.get(), "a", "z"), expected);
		std::vector<std::string> reversed(expected.rbegin(), expected.rend());
		EXPECT_EQ(scanReverse(pooled.get(), "a", "z"), reversed);
	};

	// New key in front of, between and after the committed keys.
	ASSERT_TRUE(txn->Put("a1", "staged").ok());
	ASSERT_TRUE(txn->Put("b5", "staged").ok());
	ASSERT_TRUE(txn->Put("d1", "staged").ok());
	expectSameAsFresh({ "a1=staged", "b1=committed", "b5=staged", "c1=committed", "d1=staged" });

	// Overwrite a committed key and a staged key, then delete each kind.
	ASSERT_TRUE(txn->Put("b1", "overwritten").ok());
	ASSERT_TRUE(txn->Put("b5", "restaged").ok());
	expectSameAsFresh({ "a1=staged", "b1=overwritten", "b5=restaged", "c1=committed", "d1=staged" });
	ASSERT_TRUE(txn->Delete("c1").ok());
	ASSERT_TRUE(txn->Delete("a1").ok());
	expectSameAsFresh({ "b1=overwritten", "b5=restaged", "d1=staged" });

	// Many writes, enough to grow the batch's buffer several times over.
	for (int i = 0; i < 2000; i++) {
		ASSERT_TRUE(txn->Put("e" + std::to_string(10000 + i), std::string(100, 'x')).ok());
	}
	std::unique_ptr<rocksdb::Iterator> fresh(txn->GetIterator(readOptions));
	size_t freshCount = scan(fresh.get(), "a", "z").size();
	EXPECT_EQ(freshCount, 2003u);
	EXPECT_EQ(scan(pooled.get(), "a", "z").size(), freshCount);
	EXPECT_EQ(scan(pooled.get(), "e10500", "e10502"),
		(std::vector<std::string>{ "e10500=" + std::string(100, 'x'), "e10501=" + std::string(100, 'x') }));
}

} // namespace

TEST(TransactionIteratorReuse, OptimisticIteratorSeesLaterWritesAfterReseek) {
	auto root = testRoot("optimistic");
	rocksdb::Options options;
	options.create_if_missing = true;
	rocksdb::OptimisticTransactionDB* rawDb = nullptr;
	rocksdb::Status s = rocksdb::OptimisticTransactionDB::Open(options, (root / "db").string(), &rawDb);
	ASSERT_TRUE(s.ok()) << s.ToString();
	std::unique_ptr<rocksdb::OptimisticTransactionDB> db(rawDb);
	ASSERT_TRUE(db->Put({}, "b1", "committed").ok());
	ASSERT_TRUE(db->Put({}, "c1", "committed").ok());

	std::unique_ptr<rocksdb::Transaction> txn(db->BeginTransaction({}));
	checkLaterWritesVisible(db.get(), txn.get());
	ASSERT_TRUE(txn->Rollback().ok());
	txn.reset();
	db.reset();
	std::filesystem::remove_all(root);
}

TEST(TransactionIteratorReuse, PessimisticIteratorSeesLaterWritesAfterReseek) {
	auto root = testRoot("pessimistic");
	rocksdb::Options options;
	options.create_if_missing = true;
	rocksdb::TransactionDB* rawDb = nullptr;
	rocksdb::Status s = rocksdb::TransactionDB::Open(options, {}, (root / "db").string(), &rawDb);
	ASSERT_TRUE(s.ok()) << s.ToString();
	std::unique_ptr<rocksdb::TransactionDB> db(rawDb);
	ASSERT_TRUE(db->Put({}, "b1", "committed").ok());
	ASSERT_TRUE(db->Put({}, "c1", "committed").ok());

	std::unique_ptr<rocksdb::Transaction> txn(db->BeginTransaction({}));
	checkLaterWritesVisible(db.get(), txn.get());
	ASSERT_TRUE(txn->Rollback().ok());
	txn.reset();
	db.reset();
	std::filesystem::remove_all(root);
}

// A pooled iterator has no RocksDB bounds, so running past the end of a range
// can mean skipping every deletion tombstone up to the next live key. The pool
// caps that with `max_skippable_internal_keys` and falls back to a bounded
// iterator on `Incomplete`, which is only correct if the transaction iterator
// fails closed there rather than silently yielding the staged keys beyond.
TEST(TransactionIteratorReuse, SkipLimitFailsClosedThroughTransactionIterator) {
	auto root = testRoot("skip-limit");
	rocksdb::Options options;
	options.create_if_missing = true;
	rocksdb::OptimisticTransactionDB* rawDb = nullptr;
	rocksdb::Status s = rocksdb::OptimisticTransactionDB::Open(options, (root / "db").string(), &rawDb);
	ASSERT_TRUE(s.ok()) << s.ToString();
	std::unique_ptr<rocksdb::OptimisticTransactionDB> db(rawDb);
	ASSERT_TRUE(db->Put({}, "a", "live").ok());
	for (int i = 0; i < 1000; i++) {
		ASSERT_TRUE(db->Put({}, "m" + std::to_string(10000 + i), "x").ok());
	}
	for (int i = 0; i < 1000; i++) {
		ASSERT_TRUE(db->Delete({}, "m" + std::to_string(10000 + i)).ok());
	}
	ASSERT_TRUE(db->Put({}, "z", "live").ok());
	for (bool flush : { false, true }) {
		if (flush) {
			ASSERT_TRUE(db->Flush({}).ok());
		}
		std::unique_ptr<rocksdb::Transaction> txn(db->BeginTransaction({}));
		ASSERT_TRUE(txn->Put("b", "staged").ok());
		ASSERT_TRUE(txn->Put("n", "staged").ok());
		rocksdb::ReadOptions readOptions;
		readOptions.max_skippable_internal_keys = 16;
		std::unique_ptr<rocksdb::Iterator> it(txn->GetIterator(readOptions));

		it->Seek("a");
		ASSERT_TRUE(it->Valid());
		EXPECT_EQ(it->key().ToString(), "a");
		// The staged "b" comes next, but the base side has to skip the tombstones to
		// find its own next key first; the merged iterator must not yield "b" or "n".
		it->Next();
		EXPECT_FALSE(it->Valid());
		EXPECT_TRUE(it->status().IsIncomplete()) << it->status().ToString();
		it->Seek("b");
		EXPECT_FALSE(it->Valid());
		EXPECT_TRUE(it->status().IsIncomplete()) << it->status().ToString();
		// A reseek past the tombstones recovers.
		it->Seek("y");
		ASSERT_TRUE(it->Valid()) << it->status().ToString();
		EXPECT_EQ(it->key().ToString(), "z");
		// Reverse across the same run fails closed too.
		it->SeekForPrev("y");
		EXPECT_FALSE(it->Valid());
		EXPECT_TRUE(it->status().IsIncomplete()) << it->status().ToString();
		ASSERT_TRUE(txn->Rollback().ok());
	}
	db.reset();
	std::filesystem::remove_all(root);
}
