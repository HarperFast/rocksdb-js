// Pins the RocksDB behavior that transaction iterator reuse depends on: an
// iterator from Transaction::GetIterator (a BaseDeltaIterator over the
// transaction's WriteBatchWithIndex) observes writes staged in the transaction
// AFTER the iterator was created once it is re-positioned with a Seek, and
// honors read bounds rewritten in place before that Seek. The pooled iterators
// in TransactionHandle rely on both, so a fresh iterator and a reused one return
// the same rows. Must stay green on every RocksDB upgrade.
#include <gtest/gtest.h>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
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

namespace {

// A pooled iterator keeps its RocksDB read bounds and is retargeted by
// rewriting the Slices those bounds point to, then re-seeking. RocksDB re-reads
// the bound pointers on every seek (DBIteratorTest.IterateBoundChangedBeforeSeek
// upstream); this checks the transaction iterator does too, against a fresh
// bounded iterator, over data spread across blocks, files and levels.
void checkRetargetedBoundsMatchFresh(rocksdb::DB* db, rocksdb::Transaction* txn) {
	txn->SetSnapshot();
	rocksdb::ReadOptions base;
	base.snapshot = txn->GetSnapshot();
	// Staged writes: new keys, overwrites and deletes of committed keys.
	for (int i = 0; i < 600; i += 7) {
		ASSERT_TRUE(txn->Put("k" + std::to_string(10000 + i) + "s", "staged").ok());
	}
	for (int i = 3; i < 600; i += 11) {
		ASSERT_TRUE(txn->Delete("k" + std::to_string(10000 + i)).ok());
	}

	std::string lowerStr, upperStr;
	rocksdb::Slice lower, upper;
	rocksdb::ReadOptions pooledOptions = base;
	pooledOptions.iterate_lower_bound = &lower;
	pooledOptions.iterate_upper_bound = &upper;
	lowerStr = "k10000";
	upperStr = "k10001";
	lower = rocksdb::Slice(lowerStr);
	upper = rocksdb::Slice(upperStr);
	std::unique_ptr<rocksdb::Iterator> pooled(txn->GetIterator(pooledOptions));

	auto collect = [](rocksdb::Iterator* it, bool reverse, const rocksdb::Slice& lo, const rocksdb::Slice& hi) {
		std::vector<std::string> rows;
		if (reverse) {
			it->SeekForPrev(hi);
			if (it->Valid() && it->key().compare(hi) == 0) {
				it->Prev();
			}
		} else {
			it->Seek(lo);
		}
		for (; it->Valid(); reverse ? it->Prev() : it->Next()) {
			rows.push_back(it->key().ToString() + "=" + it->value().ToString());
		}
		EXPECT_TRUE(it->status().ok()) << it->status().ToString();
		return rows;
	};

	uint32_t seed = 12345;
	auto nextRandom = [&seed](uint32_t bound) {
		seed = seed * 1103515245u + 12345u;
		return (seed >> 8) % bound;
	};
	for (int round = 0; round < 400; round++) {
		// Ranges from one key to the whole keyspace, so bounds grow and shrink
		// across rounds, including right after an exhausted scan.
		const uint32_t start = nextRandom(620);
		const uint32_t width = round % 5 == 0 ? nextRandom(620) : nextRandom(4) + 1;
		lowerStr = "k" + std::to_string(10000 + start);
		upperStr = "k" + std::to_string(10000 + start + width);
		lower = rocksdb::Slice(lowerStr);
		upper = rocksdb::Slice(upperStr);
		const bool reverse = nextRandom(2) == 1;

		rocksdb::ReadOptions freshOptions = base;
		rocksdb::Slice freshLower(lowerStr), freshUpper(upperStr);
		freshOptions.iterate_lower_bound = &freshLower;
		freshOptions.iterate_upper_bound = &freshUpper;
		std::unique_ptr<rocksdb::Iterator> fresh(txn->GetIterator(freshOptions));
		ASSERT_EQ(collect(pooled.get(), reverse, lower, upper), collect(fresh.get(), reverse, freshLower, freshUpper))
			<< "round " << round << " [" << lowerStr << ", " << upperStr << ") reverse=" << reverse;
		if (round == 200) {
			// More staged writes mid-way, as a transaction interleaving reads and writes would.
			for (int i = 1; i < 600; i += 13) {
				ASSERT_TRUE(txn->Put("k" + std::to_string(10000 + i), "restaged").ok());
			}
		}
	}
}

void populateLayered(rocksdb::DB* db) {
	// Older half compacted to the bottom level, newer half in two L0 files,
	// with overwrites and deletes across them.
	// Two overlapping flushes, so the compaction rewrites (and cuts into
	// several files) instead of trivially moving one file down.
	for (int pass = 0; pass < 2; pass++) {
		for (int i = 0; i < 600; i++) {
			ASSERT_TRUE(db->Put({}, "k" + std::to_string(10000 + i), std::string(40, 'a' + pass)).ok());
		}
		ASSERT_TRUE(db->Flush({}).ok());
	}
	ASSERT_TRUE(db->CompactRange({}, nullptr, nullptr).ok());
	for (int i = 0; i < 600; i += 2) {
		ASSERT_TRUE(db->Put({}, "k" + std::to_string(10000 + i), std::string(40, 'c')).ok());
	}
	ASSERT_TRUE(db->Flush({}).ok());
	for (int i = 0; i < 600; i += 5) {
		ASSERT_TRUE(db->Delete({}, "k" + std::to_string(10000 + i)).ok());
	}
	ASSERT_TRUE(db->Flush({}).ok());
	for (int i = 1; i < 600; i += 9) {
		ASSERT_TRUE(db->Put({}, "k" + std::to_string(10000 + i), "memtable").ok());
	}
}

rocksdb::Options layeredOptions() {
	rocksdb::Options options;
	options.create_if_missing = true;
	options.target_file_size_base = 4 * 1024;
	options.disable_auto_compactions = true;
	options.compression = rocksdb::kNoCompression;
	rocksdb::BlockBasedTableOptions table;
	table.block_size = 256;
	options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table));
	return options;
}

} // namespace

TEST(TransactionIteratorReuse, OptimisticRetargetedBoundsMatchFreshIterator) {
	auto root = testRoot("retarget-optimistic");
	rocksdb::OptimisticTransactionDB* rawDb = nullptr;
	rocksdb::Status s = rocksdb::OptimisticTransactionDB::Open(layeredOptions(), (root / "db").string(), &rawDb);
	ASSERT_TRUE(s.ok()) << s.ToString();
	std::unique_ptr<rocksdb::OptimisticTransactionDB> db(rawDb);
	populateLayered(db.get());
	// Several files below L0 plus L0 files, so ranges cross file and level boundaries.
	int lowerLevelFiles = 0;
	for (int level = 1; level < 7; level++) {
		std::string files;
		ASSERT_TRUE(db->GetProperty("rocksdb.num-files-at-level" + std::to_string(level), &files));
		lowerLevelFiles += std::stoi(files);
	}
	std::string l0Files;
	ASSERT_TRUE(db->GetProperty("rocksdb.num-files-at-level0", &l0Files));
	std::string levelStats;
	db->GetProperty("rocksdb.levelstats", &levelStats);
	EXPECT_GT(lowerLevelFiles, 1) << levelStats;
	EXPECT_EQ(std::stoi(l0Files), 2);
	std::unique_ptr<rocksdb::Transaction> txn(db->BeginTransaction({}));
	checkRetargetedBoundsMatchFresh(db.get(), txn.get());
	ASSERT_TRUE(txn->Rollback().ok());
	txn.reset();
	db.reset();
	std::filesystem::remove_all(root);
}

TEST(TransactionIteratorReuse, PessimisticRetargetedBoundsMatchFreshIterator) {
	auto root = testRoot("retarget-pessimistic");
	rocksdb::TransactionDB* rawDb = nullptr;
	rocksdb::Status s = rocksdb::TransactionDB::Open(layeredOptions(), {}, (root / "db").string(), &rawDb);
	ASSERT_TRUE(s.ok()) << s.ToString();
	std::unique_ptr<rocksdb::TransactionDB> db(rawDb);
	populateLayered(db.get());
	std::unique_ptr<rocksdb::Transaction> txn(db->BeginTransaction({}));
	checkRetargetedBoundsMatchFresh(db.get(), txn.get());
	ASSERT_TRUE(txn->Rollback().ok());
	txn.reset();
	db.reset();
	std::filesystem::remove_all(root);
}
