#include <gtest/gtest.h>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include "transaction_log/transaction_log_entry.h"
#include "transaction_log/transaction_log_store.h"

namespace rocksdb_js {

struct TransactionLogStoreTestPeer {
	static void writeFlushedPosition(
		TransactionLogStore& store,
		LogPosition position,
		uint64_t generation
	) {
		store.writeFlushedPosition(position, generation);
	}
};

} // namespace rocksdb_js

namespace {

std::filesystem::path uniqueFlushedStatePath() {
	auto nonce = std::chrono::steady_clock::now().time_since_epoch().count();
	return std::filesystem::temp_directory_path() /
		("rocksdb-js-flushed-state-" + std::to_string(nonce)) / "store";
}

void expectFlushedPosition(
	rocksdb_js::TransactionLogStore& store,
	uint32_t positionInLogFile,
	uint32_t logSequenceNumber
) {
	auto flushed = store.getLastFlushedPosition();
	EXPECT_EQ(flushed.logSequenceNumber, logSequenceNumber);
	EXPECT_EQ(flushed.positionInLogFile, positionInLogFile);
}

} // namespace

TEST(TransactionLogFlushedState, DestructivePurgeInvalidatesOldFlushCorrelations) {
	auto storePath = uniqueFlushedStatePath();
	auto statePath = storePath / "txn.state";
	auto store = std::make_shared<rocksdb_js::TransactionLogStore>(
		"foo", storePath, 0, std::chrono::milliseconds(0), 0);

	std::string payload = "entry";
	rocksdb_js::TransactionLogEntryBatch batch(1001.0);
	batch.addEntry(std::make_unique<rocksdb_js::TransactionLogEntry>(
		nullptr, payload.data(), static_cast<uint32_t>(payload.size())));
	rocksdb_js::LogPosition position;
	store->writeBatch(batch, position);
	store->commitFinished(position, 10);
	store->databaseFlushed(10);
	ASSERT_TRUE(std::filesystem::exists(statePath));

	store->purge(nullptr, /*all=*/true, /*before=*/0);
	ASSERT_FALSE(std::filesystem::exists(storePath));

	// The same RocksDB flush watermark must not republish a position from the
	// segment generation that destroy just removed.
	store->databaseFlushed(10);
	EXPECT_FALSE(std::filesystem::exists(statePath));

	store->close();
	std::filesystem::remove_all(storePath.parent_path());
}

TEST(TransactionLogFlushedState, RejectsFlushSelectedBeforeGenerationChange) {
	auto storePath = uniqueFlushedStatePath();
	auto statePath = storePath / "txn.state";
	auto store = std::make_shared<rocksdb_js::TransactionLogStore>(
		"foo", storePath, 0, std::chrono::milliseconds(0), 0);

	auto observedGeneration = store->flushedStateGeneration.load(std::memory_order_relaxed);
	store->flushedStateGeneration.fetch_add(1, std::memory_order_relaxed);
	rocksdb_js::TransactionLogStoreTestPeer::writeFlushedPosition(
		*store, rocksdb_js::LogPosition(100, 1), observedGeneration);

	EXPECT_FALSE(std::filesystem::exists(statePath));
	EXPECT_EQ(store->databaseFlushes.load(std::memory_order_relaxed), 0u);

	store->close();
	std::filesystem::remove_all(storePath.parent_path());
}

// Regression coverage for a writer that retained txn.state's descriptor across
// callbacks: unlinking the pathname left later writes in an orphaned inode.
#ifndef _WIN32
// Windows keeps an open CRT stream's file undeletable, so pathname loss under an
// open stream cannot be staged there.
TEST(TransactionLogFlushedState, RewritesStateFileAfterItIsUnlinked) {
	auto storePath = uniqueFlushedStatePath();
	std::filesystem::create_directories(storePath);
	auto statePath = storePath / "txn.state";

	{
		auto store = std::make_shared<rocksdb_js::TransactionLogStore>(
			"foo", storePath, 0, std::chrono::milliseconds(0), 0);

		store->recentlyCommittedSequencePositions[0] = { 10, rocksdb_js::LogPosition(100, 1) };
		store->databaseFlushed(10);
		ASSERT_TRUE(std::filesystem::exists(statePath));
		expectFlushedPosition(*store, 100, 1);

		std::filesystem::remove(statePath);
		store->recentlyCommittedSequencePositions[1] = { 20, rocksdb_js::LogPosition(200, 2) };
		store->databaseFlushed(20);
		EXPECT_TRUE(std::filesystem::exists(statePath));
		expectFlushedPosition(*store, 200, 2);

		std::filesystem::remove_all(storePath);
		ASSERT_FALSE(std::filesystem::exists(storePath));
		store->recentlyCommittedSequencePositions[2] = { 30, rocksdb_js::LogPosition(300, 3) };
		store->databaseFlushed(30);
		EXPECT_TRUE(std::filesystem::exists(statePath));
		expectFlushedPosition(*store, 300, 3);

		store->close();
	}

	std::filesystem::remove_all(storePath.parent_path());
}
#endif

#ifndef _WIN32
TEST(TransactionLogFlushedState, ReopensReplacedStateFileByPathname) {
	auto storePath = uniqueFlushedStatePath();
	std::filesystem::create_directories(storePath);
	auto statePath = storePath / "txn.state";
	auto displacedPath = storePath / "txn.state.displaced";

	{
		auto store = std::make_shared<rocksdb_js::TransactionLogStore>(
			"foo", storePath, 0, std::chrono::milliseconds(0), 0);
		store->recentlyCommittedSequencePositions[0] = { 10, rocksdb_js::LogPosition(100, 1) };
		store->databaseFlushed(10);
		expectFlushedPosition(*store, 100, 1);

		// Move the pathname away while the old implementation still has its stream
		// open, then install a stale replacement at the original name.
		std::filesystem::rename(statePath, displacedPath);
		rocksdb_js::LogPosition stale(50, 1);
		std::ofstream replacement(statePath, std::ios::binary | std::ios::trunc);
		replacement.write(reinterpret_cast<const char*>(&stale), sizeof(stale));
		replacement.close();

		store->recentlyCommittedSequencePositions[1] = { 20, rocksdb_js::LogPosition(200, 2) };
		store->databaseFlushed(20);
		expectFlushedPosition(*store, 200, 2);

		store->close();
	}

	std::filesystem::remove_all(storePath.parent_path());
}
#endif

#ifndef _WIN32
// The pathname check must run before the unchanged-position shortcut: a flush
// that resolves to the position already recorded still has to restore the file.
TEST(TransactionLogFlushedState, RewritesUnchangedPositionAfterFileIsUnlinked) {
	auto storePath = uniqueFlushedStatePath();
	std::filesystem::create_directories(storePath);
	auto statePath = storePath / "txn.state";

	{
		auto store = std::make_shared<rocksdb_js::TransactionLogStore>(
			"foo", storePath, 0, std::chrono::milliseconds(0), 0);
		store->recentlyCommittedSequencePositions[0] = { 10, rocksdb_js::LogPosition(100, 1) };
		store->databaseFlushed(10);
		ASSERT_TRUE(std::filesystem::exists(statePath));

		std::filesystem::remove(statePath);
		store->databaseFlushed(10);
		EXPECT_TRUE(std::filesystem::exists(statePath));
		expectFlushedPosition(*store, 100, 1);

		auto emptyStore = std::make_shared<rocksdb_js::TransactionLogStore>(
			"bar", storePath.parent_path() / "bar", 0, std::chrono::milliseconds(0), 0);
		emptyStore->databaseFlushed(10);
		EXPECT_FALSE(std::filesystem::exists(storePath.parent_path() / "bar"));

		store->close();
		emptyStore->close();
	}

	std::filesystem::remove_all(storePath.parent_path());
}
#endif

#ifndef _WIN32
// A pathname that cannot be checked is not "absent": the write must be left
// unrecorded so the next flush retries, rather than trusting the open stream.
TEST(TransactionLogFlushedState, LeavesPositionUnrecordedWhenPathCannotBeChecked) {
	auto storePath = uniqueFlushedStatePath();
	std::filesystem::create_directories(storePath);

	{
		auto store = std::make_shared<rocksdb_js::TransactionLogStore>(
			"foo", storePath, 0, std::chrono::milliseconds(0), 0);
		store->recentlyCommittedSequencePositions[0] = { 10, rocksdb_js::LogPosition(100, 1) };
		store->databaseFlushed(10);
		ASSERT_EQ(store->databaseFlushes.load(), 1u);

		// no search permission: exists() on a child reports an error, not absence
		std::filesystem::permissions(storePath, std::filesystem::perms::owner_read);
		std::error_code probe;
		(void) std::filesystem::exists(storePath / "txn.state", probe);
		if (!probe) {
			std::filesystem::permissions(storePath, std::filesystem::perms::owner_all);
			store->close();
			std::filesystem::remove_all(storePath.parent_path());
			GTEST_SKIP() << "directory permissions are not enforced for this user";
		}

		store->recentlyCommittedSequencePositions[1] = { 20, rocksdb_js::LogPosition(200, 2) };
		EXPECT_NO_THROW(store->databaseFlushed(20));
		EXPECT_EQ(store->databaseFlushes.load(), 1u);

		std::filesystem::permissions(storePath, std::filesystem::perms::owner_all);
		store->databaseFlushed(20);
		EXPECT_EQ(store->databaseFlushes.load(), 2u);
		expectFlushedPosition(*store, 200, 2);

		store->close();
	}

	std::filesystem::remove_all(storePath.parent_path());
}
#endif

#ifndef _WIN32
// A flush callback that cannot restore the file must report and retry later,
// never throw out of RocksDB's background thread.
TEST(TransactionLogFlushedState, RetriesAfterDirectoryCannotBeRecreated) {
	auto storePath = uniqueFlushedStatePath();
	std::filesystem::create_directories(storePath);
	auto statePath = storePath / "txn.state";
	auto parent = storePath.parent_path();

	{
		auto store = std::make_shared<rocksdb_js::TransactionLogStore>(
			"foo", storePath, 0, std::chrono::milliseconds(0), 0);
		store->recentlyCommittedSequencePositions[0] = { 10, rocksdb_js::LogPosition(100, 1) };
		store->databaseFlushed(10);
		ASSERT_TRUE(std::filesystem::exists(statePath));

		std::filesystem::remove_all(storePath);
		std::filesystem::permissions(parent, std::filesystem::perms::owner_read | std::filesystem::perms::owner_exec);
		std::error_code probe;
		std::filesystem::create_directory(parent / "probe", probe);
		if (!probe) {
			// running as a user the permission bits do not bind (root); nothing to inject
			std::filesystem::permissions(parent, std::filesystem::perms::owner_all);
			store->close();
			std::filesystem::remove_all(parent);
			GTEST_SKIP() << "directory permissions are not enforced for this user";
		}

		store->recentlyCommittedSequencePositions[1] = { 20, rocksdb_js::LogPosition(200, 2) };
		EXPECT_NO_THROW(store->databaseFlushed(20));
		EXPECT_FALSE(std::filesystem::exists(statePath));
		EXPECT_EQ(store->databaseFlushes.load(), 1u);

		std::filesystem::permissions(parent, std::filesystem::perms::owner_all);
		store->databaseFlushed(20);
		EXPECT_TRUE(std::filesystem::exists(statePath));
		expectFlushedPosition(*store, 200, 2);
		EXPECT_EQ(store->databaseFlushes.load(), 2u);

		store->close();
	}

	std::filesystem::remove_all(parent);
}
#endif
