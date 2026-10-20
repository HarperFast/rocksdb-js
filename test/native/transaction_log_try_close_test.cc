#include <gtest/gtest.h>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include "transaction_log/transaction_log_entry.h"
#include "transaction_log/transaction_log_store.h"

namespace {

std::filesystem::path uniqueTryClosePath() {
	auto nonce = std::chrono::steady_clock::now().time_since_epoch().count();
	return std::filesystem::temp_directory_path() /
		("rocksdb-js-try-close-" + std::to_string(nonce)) / "store";
}

std::unique_ptr<rocksdb_js::TransactionLogStore> openStore(const std::filesystem::path& storePath) {
	return std::make_unique<rocksdb_js::TransactionLogStore>(
		"store", storePath, 0, std::chrono::milliseconds(0), 0);
}

// Mirrors TransactionHandle::close() for a transaction that bound but never wrote.
void unbindWithoutWrite(rocksdb_js::TransactionLogStore& store) {
	std::lock_guard<std::mutex> lock(store.transactionBindMutex);
	if (!store.isClosing.load()) {
		store.pendingTransactionCount--;
	}
}

rocksdb_js::LogPosition writeUncommitted(rocksdb_js::TransactionLogStore& store) {
	std::string payload = "not yet committed";
	rocksdb_js::TransactionLogEntryBatch batch(1770000000001.0);
	batch.addEntry(std::make_unique<rocksdb_js::TransactionLogEntry>(
		nullptr, payload.data(), static_cast<uint32_t>(payload.size())));
	rocksdb_js::LogPosition position;
	store.writeBatch(batch, position);
	return position;
}

void commit(rocksdb_js::TransactionLogStore& store, rocksdb_js::LogPosition position) {
	store.commitFinished(position, []() -> rocksdb::SequenceNumber { return 1; });
}

size_t realUncommittedCount(rocksdb_js::TransactionLogStore& store) {
	std::lock_guard<std::mutex> lock(store.dataSetsMutex);
	size_t count = 0;
	for (const auto& pos : store.uncommittedTransactionPositions) {
		if (pos.logSequenceNumber != store.nextLogPosition.logSequenceNumber ||
			pos.positionInLogFile != store.nextLogPosition.positionInLogFile) {
			++count;
		}
	}
	return count;
}

} // namespace

TEST(TransactionLogTryClose, RefusesWhileBoundTransactionHasNotWritten) {
	auto storePath = uniqueTryClosePath();
	auto store = openStore(storePath);

	ASSERT_TRUE(store->tryBindTransaction());
	EXPECT_FALSE(store->tryClose());
	EXPECT_FALSE(store->isClosing.load());

	unbindWithoutWrite(*store);
	EXPECT_TRUE(store->tryClose());

	store.reset();
	std::filesystem::remove_all(storePath.parent_path());
}

TEST(TransactionLogTryClose, RefusesWhileWrittenTransactionIsUncommitted) {
	auto storePath = uniqueTryClosePath();
	auto store = openStore(storePath);

	ASSERT_TRUE(store->tryBindTransaction());
	auto position = writeUncommitted(*store);
	ASSERT_EQ(store->pendingTransactionCount.load(), 0);
	ASSERT_EQ(realUncommittedCount(*store), 1u);

	EXPECT_FALSE(store->tryClose());
	EXPECT_FALSE(store->isClosing.load());

	commit(*store, position);
	EXPECT_TRUE(store->tryClose());

	store.reset();
	std::filesystem::remove_all(storePath.parent_path());
}

// #860: a transaction that binds and finishes writeBatch() between the
// uncommitted-position check and the isClosing latch moves entirely through
// pendingTransactionCount, so neither check sees it.
TEST(TransactionLogTryClose, RefusesBindAndWriteBetweenPositionCheckAndClosingLatch) {
	auto storePath = uniqueTryClosePath();
	auto store = openStore(storePath);

	rocksdb_js::LogPosition position;
	bool raced = false;
	store->tryCloseBeforeClosingLatchForTests = [&]() {
		if (raced) return;
		raced = true;
		ASSERT_TRUE(store->tryBindTransaction());
		position = writeUncommitted(*store);
	};

	bool closed = store->tryClose();
	ASSERT_TRUE(raced);
	ASSERT_EQ(store->pendingTransactionCount.load(), 0);

	EXPECT_FALSE(closed) << "tryClose() closed the store with a written, uncommitted transaction";
	EXPECT_FALSE(store->isClosing.load());
	EXPECT_EQ(realUncommittedCount(*store), 1u);
	EXPECT_GT(store->getLogFileSize(position.logSequenceNumber), position.positionInLogFile);

	if (!closed) {
		commit(*store, position);
		EXPECT_TRUE(store->tryClose());
	}

	store.reset();
	std::filesystem::remove_all(storePath.parent_path());
}

// The generation check cannot tell an abandoned bind from a written one, so it
// refuses conservatively; the next attempt closes.
TEST(TransactionLogTryClose, RefusesBindAbandonedDuringCheckThenClosesOnRetry) {
	auto storePath = uniqueTryClosePath();
	auto store = openStore(storePath);

	bool raced = false;
	store->tryCloseBeforeClosingLatchForTests = [&]() {
		if (raced) return;
		raced = true;
		ASSERT_TRUE(store->tryBindTransaction());
		unbindWithoutWrite(*store);
	};

	EXPECT_FALSE(store->tryClose());
	ASSERT_TRUE(raced);
	EXPECT_FALSE(store->isClosing.load());
	EXPECT_EQ(store->pendingTransactionCount.load(), 0);

	EXPECT_TRUE(store->tryClose());

	store.reset();
	std::filesystem::remove_all(storePath.parent_path());
}

TEST(TransactionLogTryClose, BindAfterCloseIsRefusedWithoutChangingAccounting) {
	auto storePath = uniqueTryClosePath();
	auto store = openStore(storePath);

	ASSERT_TRUE(store->tryClose());
	auto generation = store->transactionBindGeneration;

	EXPECT_FALSE(store->tryBindTransaction());
	EXPECT_EQ(store->pendingTransactionCount.load(), 0);
	EXPECT_EQ(store->transactionBindGeneration, generation);

	store.reset();
	std::filesystem::remove_all(storePath.parent_path());
}
