#include <gtest/gtest.h>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include "core/exception.h"
#include "transaction_log/transaction_log_entry.h"
#include "transaction_log/transaction_log_store.h"

namespace {

std::filesystem::path uniqueRetentionPath() {
	auto nonce = std::chrono::steady_clock::now().time_since_epoch().count();
	return std::filesystem::temp_directory_path() /
		("rocksdb-js-retention-" + std::to_string(nonce)) / "store";
}

uint64_t futureCutoffMs() {
	return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::system_clock::now().time_since_epoch()).count()) + 60000;
}

rocksdb_js::LogPosition writeAndFlush(rocksdb_js::TransactionLogStore& store, double timestamp, rocksdb::SequenceNumber sequence) {
	std::string payload = "entry";
	rocksdb_js::TransactionLogEntryBatch batch(timestamp);
	batch.addEntry(std::make_unique<rocksdb_js::TransactionLogEntry>(
		nullptr, payload.data(), static_cast<uint32_t>(payload.size())));
	rocksdb_js::LogPosition position;
	store.writeBatch(batch, position);
	store.commitFinished(position, [sequence]() -> rocksdb::SequenceNumber { return sequence; });
	store.databaseFlushed(sequence);
	return position;
}

void writeFlushedState(const std::filesystem::path& storePath, uint32_t positionInLogFile, uint32_t sequence) {
	std::filesystem::create_directories(storePath);
	rocksdb_js::LogPosition position(positionInLogFile, sequence);
	std::ofstream state(storePath / "txn.state", std::ios::binary | std::ios::trunc);
	state.write(reinterpret_cast<const char*>(&position), sizeof(position));
}

std::shared_ptr<rocksdb_js::TransactionLogStore> loadStore(const std::filesystem::path& storePath) {
	return rocksdb_js::TransactionLogStore::load(storePath, 0, std::chrono::milliseconds(0), 0, false);
}

} // namespace

TEST(TransactionLogRetention, PurgesFlushedCurrentSegmentAndAppendsPastIt) {
	auto storePath = uniqueRetentionPath();
	auto store = std::make_shared<rocksdb_js::TransactionLogStore>(
		"foo", storePath, 0, std::chrono::milliseconds(0), 0);

	writeAndFlush(*store, 1001.0, 10);
	ASSERT_EQ(store->currentSequenceNumber.load(), 1u);
	store->purge(nullptr, false, futureCutoffMs());
	EXPECT_FALSE(std::filesystem::exists(storePath / "1.txnlog"));
	EXPECT_TRUE(std::filesystem::exists(storePath / "txn.state"));
	EXPECT_EQ(store->currentSequenceNumber.load(), 2u);

	auto position = writeAndFlush(*store, 1002.0, 20);
	EXPECT_EQ(position.logSequenceNumber, 2u);
	EXPECT_TRUE(std::filesystem::exists(storePath / "2.txnlog"));

	store->close();
	std::filesystem::remove_all(storePath.parent_path());
}

// A backup copies segments after capturing txn.state; a purge in between would read a newer
// flushed position and could delete entries the captured one still needs replayed.
TEST(TransactionLogRetention, RetentionPinHoldsOffOrdinaryPurge) {
	auto storePath = uniqueRetentionPath();
	auto store = std::make_shared<rocksdb_js::TransactionLogStore>(
		"foo", storePath, 0, std::chrono::milliseconds(0), 0);

	writeAndFlush(*store, 1001.0, 10);
	auto pin = rocksdb_js::TransactionLogStore::pinRetention(store);
	store->purge(nullptr, false, futureCutoffMs());
	EXPECT_TRUE(std::filesystem::exists(storePath / "1.txnlog"));
	EXPECT_EQ(store->currentSequenceNumber.load(), 1u);

	pin.reset();
	store->purge(nullptr, false, futureCutoffMs());
	EXPECT_FALSE(std::filesystem::exists(storePath / "1.txnlog"));

	store->close();
	std::filesystem::remove_all(storePath.parent_path());
}

TEST(TransactionLogRetention, LoadStartsPastAPurgedFlushedSequence) {
	auto storePath = uniqueRetentionPath();
	writeFlushedState(storePath, 200, 5);

	auto store = loadStore(storePath);
	ASSERT_TRUE(store);
	EXPECT_EQ(store->currentSequenceNumber.load(), 6u);
	EXPECT_EQ(store->nextLogPosition.logSequenceNumber, 6u);
	EXPECT_EQ(store->lastCommittedPosition->logSequenceNumber, 6u);
	EXPECT_EQ(store->lastCommittedPosition->positionInLogFile, 0u);

	auto position = writeAndFlush(*store, 1001.0, 10);
	EXPECT_EQ(position.logSequenceNumber, 6u);
	EXPECT_TRUE(std::filesystem::exists(storePath / "6.txnlog"));

	store->close();
	std::filesystem::remove_all(storePath.parent_path());
}

// `{0, F}` says nothing in F was flushed, so F itself is still a valid place to append.
TEST(TransactionLogRetention, LoadUsesAnUnwrittenFlushedSequence) {
	auto storePath = uniqueRetentionPath();
	writeFlushedState(storePath, 0, 5);

	auto store = loadStore(storePath);
	ASSERT_TRUE(store);
	EXPECT_EQ(store->currentSequenceNumber.load(), 5u);
	EXPECT_EQ(store->nextLogPosition.logSequenceNumber, 5u);

	store->close();
	std::filesystem::remove_all(storePath.parent_path());
}

TEST(TransactionLogRetention, LoadRefusesAnUnreadableStateWithNoSegment) {
	auto storePath = uniqueRetentionPath();
	std::filesystem::create_directories(storePath);
	{
		std::ofstream state(storePath / "txn.state", std::ios::binary | std::ios::trunc);
		const char torn[5] = { 1, 0, 0, 0, 9 };
		state.write(torn, sizeof(torn));
	}

	EXPECT_THROW(loadStore(storePath), rocksdb_js::DBException);

	// a surviving segment still records the sequence, so the torn state is ignored
	auto writer = std::make_shared<rocksdb_js::TransactionLogStore>(
		"foo", storePath, 0, std::chrono::milliseconds(0), 0);
	std::string payload = "entry";
	rocksdb_js::TransactionLogEntryBatch batch(1001.0);
	batch.addEntry(std::make_unique<rocksdb_js::TransactionLogEntry>(
		nullptr, payload.data(), static_cast<uint32_t>(payload.size())));
	rocksdb_js::LogPosition position;
	writer->writeBatch(batch, position);
	writer->close();
	auto store = loadStore(storePath);
	ASSERT_TRUE(store);
	EXPECT_EQ(store->getLastFlushedPosition().fullPosition, 0u);
	EXPECT_EQ(store->currentSequenceNumber.load(), 1u);

	store->close();
	std::filesystem::remove_all(storePath.parent_path());
}

// The committed watermark is seeded from txn.state at load and can sit in the segment a purge
// then removes; an uncommitted read starts its walk at the watermark's sequence.
TEST(TransactionLogRetention, PurgeMovesAWatermarkOutOfAPurgedSegment) {
	auto storePath = uniqueRetentionPath();
	{
		auto writer = std::make_shared<rocksdb_js::TransactionLogStore>(
			"foo", storePath, 0, std::chrono::milliseconds(0), 0);
		writeAndFlush(*writer, 1001.0, 10);
		writer->close();
	}
	{
		std::ifstream first(storePath / "1.txnlog", std::ios::binary);
		std::string header(TRANSACTION_LOG_FILE_HEADER_SIZE, '\0');
		first.read(header.data(), header.size());
		std::ofstream second(storePath / "2.txnlog", std::ios::binary | std::ios::trunc);
		second.write(header.data(), header.size());
	}

	auto store = loadStore(storePath);
	ASSERT_TRUE(store);
	ASSERT_EQ(store->currentSequenceNumber.load(), 2u);
	ASSERT_EQ(store->lastCommittedPosition->logSequenceNumber, 1u);

	store->purge(nullptr, false, futureCutoffMs());
	EXPECT_FALSE(std::filesystem::exists(storePath / "1.txnlog"));
	EXPECT_TRUE(std::filesystem::exists(storePath / "2.txnlog"));
	EXPECT_EQ(store->lastCommittedPosition->logSequenceNumber, 2u);
	EXPECT_EQ(store->lastCommittedPosition->positionInLogFile, 0u);

	store->close();
	std::filesystem::remove_all(storePath.parent_path());
}

// Retiring a current segment at the last sequence would wrap the writer below it.
TEST(TransactionLogRetention, KeepsACurrentSegmentWithNoSuccessorSequence) {
	auto storePath = uniqueRetentionPath();
	{
		auto writer = std::make_shared<rocksdb_js::TransactionLogStore>(
			"foo", storePath, 0, std::chrono::milliseconds(0), 0);
		writeAndFlush(*writer, 1001.0, 10);
		writer->close();
	}
	auto lastSegment = storePath / (std::to_string(UINT32_MAX) + ".txnlog");
	std::filesystem::rename(storePath / "1.txnlog", lastSegment);
	writeFlushedState(storePath, static_cast<uint32_t>(std::filesystem::file_size(lastSegment)), UINT32_MAX);

	auto store = loadStore(storePath);
	ASSERT_TRUE(store);
	ASSERT_EQ(store->currentSequenceNumber.load(), UINT32_MAX);
	store->purge(nullptr, false, futureCutoffMs());
	EXPECT_TRUE(std::filesystem::exists(lastSegment));
	EXPECT_EQ(store->currentSequenceNumber.load(), UINT32_MAX);

	store->close();
	std::filesystem::remove_all(storePath.parent_path());
}

TEST(TransactionLogRetention, LoadRefusesAFlushedSequenceWithNoSuccessor) {
	auto storePath = uniqueRetentionPath();
	writeFlushedState(storePath, 200, UINT32_MAX - 1);

	EXPECT_THROW(loadStore(storePath), rocksdb_js::DBException);

	std::filesystem::remove_all(storePath.parent_path());
}
