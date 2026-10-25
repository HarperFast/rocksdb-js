#include <gtest/gtest.h>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include "core/encoding.h"
#include "transaction_log/transaction_log_entry.h"
#include "transaction_log/transaction_log_file.h"
#include "transaction_log/transaction_log_store.h"

namespace {

std::filesystem::path uniqueFirstEntryStorePath() {
	auto nonce = std::chrono::steady_clock::now().time_since_epoch().count();
	return std::filesystem::temp_directory_path() /
		("rocksdb-js-first-entry-" + std::to_string(nonce)) / "store";
}

double timestampAt(const std::filesystem::path& file, uint32_t offset) {
	std::ifstream in(file, std::ios::binary);
	in.seekg(offset);
	char bytes[8];
	in.read(bytes, sizeof(bytes));
	return in.gcount() == sizeof(bytes) ? rocksdb_js::readDoubleBE(bytes) : 0;
}

void writeOne(
	rocksdb_js::TransactionLogStore& store,
	double timestamp,
	std::string payload,
	rocksdb_js::LogPosition& position,
	rocksdb_js::LogPosition& firstEntry
) {
	rocksdb_js::TransactionLogEntryBatch batch(timestamp);
	batch.addEntry(std::make_unique<rocksdb_js::TransactionLogEntry>(
		nullptr, payload.data(), static_cast<uint32_t>(payload.size())));
	firstEntry = rocksdb_js::LogPosition(1, 99); // writeBatch must overwrite whatever the caller had
	store.writeBatch(batch, position, &firstEntry);
}

} // namespace

TEST(TransactionLogFirstEntry, FirstBatchOfANewSegmentStartsAfterTheFileHeader) {
	auto storePath = uniqueFirstEntryStorePath();
	std::filesystem::create_directories(storePath);
	{
		rocksdb_js::TransactionLogStore store("store", storePath, 0, std::chrono::milliseconds(0), 0);
		rocksdb_js::LogPosition position;
		rocksdb_js::LogPosition firstEntry;
		writeOne(store, 1770000000001.0, "first", position, firstEntry);

		EXPECT_EQ(firstEntry.logSequenceNumber, 1u);
		EXPECT_EQ(firstEntry.positionInLogFile, static_cast<uint32_t>(TRANSACTION_LOG_FILE_HEADER_SIZE));
		EXPECT_EQ(timestampAt(storePath / "1.txnlog", firstEntry.positionInLogFile), 1770000000001.0);

		writeOne(store, 1770000000002.0, "second", position, firstEntry);
		EXPECT_EQ(firstEntry.logSequenceNumber, 1u);
		EXPECT_EQ(firstEntry.positionInLogFile, position.positionInLogFile);
		EXPECT_EQ(timestampAt(storePath / "1.txnlog", firstEntry.positionInLogFile), 1770000000002.0);
		store.close();
	}
	std::filesystem::remove_all(storePath.parent_path());
}

TEST(TransactionLogFirstEntry, BatchThatRotatesIsAddressedInTheNewSegment) {
	auto storePath = uniqueFirstEntryStorePath();
	std::filesystem::create_directories(storePath);
	{
		const uint32_t maxFileSize = TRANSACTION_LOG_FILE_HEADER_SIZE + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 20;
		rocksdb_js::TransactionLogStore store("store", storePath, maxFileSize, std::chrono::milliseconds(0), 0);
		rocksdb_js::LogPosition position;
		rocksdb_js::LogPosition firstEntry;
		writeOne(store, 1770000000001.0, std::string(10, 'a'), position, firstEntry);
		ASSERT_EQ(firstEntry.logSequenceNumber, 1u);

		writeOne(store, 1770000000002.0, std::string(40, 'b'), position, firstEntry);
		EXPECT_EQ(firstEntry.logSequenceNumber, 2u);
		EXPECT_EQ(firstEntry.positionInLogFile, static_cast<uint32_t>(TRANSACTION_LOG_FILE_HEADER_SIZE));
		EXPECT_EQ(timestampAt(storePath / "2.txnlog", firstEntry.positionInLogFile), 1770000000002.0);
		EXPECT_FALSE(
			position.logSequenceNumber == firstEntry.logSequenceNumber &&
			position.positionInLogFile == firstEntry.positionInLogFile);
		store.close();
	}
	std::filesystem::remove_all(storePath.parent_path());
}

TEST(TransactionLogFirstEntry, LazilyOpenedSegmentWithDataIsAddressedAtItsEnd) {
	auto storePath = uniqueFirstEntryStorePath();
	std::filesystem::create_directories(storePath);
	uint32_t existingSize = 0;
	{
		rocksdb_js::TransactionLogFile file(storePath / "1.txnlog", 1);
		file.open(1770000000000.0);
		std::string payload = "already on disk";
		rocksdb_js::TransactionLogEntryBatch batch(1770000000001.0);
		batch.addEntry(std::make_unique<rocksdb_js::TransactionLogEntry>(
			nullptr, payload.data(), static_cast<uint32_t>(payload.size())));
		file.writeEntries(batch);
		existingSize = file.size.load(std::memory_order_relaxed);
		file.close();
	}
	ASSERT_GT(existingSize, static_cast<uint32_t>(TRANSACTION_LOG_FILE_HEADER_SIZE));
	{
		// constructed without load(): segment 1 is opened lazily by the first append
		rocksdb_js::TransactionLogStore store("store", storePath, 0, std::chrono::milliseconds(0), 0);
		rocksdb_js::LogPosition position;
		rocksdb_js::LogPosition firstEntry;
		writeOne(store, 1770000000002.0, "appended", position, firstEntry);
		EXPECT_EQ(firstEntry.logSequenceNumber, 1u);
		EXPECT_EQ(firstEntry.positionInLogFile, existingSize);
		EXPECT_EQ(timestampAt(storePath / "1.txnlog", firstEntry.positionInLogFile), 1770000000002.0);
		store.close();
	}
	std::filesystem::remove_all(storePath.parent_path());
}

TEST(TransactionLogFirstEntry, EmptyBatchRecordsNoAddress) {
	auto storePath = uniqueFirstEntryStorePath();
	std::filesystem::create_directories(storePath);
	{
		rocksdb_js::TransactionLogStore store("store", storePath, 0, std::chrono::milliseconds(0), 0);
		rocksdb_js::LogPosition position;
		rocksdb_js::LogPosition firstEntry(1, 99);
		rocksdb_js::TransactionLogEntryBatch batch(1770000000001.0);
		store.writeBatch(batch, position, &firstEntry);
		EXPECT_EQ(firstEntry.fullPosition, 0);
		store.close();
	}
	std::filesystem::remove_all(storePath.parent_path());
}
