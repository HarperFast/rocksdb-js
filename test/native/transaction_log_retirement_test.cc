#include <gtest/gtest.h>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include "transaction_log/transaction_log_entry.h"
#include "transaction_log/transaction_log_file.h"
#include "transaction_log/transaction_log_store.h"
#include "transaction_log/transaction_log_validation.h"

namespace {

std::filesystem::path uniqueRetirementStorePath() {
	auto nonce = std::chrono::steady_clock::now().time_since_epoch().count();
	return std::filesystem::temp_directory_path() /
		("rocksdb-js-retirement-" + std::to_string(nonce)) / "store";
}

} // namespace

TEST(TransactionLogRetirement, PersistedBoundarySurvivesRestartAndFailsClosedWhenCorrupt) {
	auto storePath = uniqueRetirementStorePath();
	auto logPath = storePath / "1.txnlog";
	auto markerPath = rocksdb_js::transactionLogAppendBoundaryMarkerPath(logPath);
	std::filesystem::create_directories(storePath);
	uint32_t committedSize = 0;

	{
		rocksdb_js::TransactionLogFile file(logPath, 1, true);
		file.open(1770000000000.0);
		EXPECT_EQ(
			std::filesystem::file_size(markerPath),
			static_cast<uintmax_t>(TRANSACTION_LOG_APPEND_BOUNDARY_MARKER_SIZE));
		std::string payload = "committed";
		rocksdb_js::TransactionLogEntryBatch batch(1770000000001.0);
		batch.addEntry(std::make_unique<rocksdb_js::TransactionLogEntry>(
			nullptr, payload.data(), static_cast<uint32_t>(payload.size())));
		file.writeEntries(batch);
		committedSize = file.size.load(std::memory_order_relaxed);
		file.close();

		std::ofstream physicalTail(logPath, std::ios::binary | std::ios::app);
		physicalTail.write("orphan", 6);
		physicalTail.close();

		file.appendBoundaryLost.store(true, std::memory_order_relaxed);
		file.persistAppendBoundaryRetirement();
	}

	EXPECT_EQ(std::filesystem::file_size(logPath), committedSize + 6);
	EXPECT_EQ(rocksdb_js::readTransactionLogAppendBoundaryMarker(logPath), committedSize);
	EXPECT_EQ(
		std::filesystem::file_size(markerPath),
		static_cast<uintmax_t>(TRANSACTION_LOG_APPEND_BOUNDARY_MARKER_SIZE));

	auto reopened = rocksdb_js::TransactionLogStore::load(
		storePath, 0, std::chrono::milliseconds(0), 0);
	ASSERT_NE(reopened, nullptr);
	EXPECT_EQ(reopened->currentSequenceNumber.load(std::memory_order_relaxed), 2u);
	EXPECT_EQ(reopened->getLogFileSize(1), committedSize);
	EXPECT_TRUE(rocksdb_js::validateTransactionLogStore(storePath, true).valid);
	reopened->close();

	std::filesystem::resize_file(markerPath, 3);
	ASSERT_EQ(std::filesystem::file_size(markerPath), 3u);
	EXPECT_FALSE(rocksdb_js::validateTransactionLogStore(storePath, true).valid);
	EXPECT_THROW(
		rocksdb_js::TransactionLogStore::load(
			storePath, 0, std::chrono::milliseconds(0), 0),
		rocksdb_js::TransactionLogAppendBoundaryException);

	std::filesystem::remove_all(storePath.parent_path());
	std::filesystem::remove_all(markerPath.parent_path());
}

TEST(TransactionLogRetirement, RepeatedSegmentOpenFailureIsBounded) {
	auto storePath = uniqueRetirementStorePath();
	std::filesystem::create_directories(storePath / "1.txnlog");
	std::filesystem::create_directories(storePath / "2.txnlog");

	rocksdb_js::TransactionLogStore store(
		"store", storePath, 0, std::chrono::milliseconds(0), 0);
	rocksdb_js::LogPosition position;
	std::string payload = "entry";
	rocksdb_js::TransactionLogEntryBatch batch(1770000000001.0);
	batch.addEntry(std::make_unique<rocksdb_js::TransactionLogEntry>(
		nullptr, payload.data(), static_cast<uint32_t>(payload.size())));

	EXPECT_THROW(store.writeBatch(batch, position), rocksdb_js::DBException);
	EXPECT_EQ(store.currentSequenceNumber.load(std::memory_order_relaxed), 2u);
	EXPECT_FALSE(batch.isComplete());

	store.close();
	std::filesystem::remove_all(storePath.parent_path());
}

TEST(TransactionLogRetirement, MarkerInitializationPublishesOnlyCompleteRecords) {
	auto storePath = uniqueRetirementStorePath();
	auto logPath = storePath / "1.txnlog";
	rocksdb_js::TransactionLogFile first(logPath, 1, true);
	rocksdb_js::TransactionLogFile second(logPath, 1, true);
	std::exception_ptr firstError;
	std::exception_ptr secondError;

	std::thread firstPublisher([&]() {
		try {
			first.ensureAppendBoundaryMarker();
		} catch (...) {
			firstError = std::current_exception();
		}
	});
	std::thread secondPublisher([&]() {
		try {
			second.ensureAppendBoundaryMarker();
		} catch (...) {
			secondError = std::current_exception();
		}
	});
	firstPublisher.join();
	secondPublisher.join();

	EXPECT_EQ(firstError, nullptr);
	EXPECT_EQ(secondError, nullptr);
	auto markerPath = rocksdb_js::transactionLogAppendBoundaryMarkerPath(logPath);
	EXPECT_EQ(
		std::filesystem::file_size(markerPath),
		static_cast<uintmax_t>(TRANSACTION_LOG_APPEND_BOUNDARY_MARKER_SIZE));
	EXPECT_EQ(rocksdb_js::readTransactionLogAppendBoundaryMarker(logPath), 0u);

	std::filesystem::remove_all(storePath.parent_path());
	std::filesystem::remove_all(markerPath.parent_path());
}

TEST(TransactionLogRetirement, MissingSegmentDiscardsItsStaleRetirementMarker) {
	auto storePath = uniqueRetirementStorePath();
	auto logPath = storePath / "1.txnlog";
	std::filesystem::create_directories(storePath);
	{
		rocksdb_js::TransactionLogFile original(logPath, 1, true);
		original.open(1770000000000.0);
		original.persistAppendBoundaryRetirement();
		original.close();
	}
	ASSERT_TRUE(std::filesystem::remove(logPath));

	rocksdb_js::TransactionLogFile replacement(logPath, 1, true);
	EXPECT_NO_THROW(replacement.open(1770000000001.0));
	EXPECT_EQ(rocksdb_js::readTransactionLogAppendBoundaryMarker(logPath), 0u);
	EXPECT_EQ(
		replacement.size.load(std::memory_order_relaxed),
		static_cast<uint32_t>(TRANSACTION_LOG_FILE_HEADER_SIZE));
	replacement.close();

	auto markerPath = rocksdb_js::transactionLogAppendBoundaryMarkerPath(logPath);
	std::filesystem::remove_all(storePath.parent_path());
	std::filesystem::remove_all(markerPath.parent_path());
}
