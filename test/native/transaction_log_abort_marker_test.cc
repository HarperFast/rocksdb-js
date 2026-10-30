#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include "core/test_seam.h"
#include "transaction_log/transaction_log_entry.h"
#include "transaction_log/transaction_log_file.h"
#include "transaction_log/transaction_log_store.h"

namespace {

using rocksdb_js::LogPosition;
using rocksdb_js::TransactionLogEntry;
using rocksdb_js::TransactionLogEntryBatch;
using rocksdb_js::TransactionLogFile;
using rocksdb_js::TransactionLogStore;

constexpr double T1 = 1770000000001.0;
constexpr double T2 = 1770000000002.0;
constexpr double T3 = 1770000000003.0;
// Layout written by writeSegment(): T1 {"a", "bb"} at 13 and 27, T2 {"c"} at 42, T3 {"d"} at 56.
constexpr uint32_t FIRST = TRANSACTION_LOG_FILE_HEADER_SIZE;
constexpr uint32_t SECOND_ENTRY = FIRST + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 1;
constexpr uint32_t SECOND_BATCH = SECOND_ENTRY + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 2;
constexpr uint32_t THIRD_BATCH = SECOND_BATCH + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 1;

struct RemoveOnExit {
	std::filesystem::path path;
	~RemoveOnExit() {
		std::error_code ignored;
		std::filesystem::remove_all(this->path, ignored);
	}
};

std::filesystem::path uniqueStorePath() {
	auto nonce = std::chrono::steady_clock::now().time_since_epoch().count();
	return std::filesystem::temp_directory_path() / ("rocksdb-js-abort-marker-" + std::to_string(nonce)) / "store";
}

void writeBatch(TransactionLogStore& store, double timestamp, std::vector<std::string> payloads) {
	TransactionLogEntryBatch batch(timestamp);
	for (auto& payload : payloads) {
		batch.addEntry(std::make_unique<TransactionLogEntry>(nullptr, payload.data(), static_cast<uint32_t>(payload.size())));
	}
	LogPosition position;
	store.writeBatch(batch, position);
}

std::filesystem::path writeSegment(const std::filesystem::path& storePath) {
	std::filesystem::create_directories(storePath);
	TransactionLogStore store("store", storePath, 0, std::chrono::milliseconds(0), 0);
	writeBatch(store, T1, { "a", "bb" });
	writeBatch(store, T2, { "c" });
	writeBatch(store, T3, { "d" });
	store.close();
	return storePath / "1.txnlog";
}

std::string readBytes(const std::filesystem::path& path) {
	std::ifstream in(path, std::ios::binary);
	return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

std::unique_ptr<TransactionLogFile> openSegment(const std::filesystem::path& path) {
	auto file = std::make_unique<TransactionLogFile>(path, 1);
	file->open(0);
	return file;
}

} // namespace

TEST(TransactionLogAbortMarker, ChangesOnlyTheAbortBitOfTheFirstEntry) {
	auto storePath = uniqueStorePath();
	RemoveOnExit cleanup{storePath.parent_path()};
	auto path = writeSegment(storePath);
	std::string before = readBytes(path);
	{
		auto file = openSegment(path);
		file->syncThrough(file->markEntryAborted(FIRST, T1));
		file->close();
	}
	std::string after = readBytes(path);
	ASSERT_EQ(before.size(), after.size());
	for (size_t i = 0; i < before.size(); i++) {
		if (i == FIRST + TRANSACTION_LOG_ENTRY_HEADER_SIZE - 1) {
			EXPECT_EQ(static_cast<uint8_t>(before[i] ^ after[i]), TRANSACTION_LOG_ENTRY_ABORTED_FLAG);
		} else {
			EXPECT_EQ(before[i], after[i]) << "byte " << i;
		}
	}
}

TEST(TransactionLogAbortMarker, RefusesAnAddressThatDoesNotStartItsBatchWithoutWriting) {
	auto storePath = uniqueStorePath();
	RemoveOnExit cleanup{storePath.parent_path()};
	auto path = writeSegment(storePath);
	std::string before = readBytes(path);
	{
		auto file = openSegment(path);
		EXPECT_THROW(file->markEntryAborted(0, T1), rocksdb_js::DBException);                 // file header
		EXPECT_THROW(file->markEntryAborted(FIRST + 3, T1), rocksdb_js::DBException);         // inside a header
		EXPECT_THROW(file->markEntryAborted(SECOND_BATCH, T1), rocksdb_js::DBException);      // wrong timestamp
		EXPECT_THROW(file->markEntryAborted(THIRD_BATCH + 1, T3), rocksdb_js::DBException);   // past the extent
		EXPECT_THROW(file->markEntryAborted(UINT32_MAX - 4, T1), rocksdb_js::DBException);    // overflowing
		file->close();
	}
	EXPECT_EQ(readBytes(path), before);
}

TEST(TransactionLogAbortMarker, ARetryAfterAFailedSyncWritesTheMarkerAgain) {
	auto storePath = uniqueStorePath();
	RemoveOnExit cleanup{storePath.parent_path()};
	auto path = writeSegment(storePath);
	auto file = openSegment(path);
	std::atomic<int> syncs = 0;
	bool failNext = true;
	file->beforeSyncForTests = [&]() {
		syncs++;
		if (failNext) {
			failNext = false;
			throw rocksdb_js::DBException("injected sync failure");
		}
	};
	uint64_t first = file->markEntryAborted(SECOND_BATCH, T2);
	EXPECT_THROW(file->syncThrough(first), rocksdb_js::DBException);
	std::string marked = readBytes(path);
	// After a failed fsync the kernel may have cleaned the page without writing it, so a retry
	// rewrites the byte and needs a sync of its own rather than counting the failed one.
	uint64_t retried = file->markEntryAborted(SECOND_BATCH, T2);
	EXPECT_GT(retried, first);
	file->syncThrough(retried);
	EXPECT_EQ(syncs.load(), 2);
	EXPECT_EQ(readBytes(path), marked);
	file->close();
}

TEST(TransactionLogAbortMarker, AFailedSyncFailsEveryMarkerWrittenBeforeIt) {
	auto storePath = uniqueStorePath();
	RemoveOnExit cleanup{storePath.parent_path()};
	auto path = writeSegment(storePath);
	auto file = openSegment(path);
	bool failNext = true;
	file->beforeSyncForTests = [&]() {
		if (failNext) {
			failNext = false;
			throw rocksdb_js::DBException("injected sync failure");
		}
	};
	uint64_t first = file->markEntryAborted(FIRST, T1);
	uint64_t second = file->markEntryAborted(SECOND_BATCH, T2);
	EXPECT_THROW(file->syncThrough(first), rocksdb_js::DBException);
	// The failed sync may have dropped the second marker's page as well; a later sync that succeeds
	// must not vouch for it.
	EXPECT_THROW(file->syncThrough(second), rocksdb_js::DBException);
	file->syncThrough(file->markEntryAborted(SECOND_BATCH, T2));

	// A marker written while a failing sync runs is just as unproven.
	uint64_t during = 0;
	file->beforeSyncForTests = [&]() {
		during = file->markEntryAborted(THIRD_BATCH, T3);
		throw rocksdb_js::DBException("injected sync failure");
	};
	EXPECT_THROW(file->syncThrough(file->markEntryAborted(FIRST, T1)), rocksdb_js::DBException);
	file->beforeSyncForTests = nullptr;
	EXPECT_THROW(file->syncThrough(during), rocksdb_js::DBException);
	file->close();
}

TEST(TransactionLogAbortMarker, ConcurrentAbortsShareOneSync) {
	auto storePath = uniqueStorePath();
	RemoveOnExit cleanup{storePath.parent_path()};
	auto path = writeSegment(storePath);
	auto file = openSegment(path);
	std::atomic<int> syncs = 0;
	std::atomic<bool> releaseSync = false;
	file->beforeSyncForTests = [&]() {
		syncs++;
		while (!releaseSync.load()) {
			std::this_thread::yield();
		}
	};
	std::vector<uint64_t> generations = {
		file->markEntryAborted(FIRST, T1),
		file->markEntryAborted(SECOND_BATCH, T2),
		file->markEntryAborted(THIRD_BATCH, T3),
	};
	std::vector<std::thread> waiters;
	std::atomic<int> returned = 0;
	for (uint64_t generation : generations) {
		waiters.emplace_back([&, generation]() {
			file->syncThrough(generation);
			returned++;
		});
	}
	while (syncs.load() == 0) {
		std::this_thread::yield();
	}
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	EXPECT_EQ(returned.load(), 0); // nobody is released before the sync covering them completes
	releaseSync = true;
	for (auto& waiter : waiters) {
		waiter.join();
	}
	EXPECT_EQ(syncs.load(), 1);
	EXPECT_EQ(returned.load(), 3);
	file->close();
}

TEST(TransactionLogAbortMarker, FlushSyncsAMarkerThatDidNotGrowTheFileEvenAfterClose) {
	auto storePath = uniqueStorePath();
	RemoveOnExit cleanup{storePath.parent_path()};
	auto path = writeSegment(storePath);
	auto file = openSegment(path);
	std::atomic<int> syncs = 0;
	file->beforeSyncForTests = [&]() { syncs++; };
	file->flush();
	int baseline = syncs.load();
	file->flush();
	EXPECT_EQ(syncs.load(), baseline);
	file->markEntryAborted(FIRST, T1);
	file->close();
	file->flush();
	EXPECT_EQ(syncs.load(), baseline + 1);
	file->flush();
	EXPECT_EQ(syncs.load(), baseline + 1);
}

TEST(TransactionLogAbortMarker, AFailedCancellationRefusesAppendsUntilARetryMarksIt) {
	auto storePath = uniqueStorePath();
	RemoveOnExit cleanup{storePath.parent_path()};
	std::filesystem::create_directories(storePath);
	TransactionLogStore store("store", storePath, 0, std::chrono::milliseconds(0), 0);
	writeBatch(store, T1, { "kept" });

	TransactionLogEntryBatch batch(T2);
	std::string payload = "aborted";
	batch.addEntry(std::make_unique<TransactionLogEntry>(nullptr, payload.data(), static_cast<uint32_t>(payload.size())));
	LogPosition position;
	LogPosition firstEntry;
	ASSERT_TRUE(store.tryBindTransaction());
	store.writeBatch(batch, position, &firstEntry);

	abortMarkFailureFlag().store(2);
	EXPECT_FALSE(store.cancelBatch(firstEntry, T2, position));
	EXPECT_TRUE(store.isFailedCancellation(position));
	EXPECT_THROW(writeBatch(store, T3, { "refused" }), rocksdb_js::DBException);
	EXPECT_EQ(store.abortMarkFailures.load(), 2u);

	writeBatch(store, T3, { "after" });
	EXPECT_FALSE(store.isFailedCancellation(position));
	EXPECT_EQ(store.abortsMarked.load(), 1u);
	store.close();

	std::string bytes = readBytes(storePath / "1.txnlog");
	EXPECT_EQ(static_cast<uint8_t>(bytes[firstEntry.positionInLogFile + TRANSACTION_LOG_ENTRY_HEADER_SIZE - 1]),
		TRANSACTION_LOG_ENTRY_ABORTED_FLAG | TRANSACTION_LOG_ENTRY_LAST_FLAG);
}
