#include <gtest/gtest.h>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <string>
#ifndef _WIN32
#include <unistd.h>
#endif
#include "core/encoding.h"
#include "transaction_log/transaction_log_entry.h"
#include "transaction_log/transaction_log_purge_record.h"
#include "transaction_log/transaction_log_store.h"

namespace {

using rocksdb_js::TransactionLogPurgeRecord;
using rocksdb_js::TransactionLogPurgeRecordState;
using rocksdb_js::TransactionLogStore;

// Two 100-byte entries fit a 256-byte segment; a third rotates.
constexpr uint32_t MAX_FILE_SIZE = 256;
constexpr uint32_t PAYLOAD_SIZE = 100;

// Keys the 2.13.0 fixture holds ahead of the clock: 2100-01-01T00:00:00Z and a second after it.
constexpr double RELEASE_213_AHEAD_1 = 4102444800000.0;
constexpr double RELEASE_213_AHEAD_2 = RELEASE_213_AHEAD_1 + 1000;

struct PurgeWatermarkTest : ::testing::Test {
	std::filesystem::path root;
	std::filesystem::path storePath;
	rocksdb::SequenceNumber rocksSequence = 0;

	void SetUp() override {
		auto nonce = std::chrono::steady_clock::now().time_since_epoch().count();
		root = std::filesystem::temp_directory_path() / ("rocksdb-js-purge-watermark-" + std::to_string(nonce));
		storePath = root / "store";
	}

	void TearDown() override {
		TransactionLogStore::purgeScanBudgetMsForTests.store(-1);
		std::error_code ignored;
		std::filesystem::remove_all(root, ignored);
	}

	/** A store whose directory this process created, as ResolveStore makes one. */
	std::shared_ptr<TransactionLogStore> createStore(std::chrono::milliseconds retention = std::chrono::milliseconds(0)) {
		std::filesystem::create_directories(storePath);
		auto store = std::make_shared<TransactionLogStore>("foo", storePath, MAX_FILE_SIZE, retention, 0);
		store->initializeNewPurgeRecord();
		return store;
	}

	std::shared_ptr<TransactionLogStore> loadStore(
		std::chrono::milliseconds retention = std::chrono::milliseconds(0), bool readOnly = false) {
		return TransactionLogStore::load(storePath, MAX_FILE_SIZE, retention, 0, readOnly);
	}

	/** Writes one entry keyed `timestamp`, commits it and flushes it into txn.state. */
	void write(TransactionLogStore& store, double timestamp) {
		std::string payload(PAYLOAD_SIZE, 'x');
		rocksdb_js::TransactionLogEntryBatch batch(timestamp);
		batch.addEntry(std::make_unique<rocksdb_js::TransactionLogEntry>(
			nullptr, payload.data(), static_cast<uint32_t>(payload.size())));
		rocksdb_js::LogPosition position;
		store.writeBatch(batch, position);
		auto sequence = ++rocksSequence;
		store.commitFinished(position, [sequence]() -> rocksdb::SequenceNumber { return sequence; });
		store.databaseFlushed(sequence);
	}

	std::filesystem::path segment(uint32_t sequence) const {
		return storePath / (std::to_string(sequence) + ".txnlog");
	}

	void ageSegment(uint32_t sequence, std::chrono::hours age = std::chrono::hours(2)) {
		std::filesystem::last_write_time(segment(sequence), std::filesystem::file_time_type::clock::now() - age);
	}

	static uint64_t cutoffOneHourAgo() {
		return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
			(std::chrono::system_clock::now() - std::chrono::hours(1)).time_since_epoch()).count());
	}

	std::string readSlot(uint64_t generation) const {
		std::string bytes(rocksdb_js::TRANSACTION_LOG_PURGE_RECORD_SLOT_SIZE, '\0');
		std::ifstream file(storePath / TRANSACTION_LOG_PURGE_RECORD_NAME, std::ios::binary);
		file.seekg((generation & 1) ? rocksdb_js::TRANSACTION_LOG_PURGE_RECORD_SLOT_STRIDE : 0);
		file.read(bytes.data(), bytes.size());
		return bytes;
	}

	TransactionLogPurgeRecord recordOnDisk(const std::filesystem::path& path) const {
		TransactionLogPurgeRecord record;
		EXPECT_EQ(rocksdb_js::readTransactionLogPurgeRecord(path, record), TransactionLogPurgeRecordState::Valid);
		return record;
	}

	/**
	 * Copies the store rocksdb-js 2.13.0 wrote (test/fixtures/txnlog-2.13.0/generate.mjs): 2.13.0
	 * deleted segment 1, which held RELEASE_213_AHEAD_1; segment 2 holds 2000 and RELEASE_213_AHEAD_2;
	 * segment 3, the current one, holds 3000. txn.state covers all of them.
	 */
	void copyRelease213Store() {
		auto fixture = std::filesystem::current_path() / "test" / "fixtures" / "txnlog-2.13.0" / "store";
		ASSERT_TRUE(std::filesystem::exists(fixture / "2.txnlog"))
			<< "fixture not found at " << fixture << "; run the tests from the repository root";
		std::filesystem::create_directories(root);
		std::filesystem::copy(fixture, storePath, std::filesystem::copy_options::recursive);
		ASSERT_FALSE(std::filesystem::exists(storePath / TRANSACTION_LOG_PURGE_RECORD_NAME));
	}

	/**
	 * Three segments: [1000, 5000] [3000, 1500] [2000]. Segment 1 holds the highest key of the log,
	 * above every key of the segments after it.
	 */
	void writeOutOfOrderSegments(TransactionLogStore& store) {
		write(store, 1000);
		write(store, 5000);
		write(store, 3000);
		write(store, 1500);
		write(store, 2000);
		ASSERT_TRUE(std::filesystem::exists(segment(3)));
	}
};

} // namespace

TEST_F(PurgeWatermarkTest, NewStoreIsKnownWithNothingPurged) {
	auto store = createStore();
	auto watermark = store->getPurgeWatermark();
	EXPECT_TRUE(watermark.known);
	EXPECT_EQ(watermark.purgedThrough, 0);
	store->close();

	auto reloaded = loadStore();
	EXPECT_TRUE(reloaded->getPurgeWatermark().known);
}

// Keys are out of order within and across segments; the watermark is the purged segment's own
// largest key, not a later segment's.
TEST_F(PurgeWatermarkTest, WatermarkIsThePurgedSegmentsTrueMaximum) {
	auto store = createStore();
	writeOutOfOrderSegments(*store);
	ageSegment(1);

	store->purge(nullptr, false, cutoffOneHourAgo());
	EXPECT_FALSE(std::filesystem::exists(segment(1)));
	EXPECT_TRUE(std::filesystem::exists(segment(2)));
	auto watermark = store->getPurgeWatermark();
	ASSERT_TRUE(watermark.known);
	EXPECT_EQ(watermark.purgedThrough, 5000);
	EXPECT_EQ(recordOnDisk(storePath).watermark, 5000);

	ageSegment(2);
	store->purge(nullptr, false, cutoffOneHourAgo());
	EXPECT_FALSE(std::filesystem::exists(segment(2)));
	EXPECT_EQ(store->getPurgeWatermark().purgedThrough, 5000);
	store->close();
}

TEST_F(PurgeWatermarkTest, InheritedSegmentIsScannedForItsTrueMaximum) {
	{
		auto writer = createStore();
		writeOutOfOrderSegments(*writer);
		writer->close();
	}
	ageSegment(1);
	auto store = loadStore();
	ASSERT_TRUE(store->getPurgeWatermark().known);
	store->purge(nullptr, false, cutoffOneHourAgo());
	EXPECT_FALSE(std::filesystem::exists(segment(1)));
	auto watermark = store->getPurgeWatermark();
	ASSERT_TRUE(watermark.known);
	EXPECT_EQ(watermark.purgedThrough, 5000);
	store->close();
}

// A key minted ahead of the local clock, in a segment old enough to purge by mtime.
TEST_F(PurgeWatermarkTest, CoversAKeyAheadOfTheClock) {
	double nowMs = static_cast<double>(std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::system_clock::now().time_since_epoch()).count());
	double aheadOfClock = nowMs + 24.0 * 60 * 60 * 1000;
	{
		auto writer = createStore();
		write(*writer, nowMs - 1000);
		write(*writer, aheadOfClock);
		write(*writer, nowMs);
		writer->close();
	}
	ageSegment(1);
	auto store = loadStore(std::chrono::milliseconds(60 * 60 * 1000));
	EXPECT_FALSE(std::filesystem::exists(segment(1)));
	auto watermark = store->getPurgeWatermark();
	ASSERT_TRUE(watermark.known);
	EXPECT_EQ(watermark.purgedThrough, aheadOfClock);
	store->close();
}

TEST_F(PurgeWatermarkTest, RetentionOnOpenRaisesAndPersistsIt) {
	{
		auto writer = createStore();
		writeOutOfOrderSegments(*writer);
		writer->close();
	}
	ageSegment(1);
	auto store = loadStore(std::chrono::milliseconds(60 * 60 * 1000));
	EXPECT_FALSE(std::filesystem::exists(segment(1)));
	EXPECT_EQ(store->getPurgeWatermark().purgedThrough, 5000);
	store->close();

	auto record = recordOnDisk(storePath);
	EXPECT_TRUE(record.known);
	EXPECT_EQ(record.watermark, 5000);
	EXPECT_EQ(record.throughSequence, 1u);
	auto reloaded = loadStore();
	EXPECT_EQ(reloaded->getPurgeWatermark().purgedThrough, 5000);
}

// Close and rotation delete nothing (doClose() empties the registry before its purge call), so
// they have nothing to record.
TEST_F(PurgeWatermarkTest, CloseAndRotationDeleteNothing) {
	auto store = createStore(std::chrono::milliseconds(1));
	write(*store, 1000);
	write(*store, 5000);
	ageSegment(1);
	write(*store, 3000); // rotates to segment 2
	EXPECT_TRUE(std::filesystem::exists(segment(1)));
	store->close();
	EXPECT_TRUE(std::filesystem::exists(segment(1)));
	EXPECT_TRUE(std::filesystem::exists(segment(2)));
	auto record = recordOnDisk(storePath);
	EXPECT_TRUE(record.known);
	EXPECT_EQ(record.watermark, 0);
}

// A crash after the record is durable and before any unlink leaves it at or above every key
// the purge was about to delete, and still known: the surviving files are already covered.
TEST_F(PurgeWatermarkTest, CrashBetweenRecordAndUnlinkLeavesItCovering) {
	auto store = createStore();
	writeOutOfOrderSegments(*store);
	ageSegment(1);
	ageSegment(2);
	auto crashImage = root / "crash";
	store->afterPurgeRecordPersistedForTests = [&]() {
		std::filesystem::copy(storePath, crashImage, std::filesystem::copy_options::recursive);
	};
	store->purge(nullptr, false, cutoffOneHourAgo());
	store->afterPurgeRecordPersistedForTests = nullptr;
	EXPECT_FALSE(std::filesystem::exists(segment(1)));
	EXPECT_FALSE(std::filesystem::exists(segment(2)));
	store->close();

	ASSERT_TRUE(std::filesystem::exists(crashImage / "1.txnlog"));
	ASSERT_TRUE(std::filesystem::exists(crashImage / "2.txnlog"));
	auto recovered = TransactionLogStore::load(crashImage, MAX_FILE_SIZE, std::chrono::milliseconds(0), 0, false);
	auto watermark = recovered->getPurgeWatermark();
	ASSERT_TRUE(watermark.known);
	EXPECT_GE(watermark.purgedThrough, 5000);

	// the interrupted purge finishes on a later run without losing what it recorded
	for (const char* name : { "1.txnlog", "2.txnlog" }) {
		std::filesystem::last_write_time(crashImage / name, std::filesystem::file_time_type::clock::now() - std::chrono::hours(2));
	}
	recovered->purge(nullptr, false, cutoffOneHourAgo());
	EXPECT_FALSE(std::filesystem::exists(crashImage / "1.txnlog"));
	EXPECT_TRUE(recovered->getPurgeWatermark().known);
	EXPECT_EQ(recovered->getPurgeWatermark().purgedThrough, 5000);
	recovered->close();
}

// A purge that chose the current segment recorded it, then stopped before the unlink: the reopened
// writer starts past it, so the record stays known and no key lands in a sequence it covers.
TEST_F(PurgeWatermarkTest, CrashBeforeUnlinkingTheCurrentSegmentStaysKnown) {
	auto store = createStore();
	write(*store, 1000);
	write(*store, 5000);
	ageSegment(1);
	auto crashImage = root / "crash";
	store->afterPurgeRecordPersistedForTests = [&]() {
		std::filesystem::copy(storePath, crashImage, std::filesystem::copy_options::recursive);
	};
	store->purge(nullptr, false, cutoffOneHourAgo());
	store->afterPurgeRecordPersistedForTests = nullptr;
	EXPECT_FALSE(std::filesystem::exists(segment(1)));
	store->close();

	ASSERT_TRUE(std::filesystem::exists(crashImage / "1.txnlog"));
	auto recovered = TransactionLogStore::load(crashImage, MAX_FILE_SIZE, std::chrono::milliseconds(0), 0, false);
	EXPECT_EQ(recovered->currentSequenceNumber.load(), 2u);
	auto watermark = recovered->getPurgeWatermark();
	ASSERT_TRUE(watermark.known);
	EXPECT_EQ(watermark.purgedThrough, 5000);
	write(*recovered, 9000);
	EXPECT_TRUE(std::filesystem::exists(crashImage / "2.txnlog"));
	recovered->close();
	EXPECT_TRUE(TransactionLogStore::load(crashImage, MAX_FILE_SIZE, std::chrono::milliseconds(0), 0, false)
		->getPurgeWatermark().known);
}

// An unlink that fails stops the run before the chosen current segment is deleted; the writer has
// already moved past it, so no key lands in a sequence the record covers.
TEST_F(PurgeWatermarkTest, FailedUnlinkLeavesTheChosenCurrentSegmentRetired) {
	auto store = createStore();
	write(*store, 1000);
	write(*store, 5000);
	write(*store, 3000); // segment 2, current
	ageSegment(1);
	ageSegment(2);
	store->failUnlinkForTests = [](uint32_t sequence) { return sequence == 1; };
	store->purge(nullptr, false, cutoffOneHourAgo());
	store->failUnlinkForTests = nullptr;
	EXPECT_TRUE(std::filesystem::exists(segment(1)));
	EXPECT_TRUE(std::filesystem::exists(segment(2)));
	EXPECT_EQ(store->currentSequenceNumber.load(), 3u);
	write(*store, 9000);
	EXPECT_TRUE(std::filesystem::exists(segment(3)));
	store->close();
}

// A retry inherits the failed attempt's sequence, which can cover the current segment even when this
// run did not choose it; the writer must still move past it.
TEST_F(PurgeWatermarkTest, RetryRetiresACurrentSegmentTheRecordCovers) {
	auto store = createStore();
	write(*store, 1000);
	write(*store, 5000);
	write(*store, 3000); // segment 2, current
	ageSegment(1);
	ageSegment(2);
	auto durable = recordOnDisk(storePath).generation;
	store->failPurgeRecordWriteForTests = [&]() {
		TransactionLogPurgeRecord attempted;
		attempted.known = true;
		attempted.watermark = 5000;
		attempted.throughSequence = 2;
		attempted.generation = durable + 1;
		EXPECT_TRUE(rocksdb_js::writeTransactionLogPurgeRecord(storePath, attempted));
		return true;
	};
	store->purge(nullptr, false, cutoffOneHourAgo());
	store->failPurgeRecordWriteForTests = nullptr;
	ASSERT_EQ(store->currentSequenceNumber.load(), 2u);
	write(*store, 9000); // segment 2 again, now too recent to purge
	ageSegment(1);
	store->purge(nullptr, false, cutoffOneHourAgo());
	EXPECT_FALSE(std::filesystem::exists(segment(1)));
	EXPECT_EQ(recordOnDisk(storePath).throughSequence, 2u);
	EXPECT_EQ(store->currentSequenceNumber.load(), 3u);
	store->close();
}

TEST_F(PurgeWatermarkTest, FailedRecordWriteDeletesNothing) {
	auto store = createStore();
	writeOutOfOrderSegments(*store);
	ageSegment(1);
	store->failPurgeRecordWriteForTests = []() { return true; };
	store->purge(nullptr, false, cutoffOneHourAgo());
	EXPECT_TRUE(std::filesystem::exists(segment(1)));
	EXPECT_EQ(store->getPurgeWatermark().purgedThrough, 0);

	store->failPurgeRecordWriteForTests = nullptr;
	store->purge(nullptr, false, cutoffOneHourAgo());
	EXPECT_FALSE(std::filesystem::exists(segment(1)));
	EXPECT_EQ(store->getPurgeWatermark().purgedThrough, 5000);
	store->close();
}

// A torn write damages only the slot being written; the other slot is the previous record,
// which is still correct because nothing is unlinked until the write is durable.
// A write whose sync failed may never reach the disk, so its retry must not overwrite the other,
// durable slot.
TEST_F(PurgeWatermarkTest, RetryAfterAFailedWriteKeepsTheDurableSlot) {
	auto store = createStore();
	writeOutOfOrderSegments(*store);
	auto durable = recordOnDisk(storePath).generation;
	auto durableSlot = (durable & 1) ? rocksdb_js::TRANSACTION_LOG_PURGE_RECORD_SLOT_STRIDE : 0;
	auto readSlot = [&](uint32_t offset) {
		std::string bytes(rocksdb_js::TRANSACTION_LOG_PURGE_RECORD_SLOT_SIZE, '\0');
		std::ifstream file(storePath / TRANSACTION_LOG_PURGE_RECORD_NAME, std::ios::binary);
		file.seekg(offset);
		file.read(bytes.data(), bytes.size());
		return bytes;
	};
	auto before = readSlot(durableSlot);
	ageSegment(1);
	// The slot is written, then the sync fails: simulate it by writing the slot and failing.
	store->failPurgeRecordWriteForTests = [&]() {
		TransactionLogPurgeRecord attempted;
		attempted.known = true;
		attempted.watermark = 4000;
		attempted.generation = durable + 1;
		EXPECT_TRUE(rocksdb_js::writeTransactionLogPurgeRecord(storePath, attempted));
		return true;
	};
	store->purge(nullptr, false, cutoffOneHourAgo());
	EXPECT_TRUE(std::filesystem::exists(segment(1)));
	store->failPurgeRecordWriteForTests = nullptr;
	store->purge(nullptr, false, cutoffOneHourAgo());
	EXPECT_FALSE(std::filesystem::exists(segment(1)));
	EXPECT_EQ(readSlot(durableSlot), before);
	auto record = recordOnDisk(storePath);
	EXPECT_EQ(record.generation, durable + 1);
	EXPECT_EQ(record.watermark, 5000);
	store->close();
}

TEST_F(PurgeWatermarkTest, TornWriteFallsBackToThePreviousRecord) {
	std::filesystem::create_directories(storePath);
	TransactionLogPurgeRecord first;
	first.known = true;
	first.watermark = 5000;
	first.throughSequence = 1;
	first.generation = 2;
	ASSERT_TRUE(rocksdb_js::writeTransactionLogPurgeRecord(storePath, first));
	TransactionLogPurgeRecord second = first;
	second.watermark = 9000;
	second.generation = 3;
	ASSERT_TRUE(rocksdb_js::writeTransactionLogPurgeRecord(storePath, second));
	EXPECT_EQ(recordOnDisk(storePath).watermark, 9000);

	{
		std::fstream file(storePath / TRANSACTION_LOG_PURGE_RECORD_NAME, std::ios::binary | std::ios::in | std::ios::out);
		file.seekp(rocksdb_js::TRANSACTION_LOG_PURGE_RECORD_SLOT_STRIDE + 24);
		file.write("\xff\xff", 2);
	}
	auto record = recordOnDisk(storePath);
	EXPECT_EQ(record.generation, 2u);
	EXPECT_EQ(record.watermark, 5000);

	// both slots unreadable: unknown, never a low watermark
	{
		std::fstream file(storePath / TRANSACTION_LOG_PURGE_RECORD_NAME, std::ios::binary | std::ios::in | std::ios::out);
		file.seekp(24);
		file.write("\xff\xff", 2);
	}
	TransactionLogPurgeRecord ignored;
	EXPECT_EQ(rocksdb_js::readTransactionLogPurgeRecord(storePath, ignored), TransactionLogPurgeRecordState::Corrupt);
	auto store = loadStore();
	EXPECT_FALSE(store->getPurgeWatermark().known);
	store->close();
}

// A store an older release created has no record: unknown, and purging by this release does not
// make it trustworthy, since keys the older release deleted were never recorded.
TEST_F(PurgeWatermarkTest, PreviousReleaseStoreStaysUnknown) {
	{
		auto writer = createStore();
		writeOutOfOrderSegments(*writer);
		writer->close();
	}
	std::filesystem::remove(storePath / TRANSACTION_LOG_PURGE_RECORD_NAME);
	ageSegment(1);

	auto store = loadStore();
	EXPECT_FALSE(store->getPurgeWatermark().known);
	store->purge(nullptr, false, cutoffOneHourAgo());
	EXPECT_FALSE(std::filesystem::exists(segment(1)));
	EXPECT_FALSE(store->getPurgeWatermark().known);
	store->close();

	auto record = recordOnDisk(storePath);
	EXPECT_FALSE(record.known);
	EXPECT_EQ(record.watermark, 5000);
	EXPECT_FALSE(loadStore()->getPurgeWatermark().known);
}

// An existing directory without a record is not proof of a new store: an older release can empty
// a store, and its downgrade procedure removes txn.state.
TEST_F(PurgeWatermarkTest, EmptyDirectoryWithoutARecordIsUnknown) {
	std::filesystem::create_directories(storePath);
	EXPECT_FALSE(loadStore()->getPurgeWatermark().known);
}

// After a record was written, files deleted by something that did not update it (an older
// release's retention) are detected.
TEST_F(PurgeWatermarkTest, UnrecordedDeletionAfterTheRecordIsDetected) {
	{
		auto writer = createStore();
		writeOutOfOrderSegments(*writer);
		writer->close();
	}
	std::filesystem::remove(segment(1));
	EXPECT_FALSE(loadStore()->getPurgeWatermark().known);
}

TEST_F(PurgeWatermarkTest, UnrecordedInteriorDeletionIsDetected) {
	{
		auto writer = createStore();
		writeOutOfOrderSegments(*writer);
		writer->close();
	}
	std::filesystem::remove(segment(2));
	EXPECT_FALSE(loadStore()->getPurgeWatermark().known);
}

TEST_F(PurgeWatermarkTest, ReusedSequenceSpaceIsUnknown) {
	{
		auto writer = createStore();
		write(*writer, 1000);
		writer->close();
	}
	TransactionLogPurgeRecord record;
	record.known = true;
	record.watermark = 5000;
	record.throughSequence = 5;
	record.generation = 9;
	ASSERT_TRUE(rocksdb_js::writeTransactionLogPurgeRecord(storePath, record));
	EXPECT_FALSE(loadStore()->getPurgeWatermark().known);
}

// A segment this process wrote that disappears out of band is still accounted for: every key in it
// passed through the writer. One an earlier process wrote is not.
TEST_F(PurgeWatermarkTest, MissingSegmentIsAccountedOnlyWhenItsKeysAreKnown) {
#ifdef _WIN32
	GTEST_SKIP() << "a segment the store holds open cannot be deleted out of band on Windows";
#endif
	auto store = createStore();
	writeOutOfOrderSegments(*store);
	std::filesystem::remove(segment(1));
	store->purge(nullptr, false, cutoffOneHourAgo());
	auto watermark = store->getPurgeWatermark();
	ASSERT_TRUE(watermark.known);
	EXPECT_EQ(watermark.purgedThrough, 5000);
	store->close();

	auto reloaded = loadStore();
	ASSERT_TRUE(reloaded->getPurgeWatermark().known);
	std::filesystem::remove(segment(2));
	reloaded->purge(nullptr, false, cutoffOneHourAgo());
	EXPECT_FALSE(reloaded->getPurgeWatermark().known);
	reloaded->close();
}

// A segment whose keys cannot be proven is still purged, so corruption does not stall retention,
// and the watermark becomes unknown.
TEST_F(PurgeWatermarkTest, CorruptInheritedSegmentMakesItUnknown) {
	{
		auto writer = createStore();
		writeOutOfOrderSegments(*writer);
		writer->close();
	}
	{
		// zero the first entry's length: a mid-file break with a valid entry after it
		std::fstream file(segment(1), std::ios::binary | std::ios::in | std::ios::out);
		file.seekp(TRANSACTION_LOG_FILE_HEADER_SIZE + 8);
		const char zeros[4] = { 0, 0, 0, 0 };
		file.write(zeros, sizeof(zeros));
	}
	ageSegment(1);
	auto store = loadStore();
	store->purge(nullptr, false, cutoffOneHourAgo());
	EXPECT_FALSE(std::filesystem::exists(segment(1)));
	EXPECT_FALSE(store->getPurgeWatermark().known);
	store->close();
}

// A crash during the header write leaves a file with no entries; it must not stall retention.
TEST_F(PurgeWatermarkTest, HeaderlessInheritedFileIsPurgedWithNoKeys) {
	{
		auto writer = createStore();
		writeOutOfOrderSegments(*writer);
		writer->close();
	}
	std::filesystem::resize_file(segment(1), 5);
	ageSegment(1);
	ageSegment(2);
	auto store = loadStore();
	store->purge(nullptr, false, cutoffOneHourAgo());
	EXPECT_FALSE(std::filesystem::exists(segment(1)));
	EXPECT_FALSE(std::filesystem::exists(segment(2)));
	auto watermark = store->getPurgeWatermark();
	ASSERT_TRUE(watermark.known);
	EXPECT_EQ(watermark.purgedThrough, 3000);
	store->close();
}

TEST_F(PurgeWatermarkTest, ExhaustedScanBudgetKeepsTheSegment) {
	{
		auto writer = createStore();
		writeOutOfOrderSegments(*writer);
		writer->close();
	}
	ageSegment(1);
	auto store = loadStore();
	TransactionLogStore::purgeScanBudgetMsForTests.store(0);
	store->purge(nullptr, false, cutoffOneHourAgo());
	EXPECT_TRUE(std::filesystem::exists(segment(1)));
	EXPECT_EQ(store->getPurgeWatermark().purgedThrough, 0);

	TransactionLogStore::purgeScanBudgetMsForTests.store(-1);
	store->purge(nullptr, false, cutoffOneHourAgo());
	EXPECT_FALSE(std::filesystem::exists(segment(1)));
	EXPECT_EQ(store->getPurgeWatermark().purgedThrough, 5000);
	store->close();
}

TEST_F(PurgeWatermarkTest, CurrentSegmentScanIsKeptOnlyWhileUnchanged) {
	{
		auto writer = createStore();
		write(*writer, 7000);
		writer->close();
	}
	auto store = loadStore();
	auto file = store->sequenceFiles.at(1);
	double maxKey = 0;
	EXPECT_FALSE(file->knownMaxEntryKey(maxKey));
	file->recordScannedMaxKey(7000, file->size.load());
	ASSERT_TRUE(file->knownMaxEntryKey(maxKey));
	EXPECT_EQ(maxKey, 7000);
	write(*store, 9000); // still segment 1
	ASSERT_EQ(store->currentSequenceNumber.load(), 1u);
	EXPECT_FALSE(file->knownMaxEntryKey(maxKey));
	store->close();
}

// A record behind the disk (an unreadable load, or a store built over a directory it did not load)
// takes a generation above the disk's and keeps the disk's higher watermark.
TEST_F(PurgeWatermarkTest, WritesNeverFallBehindTheDisk) {
	std::filesystem::create_directories(storePath);
	TransactionLogPurgeRecord onDisk;
	onDisk.known = true;
	onDisk.watermark = 5000;
	onDisk.throughSequence = 3;
	onDisk.generation = 5;
	ASSERT_TRUE(rocksdb_js::writeTransactionLogPurgeRecord(storePath, onDisk));

	auto store = std::make_shared<TransactionLogStore>("foo", storePath, MAX_FILE_SIZE, std::chrono::milliseconds(0), 0);
	// The first attempt's slot reaches the page cache, then its sync fails; the retry keeps the
	// disk's watermark too.
	store->failPurgeRecordWriteForTests = [&]() {
		TransactionLogPurgeRecord attempted;
		attempted.watermark = 5000;
		attempted.throughSequence = 3;
		attempted.generation = 6;
		EXPECT_TRUE(rocksdb_js::writeTransactionLogPurgeRecord(storePath, attempted));
		return true;
	};
	EXPECT_FALSE(store->invalidatePurgeRecord());
	store->failPurgeRecordWriteForTests = nullptr;
	ASSERT_TRUE(store->invalidatePurgeRecord());
	auto record = recordOnDisk(storePath);
	EXPECT_FALSE(record.known);
	EXPECT_EQ(record.generation, 6u);
	EXPECT_EQ(record.watermark, 5000);
	EXPECT_EQ(record.throughSequence, 3u);
	store->close();
}

// An unreadable record proves nothing: unknown for this open, and the file is left as it is.
TEST_F(PurgeWatermarkTest, UnreadableRecordIsUnknownAndNotOverwritten) {
	{
		auto writer = createStore();
		write(*writer, 1000);
		writer->close();
	}
	std::filesystem::remove(storePath / TRANSACTION_LOG_PURGE_RECORD_NAME);
	std::filesystem::create_directory(storePath / TRANSACTION_LOG_PURGE_RECORD_NAME);
	TransactionLogPurgeRecord ignored;
	ASSERT_EQ(rocksdb_js::readTransactionLogPurgeRecord(storePath, ignored), TransactionLogPurgeRecordState::Unreadable);
	auto store = loadStore();
	EXPECT_FALSE(store->getPurgeWatermark().known);
	store->close();
	EXPECT_TRUE(std::filesystem::is_directory(storePath / TRANSACTION_LOG_PURGE_RECORD_NAME));
}

TEST_F(PurgeWatermarkTest, BackupOfAStoreMissingASegmentIsUnknown) {
	{
		auto writer = createStore();
		writeOutOfOrderSegments(*writer);
		writer->close();
	}
	auto store = loadStore();
	std::filesystem::remove(segment(1));
	TransactionLogPurgeRecord captured;
	bool found = false;
	for (const auto& entry : store->snapshotForBackup(nullptr, false)) {
		if (entry.relativeName == TRANSACTION_LOG_PURGE_RECORD_NAME) {
			ASSERT_EQ(rocksdb_js::decodeTransactionLogPurgeRecordImage(
				entry.inlineContents.data(), entry.inlineContents.size(), captured), TransactionLogPurgeRecordState::Valid);
			found = true;
		}
	}
	ASSERT_TRUE(found);
	EXPECT_FALSE(captured.known);
	store->close();
}

TEST_F(PurgeWatermarkTest, BackupThatOmitsAMalformedSegmentIsUnknown) {
	{
		auto writer = createStore();
		writeOutOfOrderSegments(*writer);
		writer->close();
	}
	{
		std::fstream file(segment(1), std::ios::binary | std::ios::in | std::ios::out);
		file.write("\0\0\0\0", 4);
	}
	auto store = loadStore();
	TransactionLogPurgeRecord captured;
	bool sawSegment1 = false;
	for (const auto& entry : store->snapshotForBackup(nullptr, false)) {
		sawSegment1 = sawSegment1 || entry.relativeName == "1.txnlog";
		if (entry.relativeName == TRANSACTION_LOG_PURGE_RECORD_NAME) {
			ASSERT_EQ(rocksdb_js::decodeTransactionLogPurgeRecordImage(
				entry.inlineContents.data(), entry.inlineContents.size(), captured), TransactionLogPurgeRecordState::Valid);
		}
	}
	EXPECT_FALSE(sawSegment1);
	EXPECT_FALSE(captured.known);
	store->close();
}

TEST_F(PurgeWatermarkTest, WritableOpenRepairsADamagedSlot) {
	{
		auto writer = createStore();
		writeOutOfOrderSegments(*writer);
		writer->close();
	}
	auto current = recordOnDisk(storePath).generation;
	{
		std::fstream file(storePath / TRANSACTION_LOG_PURGE_RECORD_NAME, std::ios::binary | std::ios::in | std::ios::out);
		file.seekp(((current + 1) & 1) ? rocksdb_js::TRANSACTION_LOG_PURGE_RECORD_SLOT_STRIDE : 0);
		file.write("\xff\xff\xff\xff", 4);
	}
	auto store = loadStore();
	EXPECT_TRUE(store->getPurgeWatermark().known);
	store->close();
	auto reader = loadStore(std::chrono::milliseconds(0), /*readOnly=*/true);
	EXPECT_TRUE(reader->getPurgeWatermark().known);
	reader->close();
}

TEST_F(PurgeWatermarkTest, ReadOnlyStoreRereadsTheRecord) {
	auto writer = createStore();
	writeOutOfOrderSegments(*writer);
	auto reader = loadStore(std::chrono::milliseconds(0), /*readOnly=*/true);
	ASSERT_TRUE(reader->getPurgeWatermark().known);
	EXPECT_EQ(reader->getPurgeWatermark().purgedThrough, 0);

	ageSegment(1);
	writer->purge(nullptr, false, cutoffOneHourAgo());
	EXPECT_EQ(reader->getPurgeWatermark().purgedThrough, 5000);

	// The newest slot lost after its purge ran: the older one does not cover that purge.
	auto newest = recordOnDisk(storePath).generation;
	{
		std::fstream file(storePath / TRANSACTION_LOG_PURGE_RECORD_NAME, std::ios::binary | std::ios::in | std::ios::out);
		file.seekp((newest & 1) ? rocksdb_js::TRANSACTION_LOG_PURGE_RECORD_SLOT_STRIDE : 0);
		file.write("\xff\xff\xff\xff", 4);
	}
	EXPECT_FALSE(reader->getPurgeWatermark().known);
	reader->close();
	writer->close();
}

TEST_F(PurgeWatermarkTest, ReadOnlyStoreDistrustsADamagedNewerSlot) {
	auto writer = createStore();
	writeOutOfOrderSegments(*writer);
	auto reader = loadStore(std::chrono::milliseconds(0), /*readOnly=*/true);
	ageSegment(1);
	writer->purge(nullptr, false, cutoffOneHourAgo());
	auto newest = recordOnDisk(storePath).generation;
	{
		std::fstream file(storePath / TRANSACTION_LOG_PURGE_RECORD_NAME, std::ios::binary | std::ios::in | std::ios::out);
		file.seekp((newest & 1) ? rocksdb_js::TRANSACTION_LOG_PURGE_RECORD_SLOT_STRIDE : 0);
		file.write("\xff\xff\xff\xff", 4);
	}
	EXPECT_FALSE(reader->getPurgeWatermark().known);
	reader->close();
	writer->close();
}

// A partial directory listing proves nothing either way: unknown for this open, and not persisted.
TEST_F(PurgeWatermarkTest, IncompleteDiscoveryIsUnknownWithoutPersistingIt) {
	{
		auto writer = createStore();
		writeOutOfOrderSegments(*writer);
		writer->close();
	}
	std::ofstream(storePath / "not-a-sequence.txnlog") << "x";
	auto store = loadStore();
	EXPECT_FALSE(store->getPurgeWatermark().known);
	store->close();
	EXPECT_TRUE(recordOnDisk(storePath).known);
}

// Destroy marks the record unknown before it removes anything, since it may stop part-way.
TEST_F(PurgeWatermarkTest, InvalidationIsDurable) {
	auto store = createStore();
	writeOutOfOrderSegments(*store);
	ASSERT_TRUE(store->invalidatePurgeRecord());
	EXPECT_FALSE(store->getPurgeWatermark().known);
	// A retry writes a fresh slot: an earlier unknown write may not have reached the disk.
	auto generation = recordOnDisk(storePath).generation;
	ASSERT_TRUE(store->invalidatePurgeRecord());
	EXPECT_EQ(recordOnDisk(storePath).generation, generation + 1);
	store->close();
	EXPECT_FALSE(recordOnDisk(storePath).known);
	EXPECT_FALSE(loadStore()->getPurgeWatermark().known);
}

// A store 2.13.0 wrote has no record. Its first writable open starts one at the oldest surviving
// segment; a caller vouches for what 2.13.0 deleted before that.
TEST_F(PurgeWatermarkTest, Release213StoreAdoptsABaseline) {
	copyRelease213Store();
	auto store = loadStore();
	EXPECT_FALSE(store->getPurgeWatermark().known);
	auto record = recordOnDisk(storePath);
	EXPECT_FALSE(record.known);
	EXPECT_TRUE(record.tracked);
	EXPECT_EQ(record.throughSequence, 1u);
	EXPECT_EQ(record.watermark, 0);

	auto adopted = store->setPurgeBaseline(RELEASE_213_AHEAD_1);
	ASSERT_TRUE(adopted.known);
	EXPECT_EQ(adopted.purgedThrough, RELEASE_213_AHEAD_1);
	EXPECT_EQ(store->getPurgeWatermark().purgedThrough, RELEASE_213_AHEAD_1);
	store->close();

	EXPECT_TRUE(recordOnDisk(storePath).known);
	auto reloaded = loadStore();
	auto watermark = reloaded->getPurgeWatermark();
	ASSERT_TRUE(watermark.known);
	EXPECT_EQ(watermark.purgedThrough, RELEASE_213_AHEAD_1);
	reloaded->close();
}

// Whatever the caller asserts, the watermark stays at or above every key this release deleted,
// including one minted ahead of the clock in a segment old enough to purge by age.
TEST_F(PurgeWatermarkTest, BaselineNeverHidesAPurgedKeyAheadOfTheClock) {
	copyRelease213Store();
	ageSegment(2);
	auto store = loadStore(std::chrono::milliseconds(60 * 60 * 1000));
	EXPECT_FALSE(std::filesystem::exists(segment(2)));
	EXPECT_FALSE(store->getPurgeWatermark().known);
	EXPECT_EQ(recordOnDisk(storePath).watermark, RELEASE_213_AHEAD_2);

	auto adopted = store->setPurgeBaseline(3000);
	ASSERT_TRUE(adopted.known);
	EXPECT_EQ(adopted.purgedThrough, RELEASE_213_AHEAD_2);
	store->close();
}

TEST_F(PurgeWatermarkTest, PurgeAfterTheBaselineStillRaisesIt) {
	copyRelease213Store();
	auto store = loadStore();
	ASSERT_TRUE(store->setPurgeBaseline(3000).known);
	ageSegment(2);
	store->purge(nullptr, false, cutoffOneHourAgo());
	EXPECT_FALSE(std::filesystem::exists(segment(2)));
	auto watermark = store->getPurgeWatermark();
	ASSERT_TRUE(watermark.known);
	EXPECT_EQ(watermark.purgedThrough, RELEASE_213_AHEAD_2);
	store->close();
}

// The flag and the value share one slot: a write that fails, tears, or never reaches the disk leaves
// the previous, unknown record, never a known one with a lower value.
TEST_F(PurgeWatermarkTest, FailedBaselineWriteLeavesItUnknown) {
	copyRelease213Store();
	auto store = loadStore();
	auto durable = recordOnDisk(storePath).generation;
	store->failPurgeRecordWriteForTests = []() { return true; };
	EXPECT_THROW(store->setPurgeBaseline(5000), rocksdb_js::DBException);
	EXPECT_FALSE(store->getPurgeWatermark().known);
	EXPECT_EQ(recordOnDisk(storePath).generation, durable);
	store->failPurgeRecordWriteForTests = nullptr;
	store->close();

	auto reloaded = loadStore();
	EXPECT_FALSE(reloaded->getPurgeWatermark().known);
	EXPECT_TRUE(reloaded->setPurgeBaseline(5000).known);
	reloaded->close();
}

TEST_F(PurgeWatermarkTest, TornBaselineWriteLeavesItUnknown) {
	copyRelease213Store();
	auto store = loadStore();
	auto durable = recordOnDisk(storePath).generation;
	store->failPurgeRecordWriteForTests = [&]() {
		TransactionLogPurgeRecord attempted;
		attempted.known = true;
		attempted.tracked = true;
		attempted.watermark = 5000;
		attempted.throughSequence = 1;
		attempted.generation = durable + 1;
		EXPECT_TRUE(rocksdb_js::writeTransactionLogPurgeRecord(storePath, attempted));
		std::fstream file(storePath / TRANSACTION_LOG_PURGE_RECORD_NAME, std::ios::binary | std::ios::in | std::ios::out);
		file.seekp(((durable + 1) & 1) ? rocksdb_js::TRANSACTION_LOG_PURGE_RECORD_SLOT_STRIDE + 20 : 20);
		file.write("\xff\xff", 2);
		return true;
	};
	EXPECT_THROW(store->setPurgeBaseline(5000), rocksdb_js::DBException);
	store->failPurgeRecordWriteForTests = nullptr;
	store->close();

	auto record = recordOnDisk(storePath);
	EXPECT_EQ(record.generation, durable);
	EXPECT_FALSE(record.known);
	EXPECT_FALSE(loadStore()->getPurgeWatermark().known);
}

// A write whose sync failed may still reach the disk later. The store's next write retries that slot
// with what memory holds, so the unsynced adoption is overwritten rather than left to surface.
TEST_F(PurgeWatermarkTest, UnsyncedBaselineIsOverwrittenByTheNextWrite) {
	copyRelease213Store();
	auto store = loadStore();
	auto durable = recordOnDisk(storePath).generation;
	store->failPurgeRecordWriteForTests = [&]() {
		TransactionLogPurgeRecord attempted;
		attempted.known = true;
		attempted.tracked = true;
		attempted.watermark = 5000;
		attempted.throughSequence = 1;
		attempted.generation = durable + 1;
		EXPECT_TRUE(rocksdb_js::writeTransactionLogPurgeRecord(storePath, attempted));
		return true;
	};
	auto durableSlot = readSlot(durable);
	EXPECT_THROW(store->setPurgeBaseline(5000), rocksdb_js::DBException);
	store->failPurgeRecordWriteForTests = nullptr;
	EXPECT_FALSE(store->getPurgeWatermark().known);

	ageSegment(2);
	store->purge(nullptr, false, cutoffOneHourAgo());
	EXPECT_FALSE(std::filesystem::exists(segment(2)));
	EXPECT_EQ(readSlot(durable), durableSlot);
	auto record = recordOnDisk(storePath);
	EXPECT_EQ(record.generation, durable + 1);
	EXPECT_FALSE(record.known);
	EXPECT_TRUE(record.tracked);
	EXPECT_EQ(record.watermark, RELEASE_213_AHEAD_2);
	store->close();
}

// Retrying the adoption also rewrites the failed write's slot, never the last durable one.
TEST_F(PurgeWatermarkTest, BaselineRetryKeepsTheDurableSlot) {
	copyRelease213Store();
	auto store = loadStore();
	auto durable = recordOnDisk(storePath).generation;
	auto durableSlot = readSlot(durable);
	store->failPurgeRecordWriteForTests = [&]() {
		TransactionLogPurgeRecord attempted;
		attempted.known = true;
		attempted.tracked = true;
		attempted.watermark = 4000;
		attempted.throughSequence = 1;
		attempted.generation = durable + 1;
		EXPECT_TRUE(rocksdb_js::writeTransactionLogPurgeRecord(storePath, attempted));
		return true;
	};
	EXPECT_THROW(store->setPurgeBaseline(4000), rocksdb_js::DBException);
	store->failPurgeRecordWriteForTests = nullptr;
	auto watermark = store->setPurgeBaseline(5000);
	ASSERT_TRUE(watermark.known);
	EXPECT_EQ(watermark.purgedThrough, 5000);
	EXPECT_EQ(readSlot(durable), durableSlot);
	EXPECT_EQ(recordOnDisk(storePath).generation, durable + 1);
	store->close();
}

TEST_F(PurgeWatermarkTest, BaselineLeavesAKnownWatermarkAsItIs) {
	auto store = createStore();
	writeOutOfOrderSegments(*store);
	ageSegment(1);
	store->purge(nullptr, false, cutoffOneHourAgo());
	auto generation = recordOnDisk(storePath).generation;
	auto watermark = store->setPurgeBaseline(9000);
	ASSERT_TRUE(watermark.known);
	EXPECT_EQ(watermark.purgedThrough, 5000);
	EXPECT_EQ(recordOnDisk(storePath).generation, generation);
	store->close();
}

// A segment whose keys could not be proven was deleted after the record began: no baseline covers it.
TEST_F(PurgeWatermarkTest, BaselineIsRefusedAfterACorruptSegmentWasPurged) {
	copyRelease213Store();
	{
		std::fstream file(segment(2), std::ios::binary | std::ios::in | std::ios::out);
		file.seekp(TRANSACTION_LOG_FILE_HEADER_SIZE + 8);
		const char zeros[4] = { 0, 0, 0, 0 };
		file.write(zeros, sizeof(zeros));
	}
	auto store = loadStore();
	ageSegment(2);
	store->purge(nullptr, false, cutoffOneHourAgo());
	EXPECT_FALSE(std::filesystem::exists(segment(2)));
	EXPECT_FALSE(store->setPurgeBaseline(RELEASE_213_AHEAD_2).known);
	store->close();
	EXPECT_FALSE(recordOnDisk(storePath).tracked);
	EXPECT_FALSE(loadStore()->setPurgeBaseline(RELEASE_213_AHEAD_2).known);
}

// After the record began, something that does not update it (an older release) deleted a segment.
TEST_F(PurgeWatermarkTest, BaselineIsRefusedAfterAnUnrecordedDeletion) {
	copyRelease213Store();
	loadStore()->close();
	std::filesystem::remove(segment(2));
	auto store = loadStore();
	EXPECT_FALSE(store->setPurgeBaseline(RELEASE_213_AHEAD_2).known);
	store->close();
	EXPECT_FALSE(recordOnDisk(storePath).tracked);
}

// An interior hole at the first open is not a prefix an older release's retention left, so the record
// cannot start below it.
TEST_F(PurgeWatermarkTest, BaselineIsRefusedForAnInteriorHoleAtTheFirstOpen) {
	{
		auto writer = createStore();
		writeOutOfOrderSegments(*writer);
		writer->close();
	}
	std::filesystem::remove(storePath / TRANSACTION_LOG_PURGE_RECORD_NAME);
	std::filesystem::remove(segment(2));
	auto store = loadStore();
	EXPECT_FALSE(store->setPurgeBaseline(9000).known);
	store->close();
	EXPECT_FALSE(recordOnDisk(storePath).tracked);
}

// 2.14.0 wrote the same unknown record for a store it found without one as for a corrupt purge,
// so its records cannot tell the caller's baseline would cover everything.
TEST_F(PurgeWatermarkTest, BaselineIsRefusedForARelease214UnknownRecord) {
	copyRelease213Store();
	TransactionLogPurgeRecord release214;
	release214.generation = 1;
	ASSERT_TRUE(rocksdb_js::writeTransactionLogPurgeRecord(storePath, release214));
	auto store = loadStore();
	EXPECT_FALSE(store->setPurgeBaseline(RELEASE_213_AHEAD_2).known);
	store->close();
}

TEST_F(PurgeWatermarkTest, BaselineIsRefusedWhenTheRecordIsUnreadable) {
	copyRelease213Store();
	std::filesystem::create_directory(storePath / TRANSACTION_LOG_PURGE_RECORD_NAME);
	auto store = loadStore();
	EXPECT_FALSE(store->setPurgeBaseline(RELEASE_213_AHEAD_2).known);
	store->close();
	EXPECT_TRUE(std::filesystem::is_directory(storePath / TRANSACTION_LOG_PURGE_RECORD_NAME));
}

TEST_F(PurgeWatermarkTest, BaselineIsRefusedWhenDiscoveryIsIncomplete) {
	copyRelease213Store();
	std::ofstream(storePath / "not-a-sequence.txnlog") << "x";
	auto store = loadStore();
	EXPECT_FALSE(store->setPurgeBaseline(RELEASE_213_AHEAD_2).known);
	store->close();
	EXPECT_FALSE(std::filesystem::exists(storePath / TRANSACTION_LOG_PURGE_RECORD_NAME));
}

// A follower vouches only for a record it found at open: one that opened before the writer started
// the record reports unknown until it reopens.
TEST_F(PurgeWatermarkTest, ReadOnlyStoreSeesABaselineOnlyForARecordItOpenedWith) {
	copyRelease213Store();
	auto early = loadStore(std::chrono::milliseconds(0), /*readOnly=*/true);
	auto writer = loadStore();
	auto reader = loadStore(std::chrono::milliseconds(0), /*readOnly=*/true);
	EXPECT_FALSE(reader->getPurgeWatermark().known);
	ASSERT_TRUE(writer->setPurgeBaseline(5000).known);
	auto watermark = reader->getPurgeWatermark();
	ASSERT_TRUE(watermark.known);
	EXPECT_EQ(watermark.purgedThrough, 5000);
	EXPECT_FALSE(early->getPurgeWatermark().known);
	early->close();
	reader->close();
	writer->close();
	EXPECT_TRUE(loadStore(std::chrono::milliseconds(0), /*readOnly=*/true)->getPurgeWatermark().known);
}

// A purge that chose the current segment of a store not yet given a baseline, then stopped before the
// unlink: the writer starts past it, so the store stays adoptable and no key lands in a covered sequence.
TEST_F(PurgeWatermarkTest, CrashBeforeUnlinkingTheCurrentSegmentStaysAdoptable) {
	copyRelease213Store();
	auto store = loadStore();
	ageSegment(2);
	ageSegment(3);
	auto crashImage = root / "crash";
	store->afterPurgeRecordPersistedForTests = [&]() {
		std::filesystem::copy(storePath, crashImage, std::filesystem::copy_options::recursive);
	};
	store->purge(nullptr, false, cutoffOneHourAgo());
	store->afterPurgeRecordPersistedForTests = nullptr;
	EXPECT_FALSE(std::filesystem::exists(segment(3)));
	store->close();

	ASSERT_TRUE(std::filesystem::exists(crashImage / "3.txnlog"));
	auto recovered = TransactionLogStore::load(crashImage, MAX_FILE_SIZE, std::chrono::milliseconds(0), 0, false);
	EXPECT_EQ(recovered->currentSequenceNumber.load(), 4u);
	auto watermark = recovered->setPurgeBaseline(3000);
	ASSERT_TRUE(watermark.known);
	EXPECT_EQ(watermark.purgedThrough, RELEASE_213_AHEAD_2);
	write(*recovered, 9000);
	EXPECT_TRUE(std::filesystem::exists(crashImage / "4.txnlog"));
	recovered->close();
}

TEST_F(PurgeWatermarkTest, BackupOfATrackedStoreMissingASegmentIsNotTracked) {
	copyRelease213Store();
	auto store = loadStore();
	std::filesystem::remove(segment(2));
	TransactionLogPurgeRecord captured;
	bool found = false;
	for (const auto& entry : store->snapshotForBackup(nullptr, false)) {
		if (entry.relativeName == TRANSACTION_LOG_PURGE_RECORD_NAME) {
			ASSERT_EQ(rocksdb_js::decodeTransactionLogPurgeRecordImage(
				entry.inlineContents.data(), entry.inlineContents.size(), captured), TransactionLogPurgeRecordState::Valid);
			found = true;
		}
	}
	ASSERT_TRUE(found);
	EXPECT_FALSE(captured.tracked);
	store->close();
}

TEST_F(PurgeWatermarkTest, InvalidationClearsTracking) {
	copyRelease213Store();
	auto store = loadStore();
	ASSERT_TRUE(store->invalidatePurgeRecord());
	EXPECT_FALSE(store->setPurgeBaseline(5000).known);
	store->close();
	EXPECT_FALSE(recordOnDisk(storePath).tracked);
}

// A destroy that a bound transaction keeps open leaves the store alive, so its unknown record stays:
// an absent one would read as a store that predates the record, and become adoptable.
TEST_F(PurgeWatermarkTest, DestroyPurgeKeepsTheRecordOfASurvivingStore) {
	copyRelease213Store();
	auto store = loadStore();
	ASSERT_TRUE(store->invalidatePurgeRecord());
	store->purge(nullptr, /*all=*/true, 0);
	EXPECT_FALSE(std::filesystem::exists(segment(2)));
	EXPECT_FALSE(std::filesystem::exists(segment(3)));
	ASSERT_TRUE(std::filesystem::exists(storePath / TRANSACTION_LOG_PURGE_RECORD_NAME));
	write(*store, 9000);
	store->close();
	EXPECT_FALSE(loadStore()->setPurgeBaseline(9000).known);
}

// A destroy that stops part-way must leave a record behind: an absent one reads as a store that
// predates the record and accepts a baseline that never bounded the segments the destroy removed.
TEST_F(PurgeWatermarkTest, InvalidationWritesARecordWhereNoneExists) {
	copyRelease213Store();
	std::ofstream(storePath / "not-a-sequence.txnlog") << "x";
	auto store = loadStore();
	ASSERT_FALSE(std::filesystem::exists(storePath / TRANSACTION_LOG_PURGE_RECORD_NAME));
	ASSERT_TRUE(store->invalidatePurgeRecord());
	store->close();
	auto record = recordOnDisk(storePath);
	EXPECT_FALSE(record.known);
	EXPECT_FALSE(record.tracked);
	std::filesystem::remove(storePath / "not-a-sequence.txnlog");
	auto reloaded = loadStore();
	EXPECT_FALSE(reloaded->setPurgeBaseline(RELEASE_213_AHEAD_2).known);
	reloaded->close();
}

TEST_F(PurgeWatermarkTest, BaselineThrowsOnAClosingStore) {
	copyRelease213Store();
	auto store = loadStore();
	store->close();
	EXPECT_THROW(store->setPurgeBaseline(5000), rocksdb_js::DBException);
	EXPECT_FALSE(recordOnDisk(storePath).known);
}

// 2.13.0 deleted every segment; txn.state is all that names the sequence. Everything is before the
// record, so the baseline alone bounds it.
TEST_F(PurgeWatermarkTest, Release213StoreWithNoSegmentLeftAdoptsABaseline) {
	copyRelease213Store();
	std::filesystem::remove(segment(2));
	std::filesystem::remove(segment(3));
	auto store = loadStore();
	EXPECT_EQ(store->currentSequenceNumber.load(), 4u);
	EXPECT_EQ(recordOnDisk(storePath).throughSequence, 3u);
	ASSERT_TRUE(store->setPurgeBaseline(RELEASE_213_AHEAD_2).known);
	write(*store, 9000);
	store->close();
	auto watermark = loadStore()->getPurgeWatermark();
	ASSERT_TRUE(watermark.known);
	EXPECT_EQ(watermark.purgedThrough, RELEASE_213_AHEAD_2);
}

// A destroy that stops part-way must not leave the store without its record: an absent record reads
// as a store that predates it, which a caller's baseline could then make known.
TEST_F(PurgeWatermarkTest, StoreDirectoryRemovalDeletesTheRecordLast) {
#ifndef __linux__
	// libc++'s remove_all on macOS did not stop at the read-only child directory in CI.
	GTEST_SKIP() << "relies on a Linux directory permission to stop the removal";
#else
	if (::geteuid() == 0) {
		GTEST_SKIP() << "root ignores the directory permission that stops the removal";
	}
	copyRelease213Store();
	loadStore()->close();
	auto blocker = storePath / "blocker";
	std::filesystem::create_directory(blocker);
	std::ofstream(blocker / "file") << "x";
	std::filesystem::permissions(blocker, std::filesystem::perms::owner_read | std::filesystem::perms::owner_exec);
	EXPECT_THROW(rocksdb_js::removeTransactionLogStoreDirectory(storePath), std::filesystem::filesystem_error);
	std::filesystem::permissions(blocker, std::filesystem::perms::owner_all);
	EXPECT_TRUE(std::filesystem::exists(storePath / TRANSACTION_LOG_PURGE_RECORD_NAME));
	rocksdb_js::removeTransactionLogStoreDirectory(storePath);
	EXPECT_FALSE(std::filesystem::exists(storePath));
#endif
}

TEST(TransactionLogPurgeRecord, SlotRoundTripsAndRejectsInvalidValues) {
	TransactionLogPurgeRecord record;
	record.known = true;
	record.watermark = 1234.5;
	record.throughSequence = 7;
	record.generation = 3;
	char slot[rocksdb_js::TRANSACTION_LOG_PURGE_RECORD_SLOT_SIZE];
	rocksdb_js::encodeTransactionLogPurgeRecordSlot(slot, record);
	TransactionLogPurgeRecord decoded;
	ASSERT_TRUE(rocksdb_js::decodeTransactionLogPurgeRecordSlot(slot, decoded));
	EXPECT_TRUE(decoded.known);
	EXPECT_TRUE(decoded.tracked);
	EXPECT_EQ(decoded.watermark, 1234.5);
	EXPECT_EQ(decoded.throughSequence, 7u);
	EXPECT_EQ(decoded.generation, 3u);

	// a NaN watermark compares false against every resume key, so it must never decode
	record.watermark = std::numeric_limits<double>::quiet_NaN();
	rocksdb_js::encodeTransactionLogPurgeRecordSlot(slot, record);
	EXPECT_FALSE(rocksdb_js::decodeTransactionLogPurgeRecordSlot(slot, decoded));

	record.watermark = 1;
	rocksdb_js::encodeTransactionLogPurgeRecordSlot(slot, record);
	slot[5] |= 0x02; // a combination never written
	EXPECT_FALSE(rocksdb_js::decodeTransactionLogPurgeRecordSlot(slot, decoded));
	slot[5] = 0x04; // undefined flag bit
	EXPECT_FALSE(rocksdb_js::decodeTransactionLogPurgeRecordSlot(slot, decoded));

	// 2.14.0 accepts only 0x01 or 0: a known record keeps that byte, a tracked-only one does not
	record.known = true;
	record.tracked = true;
	rocksdb_js::encodeTransactionLogPurgeRecordSlot(slot, record);
	EXPECT_EQ(slot[5], 0x01);
	record.known = false;
	rocksdb_js::encodeTransactionLogPurgeRecordSlot(slot, record);
	EXPECT_EQ(slot[5], 0x02);
	ASSERT_TRUE(rocksdb_js::decodeTransactionLogPurgeRecordSlot(slot, decoded));
	EXPECT_FALSE(decoded.known);
	EXPECT_TRUE(decoded.tracked);

	char zeros[rocksdb_js::TRANSACTION_LOG_PURGE_RECORD_SLOT_SIZE] = {};
	EXPECT_FALSE(rocksdb_js::decodeTransactionLogPurgeRecordSlot(zeros, decoded));
}
