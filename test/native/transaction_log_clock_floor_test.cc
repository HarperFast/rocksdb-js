#include <gtest/gtest.h>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <limits>
#include <fstream>
#include <memory>
#include <string>
#include "core/encoding.h"
#include "core/platform.h"
#include "transaction_log/transaction_log_entry.h"
#include "transaction_log/transaction_log_file.h"
#include "transaction_log/transaction_log_store.h"

namespace {

// Keys are placed an hour past wherever the process clock already is, so
// neither the wall clock nor a floor raised by an earlier test can reach them;
// the seed alone decides the next claim. (Every load here raises the process
// floor; no other test in this binary reads the clock against wall time.)
double futureKey() {
	return rocksdb_js::getMonotonicTimestamp() + 3600.0 * 1000.0;
}

std::filesystem::path uniqueStorePath() {
	auto nonce = std::chrono::steady_clock::now().time_since_epoch().count();
	return std::filesystem::temp_directory_path() /
		("rocksdb-js-clock-floor-" + std::to_string(nonce)) / "store";
}

void writeSegment(const std::filesystem::path& logPath, uint32_t sequence,
	double headerTimestamp, std::initializer_list<double> batchKeys) {
	rocksdb_js::TransactionLogFile file(logPath, sequence, false);
	file.open(headerTimestamp);
	std::string payload = "entry";
	for (double key : batchKeys) {
		rocksdb_js::TransactionLogEntryBatch batch(key);
		batch.addEntry(std::make_unique<rocksdb_js::TransactionLogEntry>(
			nullptr, payload.data(), static_cast<uint32_t>(payload.size())));
		file.writeEntries(batch);
	}
	file.close();
}

std::shared_ptr<rocksdb_js::TransactionLogStore> loadStore(const std::filesystem::path& storePath) {
	return rocksdb_js::TransactionLogStore::load(storePath, 0, std::chrono::milliseconds(0), 0);
}

} // namespace

TEST(TransactionLogClockFloor, SeedsFromTheActiveSegmentsLargestKeyNotItsLast) {
	auto storePath = uniqueStorePath();
	std::filesystem::create_directories(storePath);
	// Batch keys are not monotonic in log order: a transaction that claimed its
	// timestamp earlier can commit after a later one.
	const double high = futureKey();
	writeSegment(storePath / "1.txnlog", 1, 0.0, { high - 5000.0, high, high - 1.0 });

	auto store = loadStore(storePath);
	ASSERT_NE(store, nullptr);
	EXPECT_EQ(store->latestTimestamp, high);
	EXPECT_GT(rocksdb_js::getMonotonicTimestamp(), high);
	store->close();
	std::filesystem::remove_all(storePath.parent_path());
}

TEST(TransactionLogClockFloor, SeedsFromARotatedSegmentsHeaderWord) {
	auto storePath = uniqueStorePath();
	std::filesystem::create_directories(storePath);
	// A rotation stamps the new segment's header with the store's latest key,
	// so a key that only ever lived in segment 1 is still visible from
	// segment 2's header without scanning segment 1's entries.
	const double high = futureKey();
	writeSegment(storePath / "1.txnlog", 1, 0.0, { high });
	writeSegment(storePath / "2.txnlog", 2, high, { high - 60000.0 });

	auto store = loadStore(storePath);
	ASSERT_NE(store, nullptr);
	EXPECT_EQ(store->currentSequenceNumber.load(std::memory_order_relaxed), 2u);
	EXPECT_EQ(store->latestTimestamp, high);
	EXPECT_GT(rocksdb_js::getMonotonicTimestamp(), high);
	store->close();
	std::filesystem::remove_all(storePath.parent_path());
}

TEST(TransactionLogClockFloor, NewSegmentsInheritTheSeededHeader) {
	auto storePath = uniqueStorePath();
	std::filesystem::create_directories(storePath);
	const double high = futureKey();
	writeSegment(storePath / "1.txnlog", 1, 0.0, { high });

	// Post-upgrade chain: a store loaded with a seeded latestTimestamp stamps it
	// into every segment it creates, so the newest header keeps bounding the
	// older segments' keys across restarts.
	auto store = loadStore(storePath);
	ASSERT_NE(store, nullptr);
	auto next = std::make_shared<rocksdb_js::TransactionLogFile>(storePath / "2.txnlog", 2, false);
	next->open(store->latestTimestamp);
	next->close();
	store->close();

	EXPECT_EQ(rocksdb_js::readTransactionLogFileHeaderTimestamp(storePath / "2.txnlog"), high);
	std::filesystem::remove_all(storePath.parent_path());
}

TEST(TransactionLogClockFloor, IgnoresNonFiniteKeysAndStillOpens) {
	auto storePath = uniqueStorePath();
	std::filesystem::create_directories(storePath);
	const double high = futureKey();
	// A pre-hardening writer could persist a non-finite key; it must neither
	// wedge the clock at infinity nor hide the real maximum.
	writeSegment(storePath / "1.txnlog", 1, std::numeric_limits<double>::infinity(),
		{ high, std::numeric_limits<double>::infinity(), std::numeric_limits<double>::quiet_NaN() });

	auto store = loadStore(storePath);
	ASSERT_NE(store, nullptr);
	EXPECT_EQ(store->latestTimestamp, high);
	EXPECT_GT(rocksdb_js::getMonotonicTimestamp(), high);
	EXPECT_LT(rocksdb_js::getMonotonicTimestamp(), rocksdb_js::MAX_TIMESTAMP_MS);
	store->close();
	std::filesystem::remove_all(storePath.parent_path());
}

TEST(TransactionLogClockFloor, HeaderReaderRejectsShortAndForeignFiles) {
	auto storePath = uniqueStorePath();
	std::filesystem::create_directories(storePath);
	{
		std::ofstream shortFile(storePath / "short.txnlog", std::ios::binary);
		shortFile.write("FOOW", 4);
	}
	{
		std::ofstream foreign(storePath / "foreign.txnlog", std::ios::binary);
		std::string bytes(TRANSACTION_LOG_FILE_HEADER_SIZE, '\0');
		foreign.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
	}
	EXPECT_THROW(rocksdb_js::readTransactionLogFileHeaderTimestamp(storePath / "short.txnlog"),
		rocksdb_js::TransactionLogFormatException);
	EXPECT_THROW(rocksdb_js::readTransactionLogFileHeaderTimestamp(storePath / "foreign.txnlog"),
		rocksdb_js::TransactionLogFormatException);
	EXPECT_THROW(rocksdb_js::readTransactionLogFileHeaderTimestamp(storePath / "missing.txnlog"),
		rocksdb_js::DBException);
	std::filesystem::remove_all(storePath.parent_path());
}

TEST(TransactionLogClockFloor, AFarKeyIsNeitherSeededNorAllowedToMaskTheRealMaximum) {
	auto storePath = uniqueStorePath();
	std::filesystem::create_directories(storePath);
	const double high = futureKey();
	const double far = rocksdb_js::MAX_TIMESTAMP_MS - 1.0;
	writeSegment(storePath / "1.txnlog", 1, 0.0, { high - 1.0, far, high });

	auto store = loadStore(storePath);
	ASSERT_NE(store, nullptr);
	EXPECT_EQ(store->latestTimestamp, high);
	EXPECT_GT(rocksdb_js::getMonotonicTimestamp(), high);
	EXPECT_LT(rocksdb_js::getMonotonicTimestamp(), high + 60.0 * 1000.0);
	store->close();
	std::filesystem::remove_all(storePath.parent_path());
}

TEST(TransactionLogClockFloor, TheWritePathBoundFollowsTheWallClock) {
	auto storePath = uniqueStorePath();
	std::filesystem::create_directories(storePath);
	auto store = loadStore(storePath);
	ASSERT_NE(store, nullptr);
	// A boot with the RTC at 1970 froze a bound the corrected wall clock is far
	// past; a key claimed after the correction is still accepted, so the header
	// chain keeps recording.
	store->plausibleTimestampBound = 1.0;
	const double key = rocksdb_js::getMonotonicTimestamp();
	EXPECT_TRUE(store->raiseLatestTimestamp(key));
	EXPECT_EQ(store->latestTimestamp, key);
	EXPECT_FALSE(store->raiseLatestTimestamp(
		rocksdb_js::getWallClockTimestamp() + rocksdb_js::MAX_CLOCK_FLOOR_SKEW_MS * 2));
	store->close();
	std::filesystem::remove_all(storePath.parent_path());
}

TEST(TransactionLogClockFloor, AFarHeaderWalksEverySegmentAPlausibleHeaderBeforeItCannotVouchFor) {
	auto storePath = uniqueStorePath();
	std::filesystem::create_directories(storePath);
	const double high = futureKey();
	const double far = rocksdb_js::MAX_TIMESTAMP_MS - 1.0;
	// Segment 2's header is plausible but older than the far one, so it is not
	// trusted to summarize segment 1; the key only segment 1 holds is found.
	writeSegment(storePath / "1.txnlog", 1, 0.0, { high });
	writeSegment(storePath / "2.txnlog", 2, high - 2.0, { high - 1.0 });
	writeSegment(storePath / "3.txnlog", 3, far, { high - 3.0 });

	auto store = loadStore(storePath);
	ASSERT_NE(store, nullptr);
	EXPECT_EQ(store->latestTimestamp, high);
	EXPECT_EQ(store->sequenceFiles.at(1)->maxEntryTimestamp.load(std::memory_order_relaxed), high);
	store->close();
	std::filesystem::remove_all(storePath.parent_path());
}

TEST(TransactionLogClockFloor, APlausibleHeaderNewerThanTheFarOneEndsTheWalk) {
	auto storePath = uniqueStorePath();
	std::filesystem::create_directories(storePath);
	const double high = futureKey();
	const double far = rocksdb_js::MAX_TIMESTAMP_MS - 1.0;
	// Segments 3 and 4 were created by rotations after a load that walked 1-2,
	// so 4's header already carries 3's key and nothing older is read.
	writeSegment(storePath / "1.txnlog", 1, 0.0, { high - 5.0 });
	writeSegment(storePath / "2.txnlog", 2, far, { high - 4.0 });
	writeSegment(storePath / "3.txnlog", 3, high - 2.0, { high });
	writeSegment(storePath / "4.txnlog", 4, high, { high - 1.0 });

	auto store = loadStore(storePath);
	ASSERT_NE(store, nullptr);
	EXPECT_EQ(store->latestTimestamp, high);
	for (uint32_t sequence = 1; sequence <= 3; sequence++) {
		EXPECT_EQ(store->sequenceFiles.at(sequence)->maxEntryTimestamp.load(std::memory_order_relaxed), 0.0);
	}
	store->close();
	std::filesystem::remove_all(storePath.parent_path());
}

TEST(TransactionLogClockFloor, AFarHeaderTriggersAScanOfTheSegmentsItSummarizes) {
	auto storePath = uniqueStorePath();
	std::filesystem::create_directories(storePath);
	const double high = futureKey();
	const double far = rocksdb_js::MAX_TIMESTAMP_MS - 1.0;
	// A pre-bound writer stamped the far key into segment 2's header; the real
	// maximum lives only in segment 1's entries.
	writeSegment(storePath / "1.txnlog", 1, 0.0, { high });
	writeSegment(storePath / "2.txnlog", 2, far, { high - 60000.0 });

	auto store = loadStore(storePath);
	ASSERT_NE(store, nullptr);
	EXPECT_EQ(store->latestTimestamp, high);
	EXPECT_GT(rocksdb_js::getMonotonicTimestamp(), high);
	EXPECT_LT(rocksdb_js::getMonotonicTimestamp(), high + 60.0 * 1000.0);

	// Heals: the next segment this store creates carries the plausible maximum.
	auto next = std::make_shared<rocksdb_js::TransactionLogFile>(storePath / "3.txnlog", 3, false);
	next->open(store->latestTimestamp);
	next->close();
	store->close();
	EXPECT_EQ(rocksdb_js::readTransactionLogFileHeaderTimestamp(storePath / "3.txnlog"), high);
	std::filesystem::remove_all(storePath.parent_path());
}

TEST(TransactionLogClockFloor, ARealRotationStampsTheLatestKeyIntoTheNewHeader) {
	auto storePath = uniqueStorePath();
	std::filesystem::create_directories(storePath);
	const double high = futureKey();
	std::string payload(2048, 'x');
	auto entry = [&]() {
		return std::make_unique<rocksdb_js::TransactionLogEntry>(
			nullptr, payload.data(), static_cast<uint32_t>(payload.size()));
	};
	{
		auto store = loadStore(storePath);
		ASSERT_NE(store, nullptr);
		store->maxFileSize = 1024;
		rocksdb_js::LogPosition position;
		rocksdb_js::TransactionLogEntryBatch first(high);
		first.addEntry(entry());
		store->writeBatch(first, position);
		rocksdb_js::TransactionLogEntryBatch second(high - 60000.0);
		second.addEntry(entry());
		store->writeBatch(second, position);
		store->close();
	}
	uint32_t segments = 0;
	uint32_t newest = 0;
	for (const auto& file : std::filesystem::directory_iterator(storePath)) {
		if (file.path().extension() == ".txnlog") {
			segments++;
			newest = std::max(newest, static_cast<uint32_t>(std::stoul(file.path().stem().string())));
		}
	}
	EXPECT_GE(segments, 2u);
	EXPECT_EQ(rocksdb_js::readTransactionLogFileHeaderTimestamp(
		storePath / (std::to_string(newest) + ".txnlog")), high);

	auto reopened = loadStore(storePath);
	ASSERT_NE(reopened, nullptr);
	EXPECT_EQ(reopened->latestTimestamp, high);
	reopened->close();
	std::filesystem::remove_all(storePath.parent_path());
}

TEST(TransactionLogClockFloor, EntriesPastAMidFileBreakMarkTheSeedIncomplete) {
	auto storePath = uniqueStorePath();
	std::filesystem::create_directories(storePath);
	const double high = futureKey();
	writeSegment(storePath / "1.txnlog", 1, 0.0, { high - 1.0 });
	{
		// A frame whose declared length overruns the file, then a long run of
		// intact entries keyed above everything before the break.
		std::ofstream tail(storePath / "1.txnlog", std::ios::binary | std::ios::app);
		char header[TRANSACTION_LOG_ENTRY_HEADER_SIZE];
		rocksdb_js::writeDoubleBE(header, high - 2.0);
		rocksdb_js::writeUint32BE(header + 8, 100000);
		rocksdb_js::writeUint8(header + 12, 1);
		tail.write(header, sizeof(header));
		tail.write("garbage!", 8);
		for (int i = 0; i < 12; ++i) {
			rocksdb_js::writeDoubleBE(header, high);
			rocksdb_js::writeUint32BE(header + 8, 4);
			rocksdb_js::writeUint8(header + 12, 1);
			tail.write(header, sizeof(header));
			tail.write("data", 4);
		}
	}

	auto store = loadStore(storePath);
	ASSERT_NE(store, nullptr);
	EXPECT_EQ(store->latestTimestamp, high - 1.0);
	EXPECT_FALSE(store->clockFloorComplete);
	store->close();
	std::filesystem::remove_all(storePath.parent_path());
}
