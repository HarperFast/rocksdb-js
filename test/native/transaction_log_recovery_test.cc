// Unit tests for scanTransactionLogForRecovery — the pure framing scan that
// open-time crash recovery uses to decide whether to truncate a torn tail,
// leave a mid-file corruption intact, or do nothing.

#include <gtest/gtest.h>
#include <cstdint>
#include <vector>
#include "core/encoding.h"
#include "transaction_log/transaction_log_file.h"
#include "transaction_log/transaction_log_recovery.h"

using rocksdb_js::countTransactionLogEntries;
using rocksdb_js::RecoveryScan;
using rocksdb_js::scanTransactionLogForRecovery;

namespace {

// Builds transaction-log file images for the scan to classify.
class LogImage {
public:
	LogImage() {
		// file header: token, version, timestamp
		appendU32(0x574f4f46);
		appendU8(1);
		appendF64(1.0);
	}

	// Append a well-formed entry whose declared length matches its data.
	LogImage& entry(uint32_t dataLen, uint8_t flags = 1) {
		return entryRaw(/*declaredLength=*/dataLen, /*actualDataLen=*/dataLen, flags);
	}

	// Append an entry header that declares `declaredLength` but only writes
	// `actualDataLen` data bytes (used to simulate a torn/partial entry).
	LogImage& entryRaw(uint32_t declaredLength, uint32_t actualDataLen, uint8_t flags = 1) {
		appendF64(2.0); // any non-zero timestamp
		appendU32(declaredLength);
		appendU8(flags);
		for (uint32_t i = 0; i < actualDataLen; ++i) {
			bytes.push_back(static_cast<char>(0xAB));
		}
		return *this;
	}

	// Append raw bytes (e.g. a partial header or zero padding).
	LogImage& raw(const std::vector<char>& extra) {
		bytes.insert(bytes.end(), extra.begin(), extra.end());
		return *this;
	}

	LogImage& zeros(uint32_t count) {
		bytes.insert(bytes.end(), count, '\0');
		return *this;
	}

	const char* data() const { return bytes.data(); }
	uint32_t size() const { return static_cast<uint32_t>(bytes.size()); }

private:
	void appendU8(uint8_t v) {
		char b[1];
		rocksdb_js::writeUint8(b, v);
		bytes.insert(bytes.end(), b, b + 1);
	}
	void appendU32(uint32_t v) {
		char b[4];
		rocksdb_js::writeUint32BE(b, v);
		bytes.insert(bytes.end(), b, b + 4);
	}
	void appendF64(double v) {
		char b[8];
		rocksdb_js::writeDoubleBE(b, v);
		bytes.insert(bytes.end(), b, b + 8);
	}

	std::vector<char> bytes;
};

} // namespace

TEST(TransactionLogRecovery, HeaderOnlyIsClean) {
	LogImage img; // just the 13-byte header
	auto scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::Clean);
	EXPECT_EQ(scan.validEnd, img.size());
}

TEST(TransactionLogRecovery, CleanFileEndingOnBoundary) {
	LogImage img;
	img.entry(10).entry(20).entry(30);
	auto scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::Clean);
	EXPECT_EQ(scan.validEnd, img.size());
}

TEST(TransactionLogRecovery, ZeroPaddedTailIsCleanAtPadStart) {
	LogImage img;
	img.entry(10).entry(20);
	uint32_t entriesEnd = img.size();
	img.zeros(64);
	auto scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::Clean);
	EXPECT_EQ(scan.validEnd, entriesEnd);
}

TEST(TransactionLogRecovery, TornTailDeclaredLengthOverruns) {
	LogImage img;
	img.entry(10).entry(20);
	uint32_t tornStart = img.size();
	// header claims 5000 bytes of data but only 12 are present before EOF
	img.entryRaw(/*declaredLength=*/5000, /*actualDataLen=*/12);
	auto scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::TruncateTail);
	EXPECT_EQ(scan.validEnd, tornStart);
}

TEST(TransactionLogRecovery, TornTailPartialHeader) {
	LogImage img;
	img.entry(10).entry(20);
	uint32_t entriesEnd = img.size();
	img.raw({ 0x42, 0x79, 0x05 }); // 3 stray bytes: fewer than a full entry header
	auto scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::TruncateTail);
	EXPECT_EQ(scan.validEnd, entriesEnd);
}

TEST(TransactionLogRecovery, LargeEntryExceedingRotationSizeIsValid) {
	// A single entry can legitimately exceed the rotation threshold (the first
	// entry in a fresh file is always written in full). The scan must treat it
	// as valid, not truncate it. 64 KiB here stands in for such an entry.
	LogImage img;
	img.entry(10).entry(64 * 1024).entry(20);
	auto scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::Clean);
	EXPECT_EQ(scan.validEnd, img.size());
}

TEST(TransactionLogRecovery, BrokenFrameThenLongValidRunIsMidFileCorruption) {
	LogImage img;
	img.entry(10).entry(20);
	uint32_t breakOffset = img.size();
	// a frame whose declared length overruns the file (garbage)
	img.entryRaw(/*declaredLength=*/100000, /*actualDataLen=*/8);
	// then a long run of well-formed entries (valid data resumes)
	for (int i = 0; i < 12; ++i) {
		img.entry(16);
	}
	auto scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::MidFileCorruption);
	EXPECT_EQ(scan.validEnd, breakOffset);
}

TEST(TransactionLogRecovery, BrokenFrameThenFewEntriesReachingEofIsNotTruncated) {
	// Regression for the resync false-negative: a mid-file break followed by
	// fewer than RESYNC_MIN_FRAMES valid entries that nonetheless reach EOF must
	// NOT be truncated (those trailing entries are committed). The "chain lands
	// exactly on EOF" signal protects them.
	LogImage img;
	img.entry(10).entry(20);
	uint32_t breakOffset = img.size();
	img.entryRaw(/*declaredLength=*/100000, /*actualDataLen=*/8);
	img.entry(16).entry(16).entry(16); // only 3 valid entries, ending exactly at EOF
	auto scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::MidFileCorruption);
	EXPECT_EQ(scan.validEnd, breakOffset);
}

TEST(TransactionLogRecovery, ZeroLengthFrameAtTailTruncates) {
	LogImage img;
	img.entry(10);
	uint32_t tornStart = img.size();
	// a zero-length entry is invalid framing; nothing valid follows
	img.entryRaw(/*declaredLength=*/0, /*actualDataLen=*/0);
	auto scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::TruncateTail);
	EXPECT_EQ(scan.validEnd, tornStart);
}

TEST(TransactionLogCount, HeaderOnlyHasNoEntries) {
	LogImage img; // just the 13-byte header
	EXPECT_EQ(countTransactionLogEntries(img.data(), img.size()), 0u);
}

TEST(TransactionLogCount, CountsEntriesEndingOnBoundary) {
	LogImage img;
	img.entry(10).entry(20).entry(30);
	EXPECT_EQ(countTransactionLogEntries(img.data(), img.size()), 3u);
}

TEST(TransactionLogCount, StopsAtZeroPaddedTail) {
	LogImage img;
	img.entry(10).entry(20);
	img.zeros(64); // trailing zero padding must not be counted as entries
	EXPECT_EQ(countTransactionLogEntries(img.data(), img.size()), 2u);
}

TEST(TransactionLogCount, StopsAtTornTail) {
	LogImage img;
	img.entry(10).entry(20);
	// header claims 5000 bytes of data but only 12 are present before EOF
	img.entryRaw(/*declaredLength=*/5000, /*actualDataLen=*/12);
	EXPECT_EQ(countTransactionLogEntries(img.data(), img.size()), 2u);
}

TEST(TransactionLogCount, StopsAtPartialHeader) {
	LogImage img;
	img.entry(10).entry(20);
	img.raw({ 0x42, 0x79, 0x05 }); // fewer than a full entry header remains
	EXPECT_EQ(countTransactionLogEntries(img.data(), img.size()), 2u);
}

TEST(TransactionLogCount, CountsLargeEntryExceedingRotationSize) {
	LogImage img;
	img.entry(10).entry(64 * 1024).entry(20);
	EXPECT_EQ(countTransactionLogEntries(img.data(), img.size()), 3u);
}
