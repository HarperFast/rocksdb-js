// Unit tests for scanTransactionLogForRecovery — the pure framing scan that
// open-time crash recovery uses to decide whether to truncate a torn tail,
// leave a mid-file corruption intact, or do nothing.

#include <gtest/gtest.h>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>
#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif
#include "core/encoding.h"
#include "core/exception.h"
#include "transaction_log/transaction_log_file.h"
#include "transaction_log/transaction_log_recovery.h"

using rocksdb_js::countTransactionLogEntries;
using rocksdb_js::DBException;
using rocksdb_js::findFramingResumeOffset;
using rocksdb_js::RecoveryScan;
using rocksdb_js::scanTransactionLogForRecovery;
using rocksdb_js::TransactionLogFile;

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

	// Append a well-formed entry whose declared length matches its data. Every
	// entry of a batch is stamped with the batch timestamp, so `timestamp` is what
	// distinguishes one interrupted transaction from several unflagged ones.
	LogImage& entry(uint32_t dataLen, uint8_t flags = 1, double timestamp = 2.0) {
		return entryRaw(/*declaredLength=*/dataLen, /*actualDataLen=*/dataLen, flags, timestamp);
	}

	// Append an entry header that declares `declaredLength` but only writes
	// `actualDataLen` data bytes (used to simulate a torn/partial entry).
	LogImage& entryRaw(uint32_t declaredLength, uint32_t actualDataLen, uint8_t flags = 1,
		double timestamp = 2.0) {
		appendF64(timestamp); // any non-zero timestamp
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
	// Grow-then-write instead of insert(end, ptr, ptr+N): GCC 12/13 emit a
	// -Wstringop-overflow false positive on the vector range-insert under
	// inlining (it assumes the pre-grow capacity while sizing the copy).
	void appendU8(uint8_t v) {
		size_t off = bytes.size();
		bytes.resize(off + 1);
		rocksdb_js::writeUint8(bytes.data() + off, v);
	}
	void appendU32(uint32_t v) {
		size_t off = bytes.size();
		bytes.resize(off + 4);
		rocksdb_js::writeUint32BE(bytes.data() + off, v);
	}
	void appendF64(double v) {
		size_t off = bytes.size();
		bytes.resize(off + 8);
		rocksdb_js::writeDoubleBE(bytes.data() + off, v);
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

TEST(TransactionLogRecovery, WatermarkAdvancesPastAMidFileBreak) {
	LogImage img;
	img.entry(10).entry(20);
	uint32_t breakOffset = img.size();
	img.entryRaw(/*declaredLength=*/100000, /*actualDataLen=*/8);
	for (int i = 0; i < 12; ++i) {
		img.entry(16);
	}
	auto scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::MidFileCorruption);
	EXPECT_EQ(scan.validEnd, breakOffset);
	EXPECT_EQ(scan.lastCompleteTransactionEnd, img.size());
	EXPECT_EQ(scan.unclosedTailEntries, 0u);
}

TEST(TransactionLogRecovery, WatermarkAdvancesPastAShortRunReachingEof) {
	LogImage img;
	img.entry(10).entry(20);
	uint32_t breakOffset = img.size();
	img.entryRaw(/*declaredLength=*/100000, /*actualDataLen=*/8);
	img.entry(16).entry(16).entry(16);
	auto scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::MidFileCorruption);
	EXPECT_EQ(scan.validEnd, breakOffset);
	EXPECT_EQ(scan.lastCompleteTransactionEnd, img.size());
}

// A pre-extended (Windows) segment ends in zero padding, so a short run after a
// break never lands on EOF; landing on the end of the nonzero bytes must count
// the same, or recovery truncates the run's committed entries.
TEST(TransactionLogRecovery, ShortRunReachingThePaddingIsNotTruncated) {
	LogImage img;
	img.entry(10).entry(20);
	uint32_t breakOffset = img.size();
	img.entryRaw(/*declaredLength=*/100000, /*actualDataLen=*/8);
	img.entry(16).entry(16).entry(16);
	uint32_t runEnd = img.size();
	img.zeros(4096);
	auto scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::MidFileCorruption);
	EXPECT_EQ(scan.validEnd, breakOffset);
	EXPECT_EQ(scan.lastCompleteTransactionEnd, runEnd);
}

TEST(TransactionLogRecovery, TornTailBeforeThePaddingIsStillTruncated) {
	LogImage img;
	img.entry(10).entry(20);
	uint32_t breakOffset = img.size();
	img.entryRaw(/*declaredLength=*/100000, /*actualDataLen=*/8);
	img.zeros(4096);
	auto scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::TruncateTail);
	EXPECT_EQ(scan.validEnd, breakOffset);
}

TEST(TransactionLogRecovery, WatermarkAdvancesPastMultipleBreaks) {
	LogImage img;
	img.entry(10);
	uint32_t firstBreak = img.size();
	img.entryRaw(/*declaredLength=*/0, /*actualDataLen=*/0);
	for (int i = 0; i < 12; ++i) {
		img.entry(16);
	}
	img.entryRaw(/*declaredLength=*/100000, /*actualDataLen=*/8);
	for (int i = 0; i < 12; ++i) {
		img.entry(16);
	}
	auto scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::MidFileCorruption);
	EXPECT_EQ(scan.validEnd, firstBreak);
	EXPECT_EQ(scan.lastCompleteTransactionEnd, img.size());
}

TEST(TransactionLogRecovery, FlaggedEntryAfterABreakClosesTheTornGroup) {
	// The break tore a multi-entry transaction. Its later entries are still on
	// disk, and the first flagged entry after the break advances the watermark
	// over all of them: readers see the tear through CorruptFrameError, not
	// through a watermark that hides everything behind it.
	LogImage img;
	img.entry(10, /*flags=*/1, /*timestamp=*/2.0);
	img.entry(20, /*flags=*/0, /*timestamp=*/3.0);
	uint32_t breakOffset = img.size();
	img.entryRaw(/*declaredLength=*/100000, /*actualDataLen=*/8, /*flags=*/0, /*timestamp=*/3.0);
	for (int i = 0; i < 11; ++i) {
		img.entry(16, /*flags=*/0, /*timestamp=*/3.0);
	}
	img.entry(16, /*flags=*/1, /*timestamp=*/3.0);
	auto scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::MidFileCorruption);
	EXPECT_EQ(scan.validEnd, breakOffset);
	EXPECT_EQ(scan.lastCompleteTransactionEnd, img.size());
}

TEST(TransactionLogRecovery, TornTailAfterAMidFileBreakStaysMidFileCorruption) {
	// A file with a mid-file break is never truncated, even when a torn tail
	// follows the resumed run: the watermark stops at the run's last boundary.
	LogImage img;
	img.entry(10);
	uint32_t breakOffset = img.size();
	img.entryRaw(/*declaredLength=*/100000, /*actualDataLen=*/8);
	for (int i = 0; i < 12; ++i) {
		img.entry(16);
	}
	uint32_t runEnd = img.size();
	img.entryRaw(/*declaredLength=*/5000, /*actualDataLen=*/12);
	auto scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::MidFileCorruption);
	EXPECT_EQ(scan.validEnd, breakOffset);
	EXPECT_EQ(scan.lastCompleteTransactionEnd, runEnd);
}

TEST(TransactionLogRecovery, ZeroPaddingAfterAMidFileBreakStaysMidFileCorruption) {
	LogImage img;
	img.entry(10);
	uint32_t breakOffset = img.size();
	img.entryRaw(/*declaredLength=*/100000, /*actualDataLen=*/8);
	for (int i = 0; i < 12; ++i) {
		img.entry(16);
	}
	uint32_t runEnd = img.size();
	img.zeros(64);
	auto scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::MidFileCorruption);
	EXPECT_EQ(scan.validEnd, breakOffset);
	EXPECT_EQ(scan.lastCompleteTransactionEnd, runEnd);
}

TEST(TransactionLogRecovery, UnflaggedRunAfterAMidFileBreakDoesNotAdvanceTheWatermark) {
	LogImage img;
	img.entry(10);
	uint32_t breakOffset = img.size();
	img.entryRaw(/*declaredLength=*/100000, /*actualDataLen=*/8);
	for (int i = 0; i < 12; ++i) {
		img.entry(16, /*flags=*/1);
	}
	uint32_t lastClosed = img.size();
	img.entry(16, /*flags=*/0).entry(16, /*flags=*/0);
	auto scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::MidFileCorruption);
	EXPECT_EQ(scan.validEnd, breakOffset);
	EXPECT_EQ(scan.lastCompleteTransactionEnd, lastClosed);
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

// lastCompleteTransactionEnd — the stricter bound the committed watermark uses.
// Only a batch's final entry carries TRANSACTION_LOG_ENTRY_LAST_FLAG, so a crash
// mid-batch leaves well-framed entries that are only a prefix of a transaction:
// structurally valid (validEnd accepts them) but not yet a closed transaction.

TEST(TransactionLogLastComplete, HeaderOnlyHasNoCompleteTransaction) {
	LogImage img;
	EXPECT_EQ(scanTransactionLogForRecovery(img.data(), img.size()).lastCompleteTransactionEnd, 0u);
}

TEST(TransactionLogLastComplete, SingleEntryTransactionEndsAtEof) {
	LogImage img;
	img.entry(10, /*flags=*/1);
	EXPECT_EQ(scanTransactionLogForRecovery(img.data(), img.size()).lastCompleteTransactionEnd, img.size());
}

TEST(TransactionLogLastComplete, StopsBeforeAnUnflaggedTail) {
	LogImage img;
	img.entry(10, /*flags=*/1);
	const uint32_t afterFirst = img.size();
	// a second transaction's entries, none of which closed it
	img.entry(20, /*flags=*/0).entry(30, /*flags=*/0);
	// the frames are all valid...
	EXPECT_EQ(scanTransactionLogForRecovery(img.data(), img.size()).validEnd, img.size());
	// ...but the watermark must stop at the last transaction that actually closed
	EXPECT_EQ(scanTransactionLogForRecovery(img.data(), img.size()).lastCompleteTransactionEnd, afterFirst);
}

TEST(TransactionLogLastComplete, MultiEntryTransactionEndsOnItsFlaggedEntry) {
	LogImage img;
	img.entry(10, /*flags=*/0).entry(20, /*flags=*/0).entry(30, /*flags=*/1);
	EXPECT_EQ(scanTransactionLogForRecovery(img.data(), img.size()).lastCompleteTransactionEnd, img.size());
}

TEST(TransactionLogLastComplete, NoneWhenNoTransactionEverClosed) {
	LogImage img;
	img.entry(10, /*flags=*/0).entry(20, /*flags=*/0);
	EXPECT_EQ(scanTransactionLogForRecovery(img.data(), img.size()).lastCompleteTransactionEnd, 0u);
}

TEST(TransactionLogLastComplete, IgnoresATornTailAfterAClosedTransaction) {
	LogImage img;
	img.entry(10, /*flags=*/1);
	const uint32_t afterFirst = img.size();
	img.entry(20, /*flags=*/0); // prefix of the next transaction
	img.entryRaw(/*declaredLength=*/5000, /*actualDataLen=*/12, /*flags=*/1); // torn
	EXPECT_EQ(scanTransactionLogForRecovery(img.data(), img.size()).lastCompleteTransactionEnd, afterFirst);
}

TEST(TransactionLogLastComplete, StopsAtZeroPaddedTail) {
	LogImage img;
	img.entry(10, /*flags=*/1);
	const uint32_t afterFirst = img.size();
	img.zeros(64);
	EXPECT_EQ(scanTransactionLogForRecovery(img.data(), img.size()).lastCompleteTransactionEnd, afterFirst);
}

// The unclosed-tail description — the evidence recoverTail() requires before it
// discards trailing entries. Only a run that is provably ONE interrupted batch of a
// flag-setting writer may be dropped; anything else is kept.

TEST(TransactionLogUnclosedTail, NoneWhenFileEndsOnATransactionBoundary) {
	LogImage img;
	img.entry(10, /*flags=*/1).entry(20, /*flags=*/1);
	RecoveryScan scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.unclosedTailEntries, 0u);
	EXPECT_FALSE(scan.unclosedTailIsOneTransaction);
}

TEST(TransactionLogUnclosedTail, OneTransactionWhenTheTailSharesATimestamp) {
	LogImage img;
	img.entry(10, /*flags=*/1, /*timestamp=*/2.0);
	const uint32_t afterFirst = img.size();
	// a three-entry batch whose closing entry never reached disk
	img.entry(20, /*flags=*/0, /*timestamp=*/9.0).entry(30, /*flags=*/0, /*timestamp=*/9.0);
	RecoveryScan scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.lastCompleteTransactionEnd, afterFirst);
	EXPECT_EQ(scan.unclosedTailEntries, 2u);
	EXPECT_TRUE(scan.unclosedTailIsOneTransaction);
}

TEST(TransactionLogUnclosedTail, NotOneTransactionWhenTheTailSpansTimestamps) {
	LogImage img;
	img.entry(10, /*flags=*/1, /*timestamp=*/2.0);
	// two unflagged entries from different batches: a flag-setting writer cannot
	// produce this, so the file is not ours to repair
	img.entry(20, /*flags=*/0, /*timestamp=*/9.0).entry(30, /*flags=*/0, /*timestamp=*/10.0);
	RecoveryScan scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.unclosedTailEntries, 2u);
	EXPECT_FALSE(scan.unclosedTailIsOneTransaction);
}

TEST(TransactionLogUnclosedTail, ResetsAtEveryClosedTransaction) {
	LogImage img;
	// distinct timestamps, but each batch closes — so there is no tail at all
	img.entry(10, /*flags=*/0, /*timestamp=*/2.0).entry(10, /*flags=*/1, /*timestamp=*/2.0);
	img.entry(10, /*flags=*/1, /*timestamp=*/3.0);
	const uint32_t afterSecond = img.size();
	img.entry(10, /*flags=*/0, /*timestamp=*/4.0);
	RecoveryScan scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.lastCompleteTransactionEnd, afterSecond);
	EXPECT_EQ(scan.unclosedTailEntries, 1u);
	EXPECT_TRUE(scan.unclosedTailIsOneTransaction);
}

TEST(TransactionLogUnclosedTail, CountsWholeEntriesBeforeATornFrame) {
	LogImage img;
	img.entry(10, /*flags=*/1, /*timestamp=*/2.0);
	const uint32_t afterFirst = img.size();
	img.entry(20, /*flags=*/0, /*timestamp=*/9.0); // whole entry of the interrupted batch
	img.entryRaw(/*declaredLength=*/5000, /*actualDataLen=*/12, /*flags=*/1, /*timestamp=*/9.0);
	RecoveryScan scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::TruncateTail);
	// the torn bytes go first, then the whole entry left over from the same batch
	EXPECT_EQ(scan.validEnd, afterFirst + 13u + 20u);
	EXPECT_EQ(scan.unclosedTailEntries, 1u);
	EXPECT_TRUE(scan.unclosedTailIsOneTransaction);
}

TEST(TransactionLogUnclosedTail, NoBoundaryWhenNothingEverClosed) {
	LogImage img;
	// the whole file is the continuation of a batch that began in an earlier file
	img.entry(10, /*flags=*/0, /*timestamp=*/9.0).entry(20, /*flags=*/0, /*timestamp=*/9.0);
	RecoveryScan scan = scanTransactionLogForRecovery(img.data(), img.size());
	EXPECT_EQ(scan.lastCompleteTransactionEnd, 0u);
	EXPECT_EQ(scan.unclosedTailEntries, 2u);
	// uniform, but with no boundary to fall back to there is nothing to truncate to
	EXPECT_TRUE(scan.unclosedTailIsOneTransaction);
}

namespace {

struct CountingRead {
	const char* data;
	uint32_t size;
	uint64_t bytes = 0;
	uint32_t maxN = 0;
	uint32_t reads = 0;
};

bool countingRead(void* context, uint32_t offset, void* dest, uint32_t n) {
	auto* counted = static_cast<CountingRead*>(context);
	counted->bytes += n;
	counted->reads += 1;
	if (n > counted->maxN) {
		counted->maxN = n;
	}
	if (static_cast<uint64_t>(offset) + n > counted->size) {
		return false;
	}
	std::memcpy(dest, counted->data + offset, n);
	return true;
}

struct FailingRead {
	const char* data;
	uint32_t size;
	uint32_t succeedCount;
	uint32_t reads = 0;
};

bool failingRead(void* context, uint32_t offset, void* dest, uint32_t n) {
	auto* failing = static_cast<FailingRead*>(context);
	if (failing->reads++ >= failing->succeedCount) {
		return false;
	}
	if (static_cast<uint64_t>(offset) + n > failing->size) {
		return false;
	}
	std::memcpy(dest, failing->data + offset, n);
	return true;
}

class OpenedLogFile {
public:
	explicit OpenedLogFile(const LogImage& img) {
		path = std::filesystem::temp_directory_path() /
			("rocksdb-js-recovery-scan-" + std::to_string(++sequence) + ".txnlog");
		{
			std::ofstream out(path, std::ios::binary);
			out.write(img.data(), static_cast<std::streamsize>(img.size()));
			EXPECT_TRUE(out);
		}
		file = std::make_unique<TransactionLogFile>(path, 1);
		file->open(2.0);
	}

	~OpenedLogFile() {
		file.reset();
		std::error_code error;
		std::filesystem::remove(path, error);
	}

	TransactionLogFile& get() { return *file; }

private:
	static uint32_t sequence;
	std::filesystem::path path;
	std::unique_ptr<TransactionLogFile> file;
};

uint32_t OpenedLogFile::sequence = 0;

} // namespace

TEST(TransactionLogRecoverySource, CleanWalkReadsOnlyHeaders) {
	LogImage img;
	img.entry(1024 * 1024).entry(1024 * 1024);
	CountingRead counted{ img.data(), img.size() };
	auto scan = scanTransactionLogForRecovery(img.size(), countingRead, &counted);
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::Clean);
	EXPECT_EQ(scan.validEnd, img.size());
	EXPECT_EQ(counted.maxN, 13u);
	EXPECT_EQ(counted.bytes, 26u);
	EXPECT_EQ(counted.reads, 2u);
}

TEST(TransactionLogRecoverySource, DenseTinyEntriesAmortizesReads) {
	LogImage img;
	for (int i = 0; i < 2000; ++i) {
		img.entry(1);
	}
	CountingRead counted{ img.data(), img.size() };
	auto scan = scanTransactionLogForRecovery(img.size(), countingRead, &counted);
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::Clean);
	EXPECT_EQ(scan.validEnd, img.size());
	EXPECT_LT(counted.reads, 10u);
	EXPECT_GT(counted.maxN, 13u);
}

TEST(TransactionLogRecoverySource, TypicalPayloadsAmortizesReads) {
	LogImage img;
	for (int i = 0; i < 500; ++i) {
		img.entry(200);
	}
	CountingRead counted{ img.data(), img.size() };
	auto scan = scanTransactionLogForRecovery(img.size(), countingRead, &counted);
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::Clean);
	EXPECT_EQ(scan.validEnd, img.size());
	EXPECT_LT(counted.reads, 10u);
	EXPECT_GT(counted.maxN, 13u);
}

TEST(TransactionLogRecoverySource, IoFailureThrowsRatherThanTruncate) {
	LogImage img;
	img.entry(10).entry(20);
	FailingRead failing{ img.data(), img.size(), /*succeedCount=*/0 };
	EXPECT_THROW(scanTransactionLogForRecovery(img.size(), failingRead, &failing), DBException);
}

TEST(TransactionLogRecoverySource, IoFailureDuringResyncThrowsRatherThanTruncate) {
	LogImage img;
	img.entry(10).entry(20);
	img.entryRaw(/*declaredLength=*/100000, /*actualDataLen=*/8);
	for (int i = 0; i < 12; ++i) {
		img.entry(16);
	}
	FailingRead failing{ img.data(), img.size(), /*succeedCount=*/2 };
	EXPECT_THROW(scanTransactionLogForRecovery(img.size(), failingRead, &failing), DBException);
}

TEST(TransactionLogRecoveryFile, MatchesBufferScanOnARealFile) {
	LogImage img;
	img.entry(10, /*flags=*/1).entry(20, /*flags=*/0).entry(30, /*flags=*/0);
	auto fromBuffer = scanTransactionLogForRecovery(img.data(), img.size());
	OpenedLogFile opened(img);
	auto fromFile = scanTransactionLogForRecovery(opened.get());
	EXPECT_EQ(fromFile.kind, fromBuffer.kind);
	EXPECT_EQ(fromFile.validEnd, fromBuffer.validEnd);
	EXPECT_EQ(fromFile.lastCompleteTransactionEnd, fromBuffer.lastCompleteTransactionEnd);
	EXPECT_EQ(fromFile.unclosedTailEntries, fromBuffer.unclosedTailEntries);
	EXPECT_EQ(fromFile.unclosedTailIsOneTransaction, fromBuffer.unclosedTailIsOneTransaction);
}

TEST(TransactionLogResumeOffset, FindsTheFirstFrameAfterABreak) {
	LogImage img;
	img.entry(10).entry(20);
	uint32_t breakOffset = img.size();
	img.entryRaw(/*declaredLength=*/100000, /*actualDataLen=*/8);
	uint32_t resumeOffset = img.size();
	for (int i = 0; i < 12; ++i) {
		img.entry(16);
	}
	EXPECT_EQ(findFramingResumeOffset(img.data(), img.size(), breakOffset + 1), resumeOffset);
	CountingRead counted{ img.data(), img.size() };
	EXPECT_EQ(findFramingResumeOffset(img.size(), countingRead, &counted, breakOffset + 1), resumeOffset);
}

TEST(TransactionLogResumeOffset, ZeroWhenNothingResumes) {
	LogImage img;
	img.entry(10).entry(20);
	uint32_t tornStart = img.size();
	img.entryRaw(/*declaredLength=*/5000, /*actualDataLen=*/12);
	EXPECT_EQ(findFramingResumeOffset(img.data(), img.size(), tornStart + 1), 0u);
	EXPECT_EQ(findFramingResumeOffset(img.data(), img.size(), img.size()), 0u);
	EXPECT_EQ(findFramingResumeOffset(img.data(), img.size(), 0), 0u);
}

TEST(TransactionLogResumeOffset, IoFailureThrows) {
	LogImage img;
	img.entry(10);
	uint32_t breakOffset = img.size();
	img.entryRaw(/*declaredLength=*/100000, /*actualDataLen=*/8);
	for (int i = 0; i < 12; ++i) {
		img.entry(16);
	}
	FailingRead failing{ img.data(), img.size(), /*succeedCount=*/0 };
	EXPECT_THROW(findFramingResumeOffset(img.size(), failingRead, &failing, breakOffset + 1), DBException);
}

// The timestamp index walks the same framing and must resume past a break too.

TEST(TransactionLogTimestampIndex, SeeksPastAMidFileBreak) {
	LogImage img;
	img.entry(16, /*flags=*/1, /*timestamp=*/10.0);
	uint32_t breakOffset = img.size();
	img.entryRaw(/*declaredLength=*/100000, /*actualDataLen=*/8, /*flags=*/1, /*timestamp=*/11.0);
	uint32_t resumeOffset = img.size();
	std::vector<uint32_t> offsets;
	for (int i = 0; i < 12; ++i) {
		offsets.push_back(img.size());
		img.entry(16, /*flags=*/1, /*timestamp=*/20.0 + i);
	}
	OpenedLogFile opened(img);
	TransactionLogFile& file = opened.get();
	EXPECT_EQ(file.findPositionByTimestamp(10.0, img.size(), /*isCurrent=*/true), TRANSACTION_LOG_FILE_HEADER_SIZE);
	EXPECT_EQ(file.findPositionByTimestamp(11.0, img.size(), /*isCurrent=*/true), resumeOffset);
	EXPECT_EQ(file.findPositionByTimestamp(20.0, img.size(), /*isCurrent=*/true), offsets[0]);
	EXPECT_EQ(file.findPositionByTimestamp(25.5, img.size(), /*isCurrent=*/true), offsets[6]);
	EXPECT_EQ(file.findPositionByTimestamp(31.0, img.size(), /*isCurrent=*/true), offsets[11]);
	EXPECT_EQ(file.findPositionByTimestamp(32.0, img.size(), /*isCurrent=*/true), 0xFFFFFFFFu);
	EXPECT_EQ(breakOffset + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 8u, resumeOffset);
}

TEST(TransactionLogTimestampIndex, ShortRunBeforeThePaddingIsIndexedAndEndsTheFile) {
	LogImage img;
	img.entry(16, /*flags=*/1, /*timestamp=*/10.0);
	img.entryRaw(/*declaredLength=*/100000, /*actualDataLen=*/8, /*flags=*/1, /*timestamp=*/11.0);
	uint32_t resumeOffset = img.size();
	img.entry(16, /*flags=*/1, /*timestamp=*/20.0).entry(16, /*flags=*/1, /*timestamp=*/21.0);
	uint32_t runEnd = img.size();
	img.zeros(4096);
	OpenedLogFile opened(img);
	TransactionLogFile& file = opened.get();
	EXPECT_EQ(file.findPositionByTimestamp(11.0, img.size(), /*isCurrent=*/true), resumeOffset);
	EXPECT_EQ(file.findPositionByTimestamp(21.0, img.size(), /*isCurrent=*/true), resumeOffset + TRANSACTION_LOG_ENTRY_HEADER_SIZE + 16u);
	EXPECT_EQ(file.findPositionByTimestamp(22.0, img.size(), /*isCurrent=*/true), 0xFFFFFFFFu);
	EXPECT_EQ(file.size.load(), runEnd);
}

TEST(TransactionLogTimestampIndex, SeeksPastMultipleBreaks) {
	LogImage img;
	img.entry(16, /*flags=*/1, /*timestamp=*/10.0);
	img.entryRaw(/*declaredLength=*/0, /*actualDataLen=*/0, /*flags=*/1, /*timestamp=*/11.0);
	for (int i = 0; i < 12; ++i) {
		img.entry(16, /*flags=*/1, /*timestamp=*/20.0 + i);
	}
	img.entryRaw(/*declaredLength=*/100000, /*actualDataLen=*/8, /*flags=*/1, /*timestamp=*/40.0);
	uint32_t lastRun = img.size();
	img.entry(16, /*flags=*/1, /*timestamp=*/50.0).entry(16, /*flags=*/1, /*timestamp=*/51.0);
	OpenedLogFile opened(img);
	TransactionLogFile& file = opened.get();
	EXPECT_EQ(file.findPositionByTimestamp(50.0, img.size(), /*isCurrent=*/true), lastRun);
	EXPECT_EQ(file.findPositionByTimestamp(51.0, img.size(), /*isCurrent=*/true), lastRun + 13u + 16u);
	EXPECT_EQ(file.findPositionByTimestamp(52.0, img.size(), /*isCurrent=*/true), 0xFFFFFFFFu);
}

TEST(TransactionLogTimestampIndex, TornTailLeavesEarlierEntriesSeekable) {
	LogImage img;
	img.entry(16, /*flags=*/1, /*timestamp=*/10.0);
	uint32_t second = img.size();
	img.entry(16, /*flags=*/1, /*timestamp=*/11.0);
	img.entryRaw(/*declaredLength=*/5000, /*actualDataLen=*/12, /*flags=*/1, /*timestamp=*/12.0);
	OpenedLogFile opened(img);
	TransactionLogFile& file = opened.get();
	EXPECT_EQ(file.findPositionByTimestamp(11.0, img.size(), /*isCurrent=*/true), second);
	EXPECT_EQ(file.findPositionByTimestamp(12.0, img.size(), /*isCurrent=*/true), 0xFFFFFFFFu);
}

// A header-only file has no entry to bound-check against the map; its own
// timestamp slot must still be indexed (an unindexed tail would read as offset 5).
TEST(TransactionLogTimestampIndex, HeaderOnlyFileIsNotAnUnindexedTail) {
	LogImage img;
	OpenedLogFile opened(img);
	TransactionLogFile& file = opened.get();
	EXPECT_EQ(file.findPositionByTimestamp(0.5, img.size(), /*isCurrent=*/true), 0u);
	EXPECT_EQ(file.findPositionByTimestamp(1.0, img.size(), /*isCurrent=*/true), 0u);
	EXPECT_EQ(file.findPositionByTimestamp(1.5, img.size(), /*isCurrent=*/true), 0xFFFFFFFFu);
}

// A partial header below the written extent (fewer than 13 bytes) is a torn
// tail the map may not fully cover; the walk must stop short of it instead of
// reading past the mapping.
TEST(TransactionLogTimestampIndex, PartialTrailingHeaderStopsTheWalk) {
	LogImage img;
	img.entry(16, /*flags=*/1, /*timestamp=*/10.0);
	uint32_t tail = img.size();
	img.raw({ '\x40', '\x24', '\x00', '\x00', '\x00' });
	OpenedLogFile opened(img);
	TransactionLogFile& file = opened.get();
	EXPECT_EQ(file.findPositionByTimestamp(10.0, img.size(), /*isCurrent=*/true), TRANSACTION_LOG_FILE_HEADER_SIZE);
	EXPECT_EQ(file.findPositionByTimestamp(11.0, img.size(), /*isCurrent=*/true), tail);
}

TEST(TransactionLogRecoveryFile, MatchesBufferScanOnMidFileCorruption) {
	LogImage img;
	img.entry(10).entry(20);
	img.entryRaw(/*declaredLength=*/100000, /*actualDataLen=*/8);
	for (int i = 0; i < 12; ++i) {
		img.entry(16);
	}
	auto fromBuffer = scanTransactionLogForRecovery(img.data(), img.size());
	OpenedLogFile opened(img);
	auto fromFile = scanTransactionLogForRecovery(opened.get());
	EXPECT_EQ(fromFile.kind, RecoveryScan::Kind::MidFileCorruption);
	EXPECT_EQ(fromFile.kind, fromBuffer.kind);
	EXPECT_EQ(fromFile.validEnd, fromBuffer.validEnd);
}

#ifndef _WIN32
TEST(TransactionLogRecoveryFile, ReadsAHeaderPastTwoGiBOnASparseFile) {
	namespace fs = std::filesystem;
	static uint32_t sparseSeq = 0;
	auto path = fs::temp_directory_path() /
		("rocksdb-js-recovery-sparse-2g-" + std::to_string(::getpid()) + "-" +
			std::to_string(++sparseSeq) + ".txnlog");
	std::error_code error;
	fs::remove(path, error);

	constexpr uint32_t secondHeader = 0x80000000u;
	constexpr uint32_t payload = 10;
	const uint32_t fileSize = secondHeader + TRANSACTION_LOG_ENTRY_HEADER_SIZE + payload;
	const uint32_t firstLength = secondHeader - TRANSACTION_LOG_FILE_HEADER_SIZE -
		TRANSACTION_LOG_ENTRY_HEADER_SIZE;

	struct Fd {
		int fd = -1;
		~Fd() {
			if (fd >= 0) {
				::close(fd);
			}
		}
	} owned;
	owned.fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0640);
	ASSERT_GE(owned.fd, 0);
	ASSERT_EQ(::ftruncate(owned.fd, static_cast<off_t>(fileSize)), 0);

	std::vector<char> fileHeader(TRANSACTION_LOG_FILE_HEADER_SIZE);
	rocksdb_js::writeUint32BE(fileHeader.data(), 0x574f4f46);
	rocksdb_js::writeUint8(fileHeader.data() + 4, 1);
	rocksdb_js::writeDoubleBE(fileHeader.data() + 5, 2.0);
	ASSERT_EQ(::pwrite(owned.fd, fileHeader.data(), fileHeader.size(), 0),
		static_cast<ssize_t>(fileHeader.size()));

	std::vector<char> firstHeader(TRANSACTION_LOG_ENTRY_HEADER_SIZE);
	rocksdb_js::writeDoubleBE(firstHeader.data(), 3.0);
	rocksdb_js::writeUint32BE(firstHeader.data() + 8, firstLength);
	rocksdb_js::writeUint8(firstHeader.data() + 12, 0);
	ASSERT_EQ(::pwrite(owned.fd, firstHeader.data(), firstHeader.size(), TRANSACTION_LOG_FILE_HEADER_SIZE),
		static_cast<ssize_t>(firstHeader.size()));

	std::vector<char> second(TRANSACTION_LOG_ENTRY_HEADER_SIZE + payload, static_cast<char>(0xAB));
	rocksdb_js::writeDoubleBE(second.data(), 4.0);
	rocksdb_js::writeUint32BE(second.data() + 8, payload);
	rocksdb_js::writeUint8(second.data() + 12, 1);
	ASSERT_EQ(::pwrite(owned.fd, second.data(), second.size(), secondHeader),
		static_cast<ssize_t>(second.size()));
	::close(owned.fd);
	owned.fd = -1;

	TransactionLogFile file(path, 1);
	file.open(2.0);
	auto scan = scanTransactionLogForRecovery(file);
	EXPECT_EQ(scan.kind, RecoveryScan::Kind::Clean);
	EXPECT_EQ(scan.validEnd, fileSize);
	EXPECT_EQ(scan.lastCompleteTransactionEnd, fileSize);

	file.close();
	fs::remove(path, error);
}
#endif

