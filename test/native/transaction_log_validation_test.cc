// Unit tests for validateTransactionLogImage / validateTransactionLogStore —
// the pure validation core behind the `validateTransactionLogStore` JS API and
// the transaction-log leg of `backups.verify()`.

#include <gtest/gtest.h>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>
#include "core/encoding.h"
#include "core/exception.h"
#include "transaction_log/transaction_log_file.h"
#include "transaction_log/transaction_log_validation.h"

using rocksdb_js::DBException;
using rocksdb_js::TransactionLogFileValidation;
using rocksdb_js::TransactionLogStoreValidation;
using rocksdb_js::validateTransactionLogImage;
using rocksdb_js::validateTransactionLogStore;

namespace {

// Any timestamp at or after 2026-01-27T00:00:00Z is considered sane.
constexpr double VALID_TS = 1769472000000.0;

// Builds transaction-log file images for validation to classify.
class LogImage {
public:
	explicit LogImage(uint32_t token = 0x574f4f46, uint8_t version = 1, double timestamp = VALID_TS) {
		appendU32(token);
		appendU8(version);
		appendF64(timestamp);
	}

	// Append a well-formed entry whose declared length matches its data.
	LogImage& entry(uint32_t dataLen, uint8_t flags = 1, double timestamp = VALID_TS) {
		return entryRaw(dataLen, dataLen, flags, timestamp);
	}

	// Append an entry header that declares `declaredLength` but only writes
	// `actualDataLen` data bytes (used to simulate a torn/partial entry).
	LogImage& entryRaw(
		uint32_t declaredLength,
		uint32_t actualDataLen,
		uint8_t flags = 1,
		double timestamp = VALID_TS
	) {
		appendF64(timestamp);
		appendU32(declaredLength);
		appendU8(flags);
		for (uint32_t i = 0; i < actualDataLen; ++i) {
			bytes.push_back(static_cast<char>(0xAB));
		}
		return *this;
	}

	LogImage& zeros(uint32_t count) {
		bytes.insert(bytes.end(), count, '\0');
		return *this;
	}

	LogImage& raw(const std::vector<char>& extra) {
		bytes.insert(bytes.end(), extra.begin(), extra.end());
		return *this;
	}

	const char* data() const { return bytes.data(); }
	uint32_t size() const { return static_cast<uint32_t>(bytes.size()); }

	void writeTo(const std::filesystem::path& path) const {
		std::ofstream file(path, std::ios::binary | std::ios::trunc);
		file.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
	}

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

std::filesystem::path makeTempStoreDir(const char* name) {
	auto dir = std::filesystem::temp_directory_path() / name;
	std::filesystem::remove_all(dir);
	std::filesystem::create_directories(dir);
	return dir;
}

void writeTxnState(const std::filesystem::path& dir, uint32_t offset, uint32_t sequence) {
	char bytes[8];
	std::memcpy(bytes, &offset, 4);
	std::memcpy(bytes + 4, &sequence, 4);
	std::ofstream file(dir / "txn.state", std::ios::binary | std::ios::trunc);
	file.write(bytes, sizeof(bytes));
}

} // namespace

// ---------------------------------------------------------------------------
// validateTransactionLogImage
// ---------------------------------------------------------------------------

TEST(TransactionLogValidation, CleanImage) {
	LogImage img;
	img.entry(10).entry(20).entry(30);
	auto result = validateTransactionLogImage(img.data(), img.size(), false);
	EXPECT_TRUE(result.valid);
	EXPECT_EQ(result.entryCount, 3u);
	EXPECT_EQ(result.validBytes, img.size());
	EXPECT_TRUE(result.errors.empty());
	EXPECT_TRUE(result.warnings.empty());
}

TEST(TransactionLogValidation, HeaderOnlyImageIsValidWithNoEntries) {
	LogImage img;
	auto result = validateTransactionLogImage(img.data(), img.size(), false);
	EXPECT_TRUE(result.valid);
	EXPECT_EQ(result.entryCount, 0u);
	EXPECT_EQ(result.validBytes, img.size());
}

TEST(TransactionLogValidation, ZeroPaddedTailIsClean) {
	// Windows pre-extends and zero-pads log files; the zero timestamp marks the
	// end of entries and everything after it is ignored.
	LogImage img;
	img.entry(10).entry(20);
	uint32_t entriesEnd = img.size();
	img.zeros(128);
	auto result = validateTransactionLogImage(img.data(), img.size(), false);
	EXPECT_TRUE(result.valid);
	EXPECT_EQ(result.entryCount, 2u);
	EXPECT_EQ(result.validBytes, entriesEnd);
}

TEST(TransactionLogValidation, TooSmallImage) {
	LogImage img;
	auto result = validateTransactionLogImage(img.data(), 5, false);
	EXPECT_FALSE(result.valid);
	ASSERT_EQ(result.errors.size(), 1u);
	EXPECT_NE(result.errors[0].find("too small"), std::string::npos);
}

TEST(TransactionLogValidation, BadToken) {
	LogImage img(/*token=*/0xdeadbeef);
	img.entry(10);
	auto result = validateTransactionLogImage(img.data(), img.size(), false);
	EXPECT_FALSE(result.valid);
	ASSERT_EQ(result.errors.size(), 1u);
	EXPECT_NE(result.errors[0].find("token"), std::string::npos);
	// the framing walk is skipped when the header is not ours
	EXPECT_EQ(result.entryCount, 0u);
}

TEST(TransactionLogValidation, UnsupportedVersion) {
	LogImage img(0x574f4f46, /*version=*/9);
	img.entry(10);
	auto result = validateTransactionLogImage(img.data(), img.size(), false);
	EXPECT_FALSE(result.valid);
	ASSERT_EQ(result.errors.size(), 1u);
	EXPECT_NE(result.errors[0].find("version"), std::string::npos);
}

TEST(TransactionLogValidation, HeaderTimestampAnomalyIsWarning) {
	LogImage img(0x574f4f46, 1, /*timestamp=*/1.0);
	img.entry(10);
	auto result = validateTransactionLogImage(img.data(), img.size(), false);
	EXPECT_TRUE(result.valid);
	ASSERT_EQ(result.warnings.size(), 1u);
	EXPECT_NE(result.warnings[0].find("Header timestamp"), std::string::npos);
}

TEST(TransactionLogValidation, SubHeaderZeroPaddingIsClean) {
	// Windows pre-extends and zero-pads log files, and rotation can leave fewer
	// than an entry header's worth (13 bytes) of padding at the end — too few for
	// the zero-timestamp end marker to be visible. That is padding, not a torn
	// entry, even in strict mode.
	LogImage img;
	img.entry(10).entry(20);
	uint32_t entriesEnd = img.size();
	img.zeros(5);
	for (bool strict : { false, true }) {
		auto result = validateTransactionLogImage(img.data(), img.size(), strict);
		EXPECT_TRUE(result.valid);
		EXPECT_TRUE(result.errors.empty());
		EXPECT_TRUE(result.warnings.empty());
		EXPECT_EQ(result.entryCount, 2u);
		EXPECT_EQ(result.validBytes, entriesEnd);
	}
}

TEST(TransactionLogValidation, NonZeroPartialHeaderTailIsTorn) {
	LogImage img;
	img.entry(10);
	img.raw({ 0x42, 0x79, 0x05 }); // nonzero partial header: a real torn write
	auto result = validateTransactionLogImage(img.data(), img.size(), false);
	EXPECT_TRUE(result.valid);
	ASSERT_EQ(result.warnings.size(), 1u);
	EXPECT_NE(result.warnings[0].find("Torn/partial entry"), std::string::npos);
}

TEST(TransactionLogValidation, TornTailIsWarningByDefault) {
	LogImage img;
	img.entry(10).entry(20);
	uint32_t tornStart = img.size();
	img.entryRaw(/*declaredLength=*/5000, /*actualDataLen=*/12);
	auto result = validateTransactionLogImage(img.data(), img.size(), false);
	EXPECT_TRUE(result.valid);
	EXPECT_EQ(result.entryCount, 2u);
	EXPECT_EQ(result.validBytes, tornStart);
	ASSERT_EQ(result.warnings.size(), 1u);
	EXPECT_NE(result.warnings[0].find("Torn/partial entry"), std::string::npos);
}

TEST(TransactionLogValidation, TornTailIsErrorInStrictMode) {
	LogImage img;
	img.entry(10).entry(20);
	img.entryRaw(/*declaredLength=*/5000, /*actualDataLen=*/12);
	auto result = validateTransactionLogImage(img.data(), img.size(), true);
	EXPECT_FALSE(result.valid);
	ASSERT_EQ(result.errors.size(), 1u);
	EXPECT_NE(result.errors[0].find("Torn/partial entry"), std::string::npos);
	EXPECT_EQ(result.entryCount, 2u);
}

TEST(TransactionLogValidation, MidFileCorruptionIsError) {
	// A broken frame with >= 8 valid frames after it classifies as mid-file
	// corruption (committed entries follow the break), which is always an error.
	LogImage img;
	img.entry(10);
	uint32_t breakAt = img.size();
	img.entryRaw(/*declaredLength=*/0, /*actualDataLen=*/0); // zero length = broken frame
	for (int i = 0; i < 8; i++) {
		img.entry(4);
	}
	auto result = validateTransactionLogImage(img.data(), img.size(), false);
	EXPECT_FALSE(result.valid);
	ASSERT_EQ(result.errors.size(), 1u);
	EXPECT_NE(result.errors[0].find("mid-file corruption"), std::string::npos);
	EXPECT_EQ(result.validBytes, breakAt);
	EXPECT_EQ(result.entryCount, 1u);
}

TEST(TransactionLogValidation, EntryAnomaliesAreWarnings) {
	LogImage img;
	img.entry(10, /*flags=*/0x82); // undefined flag bits
	img.entry(10, /*flags=*/1, /*timestamp=*/2.0); // implausible timestamp
	img.entry(10);
	auto result = validateTransactionLogImage(img.data(), img.size(), false);
	EXPECT_TRUE(result.valid);
	EXPECT_EQ(result.entryCount, 3u);
	ASSERT_EQ(result.warnings.size(), 2u);
	EXPECT_NE(result.warnings[0].find("flags"), std::string::npos);
	EXPECT_NE(result.warnings[1].find("timestamp"), std::string::npos);
}

TEST(TransactionLogValidation, EntryAnomaliesAreCapped) {
	LogImage img;
	for (int i = 0; i < 15; i++) {
		img.entry(4, /*flags=*/0x80);
	}
	auto result = validateTransactionLogImage(img.data(), img.size(), false);
	EXPECT_TRUE(result.valid);
	EXPECT_EQ(result.entryCount, 15u);
	// 10 reported individually + 1 summary line
	ASSERT_EQ(result.warnings.size(), 11u);
	EXPECT_NE(result.warnings.back().find("+5 more"), std::string::npos);
}

// ---------------------------------------------------------------------------
// validateTransactionLogStore
// ---------------------------------------------------------------------------

TEST(TransactionLogValidation, StoreMissingDirectoryThrows) {
	auto dir = std::filesystem::temp_directory_path() / "rocksdb-js-validation-does-not-exist";
	std::filesystem::remove_all(dir);
	EXPECT_THROW(validateTransactionLogStore(dir, false), DBException);
}

TEST(TransactionLogValidation, ValidStore) {
	auto dir = makeTempStoreDir("rocksdb-js-validation-valid-store");
	LogImage first;
	first.entry(10).entry(20);
	first.writeTo(dir / "1.txnlog");
	LogImage second;
	second.entry(30);
	second.writeTo(dir / "2.txnlog");
	writeTxnState(dir, /*offset=*/13, /*sequence=*/2);

	auto result = validateTransactionLogStore(dir, false);
	EXPECT_TRUE(result.valid);
	EXPECT_TRUE(result.errors.empty());
	EXPECT_TRUE(result.warnings.empty());
	ASSERT_EQ(result.files.size(), 2u);
	EXPECT_EQ(result.files[0].file, "1.txnlog");
	EXPECT_EQ(result.files[0].sequenceNumber, 1u);
	EXPECT_EQ(result.files[0].result.entryCount, 2u);
	EXPECT_EQ(result.files[1].sequenceNumber, 2u);

	std::filesystem::remove_all(dir);
}

TEST(TransactionLogValidation, EmptyStoreIsValidWithWarning) {
	auto dir = makeTempStoreDir("rocksdb-js-validation-empty-store");
	auto result = validateTransactionLogStore(dir, false);
	EXPECT_TRUE(result.valid);
	ASSERT_EQ(result.warnings.size(), 1u);
	EXPECT_NE(result.warnings[0].find("No transaction log files"), std::string::npos);
	std::filesystem::remove_all(dir);
}

TEST(TransactionLogValidation, StoreWithCorruptFileIsInvalid) {
	auto dir = makeTempStoreDir("rocksdb-js-validation-corrupt-store");
	LogImage good;
	good.entry(10);
	good.writeTo(dir / "1.txnlog");
	LogImage bad(/*token=*/0x00c0ffee);
	bad.entry(10);
	bad.writeTo(dir / "2.txnlog");

	auto result = validateTransactionLogStore(dir, false);
	EXPECT_FALSE(result.valid);
	ASSERT_EQ(result.files.size(), 2u);
	EXPECT_TRUE(result.files[0].result.valid);
	EXPECT_FALSE(result.files[1].result.valid);

	std::filesystem::remove_all(dir);
}

TEST(TransactionLogValidation, SequenceGapIsWarning) {
	auto dir = makeTempStoreDir("rocksdb-js-validation-gap-store");
	LogImage img;
	img.entry(10);
	img.writeTo(dir / "1.txnlog");
	img.writeTo(dir / "4.txnlog");

	auto result = validateTransactionLogStore(dir, false);
	EXPECT_TRUE(result.valid);
	ASSERT_EQ(result.warnings.size(), 1u);
	EXPECT_NE(result.warnings[0].find("Gap in log file sequence: 2..3 missing"), std::string::npos);

	std::filesystem::remove_all(dir);
}

TEST(TransactionLogValidation, MalformedFileNameIsError) {
	auto dir = makeTempStoreDir("rocksdb-js-validation-name-store");
	LogImage img;
	img.entry(10);
	img.writeTo(dir / "1.txnlog");
	img.writeTo(dir / "12abc.txnlog");
	img.writeTo(dir / "0.txnlog");
	// a leading zero would alias another file's sequence ("01" == "1")
	img.writeTo(dir / "01.txnlog");

	auto result = validateTransactionLogStore(dir, false);
	EXPECT_FALSE(result.valid);
	EXPECT_EQ(result.errors.size(), 3u);
	// malformed names are not validated as log files
	ASSERT_EQ(result.files.size(), 1u);

	std::filesystem::remove_all(dir);
}

TEST(TransactionLogValidation, StrictSequenceGapIsError) {
	auto dir = makeTempStoreDir("rocksdb-js-validation-strict-gap-store");
	LogImage img;
	img.entry(10);
	img.writeTo(dir / "1.txnlog");
	img.writeTo(dir / "3.txnlog");

	EXPECT_TRUE(validateTransactionLogStore(dir, false).valid);
	auto result = validateTransactionLogStore(dir, true);
	EXPECT_FALSE(result.valid);
	ASSERT_EQ(result.errors.size(), 1u);
	EXPECT_NE(result.errors[0].find("Gap in log file sequence"), std::string::npos);

	std::filesystem::remove_all(dir);
}

TEST(TransactionLogValidation, StrictTxnStateBeyondNewestIsError) {
	auto dir = makeTempStoreDir("rocksdb-js-validation-strict-state-store");
	LogImage img;
	img.entry(10);
	img.writeTo(dir / "1.txnlog");
	writeTxnState(dir, /*offset=*/13, /*sequence=*/7);

	EXPECT_TRUE(validateTransactionLogStore(dir, false).valid);
	auto result = validateTransactionLogStore(dir, true);
	EXPECT_FALSE(result.valid);
	ASSERT_EQ(result.errors.size(), 1u);
	EXPECT_NE(result.errors[0].find("newer than the newest log file"), std::string::npos);

	std::filesystem::remove_all(dir);
}

TEST(TransactionLogValidation, UnexpectedFileIsWarning) {
	auto dir = makeTempStoreDir("rocksdb-js-validation-unexpected-store");
	LogImage img;
	img.entry(10);
	img.writeTo(dir / "1.txnlog");
	std::ofstream(dir / "notes.txt") << "hello";

	auto result = validateTransactionLogStore(dir, false);
	EXPECT_TRUE(result.valid);
	ASSERT_EQ(result.warnings.size(), 1u);
	EXPECT_NE(result.warnings[0].find("Unexpected file: notes.txt"), std::string::npos);

	std::filesystem::remove_all(dir);
}

TEST(TransactionLogValidation, TxnStateWrongSizeIsError) {
	auto dir = makeTempStoreDir("rocksdb-js-validation-state-size-store");
	LogImage img;
	img.entry(10);
	img.writeTo(dir / "1.txnlog");
	std::ofstream(dir / "txn.state", std::ios::binary) << "abc";

	auto result = validateTransactionLogStore(dir, false);
	EXPECT_FALSE(result.valid);
	ASSERT_EQ(result.errors.size(), 1u);
	EXPECT_NE(result.errors[0].find("txn.state is 3 bytes"), std::string::npos);

	std::filesystem::remove_all(dir);
}

TEST(TransactionLogValidation, TxnStateBeyondNewestSequenceIsWarning) {
	auto dir = makeTempStoreDir("rocksdb-js-validation-state-seq-store");
	LogImage img;
	img.entry(10);
	img.writeTo(dir / "1.txnlog");
	writeTxnState(dir, /*offset=*/13, /*sequence=*/7);

	auto result = validateTransactionLogStore(dir, false);
	EXPECT_TRUE(result.valid);
	ASSERT_EQ(result.warnings.size(), 1u);
	EXPECT_NE(result.warnings[0].find("newer than the newest log file"), std::string::npos);

	std::filesystem::remove_all(dir);
}

TEST(TransactionLogValidation, TxnStateOffsetBeyondFileSizeIsWarning) {
	auto dir = makeTempStoreDir("rocksdb-js-validation-state-offset-store");
	LogImage img;
	img.entry(10);
	img.writeTo(dir / "1.txnlog");
	writeTxnState(dir, /*offset=*/img.size() + 1000, /*sequence=*/1);

	auto result = validateTransactionLogStore(dir, false);
	EXPECT_TRUE(result.valid);
	ASSERT_EQ(result.warnings.size(), 1u);
	EXPECT_NE(result.warnings[0].find("exceeds the size of 1.txnlog"), std::string::npos);

	std::filesystem::remove_all(dir);
}

TEST(TransactionLogValidation, StrictStoreTornTailIsInvalid) {
	auto dir = makeTempStoreDir("rocksdb-js-validation-strict-store");
	LogImage img;
	img.entry(10);
	img.entryRaw(/*declaredLength=*/5000, /*actualDataLen=*/12);
	img.writeTo(dir / "1.txnlog");

	EXPECT_TRUE(validateTransactionLogStore(dir, false).valid);
	EXPECT_FALSE(validateTransactionLogStore(dir, true).valid);

	std::filesystem::remove_all(dir);
}
