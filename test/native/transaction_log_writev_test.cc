// Unit tests for the partial-writev retry loop in
// TransactionLogFile::writeBatchToFile (POSIX), and for what an append that
// fails part-way through leaves behind on disk.
//
// The production code calls ROCKSDB_JS_WRITEV() instead of ::writev() directly.
// This translation unit provides rocksdb_js_mock_writev(), which the test
// binary is compiled with via -DROCKSDB_JS_WRITEV=rocksdb_js_mock_writev.
// Per-test globals control whether the mock simulates partial writes and
// whether it eventually fails with ENOSPC.

#ifndef _WIN32

#include <gtest/gtest.h>
#include <algorithm>
#include <fcntl.h>
#include <sys/uio.h>
#include <unistd.h>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <memory>
#include <string>
#include <vector>
#include "transaction_log/transaction_log_entry.h"
#include "transaction_log/transaction_log_file.h"
#include "transaction_log/transaction_log_recovery.h"
#include "transaction_log/transaction_log_store.h"
#include "transaction_log/transaction_log_validation.h"

// ---------------------------------------------------------------------------
// writev mock
// ---------------------------------------------------------------------------

// When non-zero, the mock writes at most this many bytes per call (simulating
// a short return). Zero means pass through to the real ::writev.
static size_t g_writev_max_bytes_per_call = 0;

// Total bytes the mock will still let through before failing with ENOSPC —
// a full disk hit part-way through an append. SIZE_MAX disables the budget.
static constexpr size_t kUnlimitedWritevBudget = SIZE_MAX;
static size_t g_writev_budget_bytes = kUnlimitedWritevBudget;

// Caps the next ::write() (the appending writeToFile path, i.e. the file
// header) at this many bytes. SIZE_MAX passes through.
static size_t g_write_cap_bytes = SIZE_MAX;

static size_t g_ftruncate_calls = 0;

extern "C" int rocksdb_js_mock_ftruncate(int fd, off_t length) {
	++g_ftruncate_calls;
	return ::ftruncate(fd, length);
}

extern "C" ssize_t rocksdb_js_mock_write(int fd, const void* buffer, size_t count) {
	if (g_write_cap_bytes != SIZE_MAX && count > g_write_cap_bytes) {
		count = g_write_cap_bytes;
	}
	return ::write(fd, buffer, count);
}

extern "C" ssize_t rocksdb_js_mock_writev(int fd, const struct iovec* iov, int iovcnt) {
	if (g_writev_budget_bytes != kUnlimitedWritevBudget) {
		if (g_writev_budget_bytes == 0) {
			errno = ENOSPC;
			return -1;
		}
		size_t toWrite = std::min(iov[0].iov_len, g_writev_budget_bytes);
		ssize_t written = ::write(fd, iov[0].iov_base, toWrite);
		if (written > 0) {
			g_writev_budget_bytes -= static_cast<size_t>(written);
		}
		return written;
	}

	if (g_writev_max_bytes_per_call == 0) {
		return ::writev(fd, iov, iovcnt);
	}

	// Simulate a short write: write only from the first iovec, capped at the
	// configured limit.
	size_t toWrite = std::min(iov[0].iov_len, g_writev_max_bytes_per_call);
	return ::write(fd, iov[0].iov_base, toWrite);
}

// ---------------------------------------------------------------------------
// Test accessor — friend of TransactionLogFile (via ROCKSDB_JS_NATIVE_TESTS)
// ---------------------------------------------------------------------------

struct WriteBatchToFileTestAccessor {
	static int64_t call(rocksdb_js::TransactionLogFile& f, iovec* iovecs, int iovcnt,
		int64_t* bytesLanded = nullptr)
	{
		int64_t sink = 0;
		return f.writeBatchToFile(iovecs, iovcnt, bytesLanded ? *bytesLanded : sink);
	}
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Creates a temporary file, returns its fd. Caller owns the fd.
static int makeTempFile() {
	char tmpl[] = "/tmp/rocksdb-js-writev-test-XXXXXX";
	int fd = ::mkstemp(tmpl);
	EXPECT_GE(fd, 0);
	::unlink(tmpl);
	return fd;
}

// Reads the full contents of fd (from current position 0) into a vector.
static std::vector<char> readAll(int fd) {
	EXPECT_EQ(::lseek(fd, 0, SEEK_SET), 0);
	std::vector<char> out;
	char buf[4096];
	ssize_t n;
	while ((n = ::read(fd, buf, sizeof(buf))) > 0) {
		out.insert(out.end(), buf, buf + n);
	}
	return out;
}

// Builds a vector of iovecs pointing at contiguous memory regions.
// Returns both the iovecs and the backing buffers (kept alive by the caller).
static void makeIovecs(
	int count, size_t chunkSize,
	std::vector<std::vector<char>>& buffers,
	std::vector<iovec>& iovecs)
{
	buffers.reserve(count);
	iovecs.reserve(count);
	for (int i = 0; i < count; ++i) {
		std::vector<char> chunk(chunkSize);
		for (size_t j = 0; j < chunkSize; ++j) {
			chunk[j] = static_cast<char>((i * 31 + j) & 0xff);
		}
		buffers.push_back(std::move(chunk));
		iovec iv = { buffers.back().data(), chunkSize };
		iovecs.push_back(iv);
	}
}

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------

class WriteBatchToFile : public ::testing::Test {
protected:
	void SetUp() override {
		g_writev_max_bytes_per_call = 0; // real writev by default
		g_writev_budget_bytes = kUnlimitedWritevBudget;
		fd_ = -1;
		file_.reset();
	}
	void TearDown() override {
		g_writev_max_bytes_per_call = 0;
		g_writev_budget_bytes = kUnlimitedWritevBudget;
		file_.reset();
		if (fd_ >= 0) {
			::close(fd_);
			fd_ = -1;
		}
	}

	rocksdb_js::TransactionLogFile& makeFile() {
		fd_ = makeTempFile();
		file_ = std::make_unique<rocksdb_js::TransactionLogFile>("", 0);
		file_->fd = fd_;
		return *file_;
	}

	int fd_ = -1;
	std::unique_ptr<rocksdb_js::TransactionLogFile> file_;
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

TEST_F(WriteBatchToFile, ZeroIovcntReturnsZero) {
	auto& f = makeFile();
	int64_t result = WriteBatchToFileTestAccessor::call(f, nullptr, 0);
	EXPECT_EQ(result, 0);
}

TEST_F(WriteBatchToFile, BadFdReturnsMinusOne) {
	rocksdb_js::TransactionLogFile f("", 0);
	f.fd = -1;
	char data[] = "hello";
	iovec iv = { data, sizeof(data) };
	int64_t result = WriteBatchToFileTestAccessor::call(f, &iv, 1);
	EXPECT_EQ(result, -1);
}

TEST_F(WriteBatchToFile, SingleIovecWritesAllBytes) {
	auto& f = makeFile();
	const char payload[] = "rocksdb-js";
	iovec iv = { const_cast<char*>(payload), sizeof(payload) - 1 };
	int64_t result = WriteBatchToFileTestAccessor::call(f, &iv, 1);
	EXPECT_EQ(result, static_cast<int64_t>(sizeof(payload) - 1));

	auto written = readAll(fd_);
	ASSERT_EQ(written.size(), sizeof(payload) - 1);
	EXPECT_EQ(std::memcmp(written.data(), payload, written.size()), 0);
}

TEST_F(WriteBatchToFile, MultipleIovecsWriteAllBytes) {
	auto& f = makeFile();
	std::vector<std::vector<char>> buffers;
	std::vector<iovec> iovecs;
	makeIovecs(8, 64, buffers, iovecs);
	const size_t totalBytes = 8 * 64;

	int64_t result = WriteBatchToFileTestAccessor::call(f, iovecs.data(), static_cast<int>(iovecs.size()));
	EXPECT_EQ(result, static_cast<int64_t>(totalBytes));

	auto written = readAll(fd_);
	ASSERT_EQ(written.size(), totalBytes);
	for (int i = 0; i < 8; ++i) {
		EXPECT_EQ(std::memcmp(written.data() + i * 64, buffers[i].data(), 64), 0)
			<< "mismatch in iovec " << i;
	}
}

// Simulate short writev returns (1 byte per call) — exercises the
// byte-progress retry loop that the original bug broke.
TEST_F(WriteBatchToFile, PartialWritesRetryUntilComplete) {
	auto& f = makeFile();
	std::vector<std::vector<char>> buffers;
	std::vector<iovec> iovecs;
	makeIovecs(4, 16, buffers, iovecs);
	const size_t totalBytes = 4 * 16;

	g_writev_max_bytes_per_call = 7; // intentionally misaligned with iovec boundaries

	int64_t result = WriteBatchToFileTestAccessor::call(f, iovecs.data(), static_cast<int>(iovecs.size()));
	EXPECT_EQ(result, static_cast<int64_t>(totalBytes));

	g_writev_max_bytes_per_call = 0; // reset before reads
	auto written = readAll(fd_);
	ASSERT_EQ(written.size(), totalBytes);
	for (int i = 0; i < 4; ++i) {
		EXPECT_EQ(std::memcmp(written.data() + i * 16, buffers[i].data(), 16), 0)
			<< "byte mismatch at iovec " << i << " — partial-write retry dropped bytes";
	}
}

// Exercises the IOV_MAX chunking path (> IOV_MAX iovecs in a single call).
TEST_F(WriteBatchToFile, AboveIovMaxChunksCorrectly) {
	auto& f = makeFile();
	const int iovcnt = IOV_MAX + 100;
	std::vector<std::vector<char>> buffers;
	std::vector<iovec> iovecs;
	makeIovecs(iovcnt, 13, buffers, iovecs);
	const size_t totalBytes = static_cast<size_t>(iovcnt) * 13;

	int64_t result = WriteBatchToFileTestAccessor::call(f, iovecs.data(), iovcnt);
	EXPECT_EQ(result, static_cast<int64_t>(totalBytes));

	auto written = readAll(fd_);
	ASSERT_EQ(written.size(), totalBytes);
	for (int i = 0; i < iovcnt; ++i) {
		EXPECT_EQ(std::memcmp(written.data() + i * 13, buffers[i].data(), 13), 0)
			<< "byte mismatch at iovec " << i;
	}
}

TEST_F(WriteBatchToFile, ReportsBytesLandedOnHardError) {
	auto& f = makeFile();
	std::vector<std::vector<char>> buffers;
	std::vector<iovec> iovecs;
	makeIovecs(2, 32, buffers, iovecs);

	g_writev_budget_bytes = 20;

	int64_t bytesLanded = -1;
	int64_t result = WriteBatchToFileTestAccessor::call(f, iovecs.data(), 2, &bytesLanded);
	EXPECT_EQ(result, -1);
	EXPECT_EQ(bytesLanded, 20);

	g_writev_budget_bytes = kUnlimitedWritevBudget;
	EXPECT_EQ(readAll(fd_).size(), 20u);
}

TEST_F(WriteBatchToFile, ReportsZeroBytesLandedWhenNothingWasWritten) {
	auto& f = makeFile();
	std::vector<std::vector<char>> buffers;
	std::vector<iovec> iovecs;
	makeIovecs(2, 32, buffers, iovecs);

	g_writev_budget_bytes = 0;

	int64_t bytesLanded = -1;
	EXPECT_EQ(WriteBatchToFileTestAccessor::call(f, iovecs.data(), 2, &bytesLanded), -1);
	EXPECT_EQ(bytesLanded, 0);
}

// ---------------------------------------------------------------------------
// Append boundary — HarperFast/rocksdb-js#748
//
// A failed append used to leave the bytes it had already written on disk. The
// fd is O_APPEND, so the next successful append landed after them and the log
// carried a permanent framing break with valid entries on both sides — the one
// shape recoverTail() refuses to repair.
// ---------------------------------------------------------------------------

namespace {

std::filesystem::path uniqueLogPath(const char* name) {
	auto path = std::filesystem::temp_directory_path() /
		("rocksdb-js-append-boundary-" + std::to_string(::getpid()) + "-" + name + ".log");
	std::filesystem::remove(path);
	return path;
}

std::vector<char> readWholeFile(const std::filesystem::path& path) {
	std::ifstream stream(path, std::ios::binary);
	return std::vector<char>((std::istreambuf_iterator<char>(stream)), std::istreambuf_iterator<char>());
}

rocksdb_js::TransactionLogEntryBatch makeBatch(double timestamp, std::initializer_list<const char*> payloads) {
	rocksdb_js::TransactionLogEntryBatch batch(timestamp);
	for (const char* payload : payloads) {
		batch.addEntry(std::make_unique<rocksdb_js::TransactionLogEntry>(
			nullptr, const_cast<char*>(payload), static_cast<uint32_t>(std::strlen(payload))));
	}
	return batch;
}

class AppendBoundary : public ::testing::Test {
protected:
	void SetUp() override {
		g_writev_max_bytes_per_call = 0;
		g_writev_budget_bytes = kUnlimitedWritevBudget;
		g_write_cap_bytes = SIZE_MAX;
		g_ftruncate_calls = 0;
		rocksdb_js::TransactionLogFile::forcedBytesLandedForTests.store(INT64_MIN);
	}
	void TearDown() override {
		g_writev_max_bytes_per_call = 0;
		g_writev_budget_bytes = kUnlimitedWritevBudget;
		g_write_cap_bytes = SIZE_MAX;
		g_ftruncate_calls = 0;
		rocksdb_js::TransactionLogFile::forcedBytesLandedForTests.store(INT64_MIN);
		if (file_) {
			file_->close();
			file_.reset();
		}
		std::filesystem::remove(path_);
	}

	rocksdb_js::TransactionLogFile& openLog(const char* name) {
		path_ = uniqueLogPath(name);
		file_ = std::make_unique<rocksdb_js::TransactionLogFile>(path_, 1);
		file_->open(1000.0);
		return *file_;
	}

	std::filesystem::path path_;
	std::unique_ptr<rocksdb_js::TransactionLogFile> file_;
};

} // namespace

TEST_F(AppendBoundary, FailedPartialAppendRetiresWithoutTruncating) {
	auto& file = openLog("orphan");

	auto first = makeBatch(1001.0, { "a" });
	file.writeEntries(first, 0);
	const uint32_t committedSize = file.size.load();
	ASSERT_EQ(std::filesystem::file_size(path_), committedSize);

	// Fail six bytes into the next entry.
	g_writev_budget_bytes = 6;
	auto interrupted = makeBatch(1002.0, { "interrupted-entry" });
	EXPECT_THROW(file.writeEntries(interrupted, 0), rocksdb_js::DBException);
	g_writev_budget_bytes = kUnlimitedWritevBudget;

	EXPECT_EQ(file.size.load(), committedSize);
	EXPECT_EQ(std::filesystem::file_size(path_), committedSize + 6);
	EXPECT_EQ(g_ftruncate_calls, size_t{ 0 }) << "failed append truncated beneath a potentially live mapping";
	EXPECT_EQ(interrupted.currentEntryIndex, 0u)
		<< "the batch was marked as written when nothing reached the file";

	// The next append must be refused, leaving the orphan as a trailing partial.
	auto second = makeBatch(1003.0, { "second-entry" });
	EXPECT_THROW(file.writeEntries(second, 0), rocksdb_js::DBException);
	EXPECT_EQ(second.currentEntryIndex, 0u);
	EXPECT_EQ(std::filesystem::file_size(path_), committedSize + 6);

	auto image = readWholeFile(path_);
	auto scan = rocksdb_js::scanTransactionLogForRecovery(image.data(), static_cast<uint32_t>(image.size()));
	EXPECT_EQ(scan.kind, rocksdb_js::RecoveryScan::Kind::TruncateTail);
	EXPECT_EQ(scan.validEnd, committedSize);
}

TEST_F(AppendBoundary, FailedPartialAppendImmediatelyRotatesTheStore) {
	auto storePath = uniqueLogPath("store-rotation");
	std::filesystem::remove(storePath);
	std::filesystem::create_directories(storePath);
	uint32_t committedSize = 0;

	{
		rocksdb_js::TransactionLogStore store(
			"store-rotation", storePath, 0, std::chrono::milliseconds(0), 0);
		rocksdb_js::LogPosition firstPosition;
		auto first = makeBatch(1001.0, { "first-entry" });
		store.writeBatch(first, firstPosition);

		auto firstPath = storePath / "1.txnlog";
		committedSize = static_cast<uint32_t>(std::filesystem::file_size(firstPath));

		g_writev_budget_bytes = 6;
		rocksdb_js::LogPosition interruptedPosition;
		auto interrupted = makeBatch(1002.0, { "interrupted-entry" });
		EXPECT_THROW(store.writeBatch(interrupted, interruptedPosition), rocksdb_js::DBException);
		g_writev_budget_bytes = kUnlimitedWritevBudget;

		EXPECT_EQ(store.currentSequenceNumber.load(std::memory_order_relaxed), 2u);
		EXPECT_EQ(interrupted.currentEntryIndex, 0u);
		EXPECT_EQ(std::filesystem::file_size(firstPath), committedSize + 6);
		EXPECT_EQ(rocksdb_js::readTransactionLogAppendBoundaryMarker(firstPath), committedSize);
		EXPECT_FALSE(std::filesystem::exists(storePath / "2.txnlog"));

		rocksdb_js::LogPosition nextPosition;
		auto next = makeBatch(1003.0, { "next-entry" });
		store.writeBatch(next, nextPosition);
		EXPECT_TRUE(next.isComplete());
		EXPECT_EQ(nextPosition.logSequenceNumber, 2u);
		EXPECT_EQ(nextPosition.positionInLogFile, 0u);

		auto firstImage = readWholeFile(firstPath);
		auto firstScan = rocksdb_js::scanTransactionLogForRecovery(
			firstImage.data(), static_cast<uint32_t>(firstImage.size()));
		EXPECT_EQ(firstScan.kind, rocksdb_js::RecoveryScan::Kind::TruncateTail);
		EXPECT_EQ(firstScan.validEnd, committedSize);

		auto nextImage = readWholeFile(storePath / "2.txnlog");
		EXPECT_EQ(rocksdb_js::countTransactionLogEntries(
			nextImage.data(), static_cast<uint32_t>(nextImage.size())), 1u);

		auto snapshot = store.snapshotForBackup();
		auto retiredEntry = std::find_if(snapshot.begin(), snapshot.end(),
			[](const rocksdb_js::TransactionLogBackupEntry& entry) {
				return entry.relativeName == "1.txnlog";
			});
		ASSERT_NE(retiredEntry, snapshot.end());
		EXPECT_EQ(retiredEntry->byteLimit, committedSize);
		EXPECT_FALSE(retiredEntry->immutable);
	}

	// The marker makes the retirement authoritative after restart: the orphaned
	// physical tail is neither validated nor exposed as part of the log.
	auto reopened = rocksdb_js::TransactionLogStore::load(
		storePath, 0, std::chrono::milliseconds(0), 0);
	ASSERT_NE(reopened, nullptr);
	EXPECT_EQ(reopened->currentSequenceNumber.load(std::memory_order_relaxed), 2u);
	EXPECT_EQ(reopened->getLogFileSize(1), committedSize);
	auto validation = rocksdb_js::validateTransactionLogStore(storePath, true);
	EXPECT_TRUE(validation.valid);
	reopened->close();

	auto markerPath = rocksdb_js::transactionLogAppendBoundaryMarkerPath(storePath / "1.txnlog");
	{
		std::ofstream marker(markerPath, std::ios::binary | std::ios::trunc);
		marker.write("bad", 3);
	}
	EXPECT_THROW(
		rocksdb_js::TransactionLogStore::load(
			storePath, 0, std::chrono::milliseconds(0), 0),
		rocksdb_js::TransactionLogAppendBoundaryException);

	std::filesystem::remove_all(storePath);
	std::filesystem::remove_all(markerPath.parent_path());
}

// The Windows backend cannot always report how much of a failed append landed.
// Erasing a guessed range could cut into committed entries, so an unreported or
// impossible extent must retire the file untouched instead. Driven here through
// the POSIX build because the branch lives in the shared caller.
class UnerasableExtent : public AppendBoundary,
	public ::testing::WithParamInterface<std::pair<const char*, int64_t>> {};

TEST_P(UnerasableExtent, RetiresTheFileWithoutErasing) {
	const auto [name, forcedBytesLanded] = GetParam();
	auto& file = openLog(name);

	auto first = makeBatch(1001.0, { "first-entry" });
	file.writeEntries(first, 0);
	const uint32_t committedSize = file.size.load();

	g_writev_budget_bytes = 6;
	rocksdb_js::TransactionLogFile::forcedBytesLandedForTests.store(forcedBytesLanded);
	auto interrupted = makeBatch(1002.0, { "interrupted-entry" });
	EXPECT_THROW(file.writeEntries(interrupted, 0), rocksdb_js::DBException);
	g_writev_budget_bytes = kUnlimitedWritevBudget;
	rocksdb_js::TransactionLogFile::forcedBytesLandedForTests.store(INT64_MIN);

	// The six bytes that really landed are still there — untouched, not erased
	// against a range we could not trust.
	EXPECT_EQ(std::filesystem::file_size(path_), committedSize + 6);

	auto next = makeBatch(1003.0, { "next-entry" });
	EXPECT_THROW(file.writeEntries(next, 0), rocksdb_js::DBException);
	EXPECT_EQ(next.currentEntryIndex, 0u) << "the retired file accepted another append";
	EXPECT_EQ(std::filesystem::file_size(path_), committedSize + 6);

	auto image = readWholeFile(path_);
	auto scan = rocksdb_js::scanTransactionLogForRecovery(image.data(), static_cast<uint32_t>(image.size()));
	EXPECT_EQ(scan.kind, rocksdb_js::RecoveryScan::Kind::TruncateTail);
	EXPECT_EQ(scan.validEnd, committedSize);
}

INSTANTIATE_TEST_SUITE_P(
	AppendBoundary, UnerasableExtent,
	::testing::Values(
		std::make_pair("unknown-extent", static_cast<int64_t>(TRANSACTION_LOG_BYTES_LANDED_UNKNOWN)),
		std::make_pair("over-reported-extent", static_cast<int64_t>(1 << 20))),
	[](const ::testing::TestParamInfo<std::pair<const char*, int64_t>>& info) {
		return std::string(info.param.first == std::string("unknown-extent") ? "Unknown" : "OverReported");
	});

// A header write that lands short must take the file with it: a size in
// (0, HEADER_SIZE) fails open()'s "too small" check on every future open, and
// freeing disk space does not heal it.
TEST_F(AppendBoundary, ShortHeaderWriteDiscardsTheFileInsteadOfBrickingIt) {
	path_ = uniqueLogPath("short-header");

	{
		rocksdb_js::TransactionLogFile file(path_, 1);
		g_write_cap_bytes = 5;
		EXPECT_THROW(file.open(1000.0), rocksdb_js::DBException);
		g_write_cap_bytes = SIZE_MAX;
	}

	EXPECT_FALSE(std::filesystem::exists(path_));

	// The same path initializes cleanly once the write can complete.
	file_ = std::make_unique<rocksdb_js::TransactionLogFile>(path_, 1);
	file_->open(1000.0);
	EXPECT_EQ(file_->size.load(), static_cast<uint32_t>(TRANSACTION_LOG_FILE_HEADER_SIZE));
	EXPECT_EQ(
		std::filesystem::file_size(path_),
		static_cast<uintmax_t>(TRANSACTION_LOG_FILE_HEADER_SIZE));
}

TEST_F(AppendBoundary, AppendThatWritesNothingLeavesTheFileUntouched) {
	auto& file = openLog("nothing-landed");

	auto first = makeBatch(1001.0, { "first-entry" });
	file.writeEntries(first, 0);
	const uint32_t committedSize = file.size.load();

	g_writev_budget_bytes = 0;
	auto rejected = makeBatch(1002.0, { "rejected-entry" });
	EXPECT_THROW(file.writeEntries(rejected, 0), rocksdb_js::DBException);
	g_writev_budget_bytes = kUnlimitedWritevBudget;

	EXPECT_EQ(file.size.load(), committedSize);
	EXPECT_EQ(std::filesystem::file_size(path_), committedSize);

	auto next = makeBatch(1003.0, { "next-entry" });
	file.writeEntries(next, 0);
	EXPECT_TRUE(next.isComplete()) << "a known-zero-byte failure retired a reusable file";

	auto image = readWholeFile(path_);
	EXPECT_EQ(rocksdb_js::countTransactionLogEntries(image.data(), static_cast<uint32_t>(image.size())), 2u);
}

TEST_F(AppendBoundary, WholeBatchRotatesOrExceedsTheTargetTogether) {
	auto& file = openLog("whole-batch-source");
	auto first = makeBatch(1001.0, { "a" });
	file.writeEntries(first, 0);
	const uint32_t committedSize = file.size.load();

	auto batch = makeBatch(1002.0, { "entry-one", "entry-two" });
	const uint32_t oneEntryCapacity = committedSize + batch.entries[0]->size;
	file.writeEntries(batch, oneEntryCapacity);
	EXPECT_EQ(batch.currentEntryIndex, 0u);
	EXPECT_EQ(file.size.load(), committedSize);

	file.close();
	file_.reset();
	path_ = uniqueLogPath("whole-batch-target");
	file_ = std::make_unique<rocksdb_js::TransactionLogFile>(path_, 2);
	file_->open(1002.0);
	file_->writeEntries(batch, oneEntryCapacity);
	EXPECT_TRUE(batch.isComplete());
	EXPECT_GT(file_->size.load(), oneEntryCapacity);

	auto image = readWholeFile(path_);
	EXPECT_EQ(rocksdb_js::countTransactionLogEntries(image.data(), static_cast<uint32_t>(image.size())), 2u);
}

#endif // !_WIN32
