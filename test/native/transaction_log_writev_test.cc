// Unit tests for the partial-writev retry loop in
// TransactionLogFile::writeBatchToFile (POSIX).
//
// The production code calls ROCKSDB_JS_WRITEV() instead of ::writev() directly.
// This translation unit provides rocksdb_js_mock_writev(), which the test
// binary is compiled with via -DROCKSDB_JS_WRITEV=rocksdb_js_mock_writev.
// A per-test global controls whether the mock simulates partial writes.

#ifndef _WIN32

#include <gtest/gtest.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <unistd.h>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include "transaction_log/transaction_log_file.h"

// ---------------------------------------------------------------------------
// writev mock
// ---------------------------------------------------------------------------

// When non-zero, the mock writes at most this many bytes per call (simulating
// a short return). Zero means pass through to the real ::writev.
static size_t g_writev_max_bytes_per_call = 0;

extern "C" ssize_t rocksdb_js_mock_writev(int fd, const struct iovec* iov, int iovcnt) {
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
	static int64_t call(rocksdb_js::TransactionLogFile& f, const iovec* iovecs, int iovcnt) {
		return f.writeBatchToFile(iovecs, iovcnt);
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
		fd_ = -1;
		file_.reset();
	}
	void TearDown() override {
		g_writev_max_bytes_per_call = 0;
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

#endif // !_WIN32
