// Unit tests for TransactionLogFile::adviseCold (POSIX MADV_COLD path).
//
// The production code calls ROCKSDB_JS_MADVISE() instead of ::madvise()
// directly. This translation unit provides rocksdb_js_mock_madvise(), which the
// test binary is compiled with via -DROCKSDB_JS_MADVISE=rocksdb_js_mock_madvise.
// Per-test globals capture the call arguments and can simulate an old kernel
// returning EINVAL.

#ifndef _WIN32

#include <gtest/gtest.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include "transaction_log/transaction_log_file.h"

// ---------------------------------------------------------------------------
// madvise mock
// ---------------------------------------------------------------------------

static int g_madvise_calls = 0;
static void* g_madvise_addr = nullptr;
static size_t g_madvise_len = 0;
static int g_madvise_advice = 0;
// When non-zero, the mock returns this value and sets errno to g_madvise_errno
// instead of forwarding to the real madvise().
static int g_madvise_forced_result = 0;
static int g_madvise_errno = 0;

extern "C" int rocksdb_js_mock_madvise(void* addr, size_t len, int advice) {
	++g_madvise_calls;
	g_madvise_addr = addr;
	g_madvise_len = len;
	g_madvise_advice = advice;
	if (g_madvise_forced_result != 0) {
		errno = g_madvise_errno;
		return g_madvise_forced_result;
	}
	return ::madvise(addr, len, advice);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

namespace {

#ifdef MADV_COLD
long pageSize() {
	long ps = ::sysconf(_SC_PAGESIZE);
	return ps > 0 ? ps : 4096;
}

// Creates a temp file containing `bytes` bytes and returns its fd (unlinked, so
// it disappears when closed). Caller transfers ownership to the log file.
int makeTempFileWithBytes(size_t bytes) {
	char tmpl[] = "/tmp/rocksdb-js-madvise-test-XXXXXX";
	int fd = ::mkstemp(tmpl);
	EXPECT_GE(fd, 0);
	::unlink(tmpl);
	std::vector<char> data(bytes, 'x');
	if (bytes > 0) {
		EXPECT_EQ(::write(fd, data.data(), bytes), static_cast<ssize_t>(bytes));
	}
	return fd;
}

// Builds a TransactionLogFile backed by a temp file of `fileBytes`, mapped with
// the given requested map size, ready for adviseCold().
std::unique_ptr<rocksdb_js::TransactionLogFile> makeMappedLog(size_t fileBytes, uint32_t mapSize) {
	auto log = std::make_unique<rocksdb_js::TransactionLogFile>("/tmp/rocksdb-js-madvise-test.log", 1);
	log->fd = makeTempFileWithBytes(fileBytes);
	log->size.store(static_cast<uint32_t>(fileBytes), std::memory_order_relaxed);
	auto map = log->getMemoryMap(mapSize);
	EXPECT_NE(map, nullptr);
	return log;
}
#endif

class TransactionLogMadviseTest : public ::testing::Test {
protected:
	void SetUp() override {
		g_madvise_calls = 0;
		g_madvise_addr = nullptr;
		g_madvise_len = 0;
		g_madvise_advice = 0;
		g_madvise_forced_result = 0;
		g_madvise_errno = 0;
#ifdef ROCKSDB_JS_NATIVE_TESTS
		rocksdb_js::TransactionLogFile::resetAdviseColdSupportForTests();
#endif
	}
};

} // namespace

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// The advice covers the file-backed region floored to a page boundary, and
// never the anonymous zero-fill tail that lies beyond actualSize.
TEST_F(TransactionLogMadviseTest, AdvisesFileBackedRangeFlooredToPage) {
#ifndef MADV_COLD
	GTEST_SKIP() << "MADV_COLD not available on this platform";
#else
	const long ps = pageSize();
	const size_t fileBytes = static_cast<size_t>(ps) + 100; // not page-aligned
	const uint32_t mapSize = static_cast<uint32_t>(ps) * 4; // anon tail beyond file

	auto log = makeMappedLog(fileBytes, mapSize);
	size_t advised = log->adviseCold();

	const size_t expected = (fileBytes / static_cast<size_t>(ps)) * static_cast<size_t>(ps);
	EXPECT_EQ(advised, expected);
	EXPECT_EQ(g_madvise_calls, 1);
	EXPECT_EQ(g_madvise_advice, MADV_COLD);
	EXPECT_EQ(g_madvise_addr, log->memoryMap->map);
	EXPECT_EQ(g_madvise_len, expected);
	// Crucially, the advised length stops short of actualSize, so it can never
	// reach the anonymous tail at [actualSize, mapSize).
	EXPECT_LT(g_madvise_len, fileBytes);
#endif
}

// A file smaller than a page floors to zero — nothing is advised and no syscall
// is issued (so the anon tail can never be touched for tiny files).
TEST_F(TransactionLogMadviseTest, SubPageFileIsNoOp) {
#ifndef MADV_COLD
	GTEST_SKIP() << "MADV_COLD not available on this platform";
#else
	auto log = makeMappedLog(100, static_cast<uint32_t>(pageSize()));
	EXPECT_EQ(log->adviseCold(), 0u);
	EXPECT_EQ(g_madvise_calls, 0);
#endif
}

// On a kernel that lacks MADV_COLD (madvise returns EINVAL), adviseCold latches
// off and stops issuing the syscall on subsequent calls.
TEST_F(TransactionLogMadviseTest, LatchesOffOnEINVAL) {
#ifndef MADV_COLD
	GTEST_SKIP() << "MADV_COLD not available on this platform";
#else
	const long ps = pageSize();
	auto log = makeMappedLog(static_cast<size_t>(ps) * 2, static_cast<uint32_t>(ps) * 2);

	g_madvise_forced_result = -1;
	g_madvise_errno = EINVAL;

	EXPECT_EQ(log->adviseCold(), 0u);
	EXPECT_EQ(g_madvise_calls, 1);

	// Latched: a second call must not issue the syscall again.
	EXPECT_EQ(log->adviseCold(), 0u);
	EXPECT_EQ(g_madvise_calls, 1);
#endif
}

#endif // _WIN32
