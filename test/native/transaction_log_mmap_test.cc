// Unit tests for TransactionLogFile memory-map ownership (POSIX):
// the current (actively-written) file retains a strong reference to its map,
// while a frozen file keeps only a weak handle so the mapping is released as
// soon as the caller (the JS external buffer) drops it. Verified via the
// process-global MemoryMap::liveCount.

#ifndef _WIN32

#include <gtest/gtest.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdint>
#include <memory>
#include <vector>
#include "transaction_log/transaction_log_file.h"

using rocksdb_js::MemoryMap;
using rocksdb_js::TransactionLogFile;

namespace {

int makeTempFileWithBytes(size_t bytes) {
	char tmpl[] = "/tmp/rocksdb-js-mmap-test-XXXXXX";
	int fd = ::mkstemp(tmpl);
	EXPECT_GE(fd, 0);
	::unlink(tmpl);
	std::vector<char> data(bytes, 'x');
	if (bytes > 0) {
		EXPECT_EQ(::write(fd, data.data(), bytes), static_cast<ssize_t>(bytes));
	}
	return fd;
}

std::unique_ptr<TransactionLogFile> makeLog(size_t bytes) {
	auto log = std::make_unique<TransactionLogFile>("/tmp/rocksdb-js-mmap-test.log", 1);
	log->fd = makeTempFileWithBytes(bytes);
	log->size.store(static_cast<uint32_t>(bytes), std::memory_order_relaxed);
	return log;
}

} // namespace

// A frozen file does not retain the map; dropping the returned shared_ptr (what
// the JS external buffer holds) unmaps it.
TEST(TransactionLogMmapOwnership, FrozenMapReleasedWhenCallerDropsIt) {
	const int64_t base = MemoryMap::liveCount.load();
	auto log = makeLog(8192);
	{
		auto map = log->getMemoryMap(8192, /*isCurrent=*/false);
		ASSERT_NE(map, nullptr);
		EXPECT_EQ(MemoryMap::liveCount.load(), base + 1);
		EXPECT_EQ(log->memoryMap, nullptr);              // no strong reference retained
		EXPECT_NE(log->frozenMapCache.lock(), nullptr);  // weak handle while caller holds it
	}
	// Only the caller held the map; it must now be unmapped.
	EXPECT_EQ(MemoryMap::liveCount.load(), base);
	EXPECT_EQ(log->frozenMapCache.lock(), nullptr);
}

// The current file retains a strong reference; the map outlives the caller's
// reference and is released when the file is destroyed.
TEST(TransactionLogMmapOwnership, CurrentMapRetainedByFile) {
	const int64_t base = MemoryMap::liveCount.load();
	auto log = makeLog(8192);
	{
		auto map = log->getMemoryMap(8192, /*isCurrent=*/true);
		ASSERT_NE(map, nullptr);
		EXPECT_NE(log->memoryMap, nullptr);  // strong reference retained
	}
	// Caller dropped its reference, but the file still holds one.
	EXPECT_EQ(MemoryMap::liveCount.load(), base + 1);
	log.reset();  // destructor -> close() releases the map
	EXPECT_EQ(MemoryMap::liveCount.load(), base);
}

// Downgrading a current file's map (on rotation) drops the file's strong ref;
// the mapping then lives only as long as the caller's reference (the JS buffer).
TEST(TransactionLogMmapOwnership, DowngradeReleasesCurrentMapWhenCallerDropsIt) {
	const int64_t base = MemoryMap::liveCount.load();
	auto log = makeLog(8192);
	auto held = log->getMemoryMap(8192, /*isCurrent=*/true);
	ASSERT_NE(held, nullptr);
	EXPECT_NE(log->memoryMap, nullptr);

	log->downgradeMapToFrozen();
	EXPECT_EQ(log->memoryMap, nullptr);              // strong ref dropped
	EXPECT_NE(log->frozenMapCache.lock(), nullptr);  // weak handle while caller holds it
	EXPECT_EQ(MemoryMap::liveCount.load(), base + 1);  // still mapped — caller holds it

	held.reset();
	EXPECT_EQ(MemoryMap::liveCount.load(), base);  // last strong ref gone -> unmapped
}

// Concurrent/sequential frozen handouts share a single mapping while one is
// still alive (deduped via the weak frozenMapCache).
TEST(TransactionLogMmapOwnership, FrozenHandoutsShareOneMap) {
	const int64_t base = MemoryMap::liveCount.load();
	auto log = makeLog(8192);
	auto a = log->getMemoryMap(8192, /*isCurrent=*/false);
	auto b = log->getMemoryMap(8192, /*isCurrent=*/false);
	ASSERT_NE(a, nullptr);
	ASSERT_NE(b, nullptr);
	EXPECT_EQ(a.get(), b.get());
	EXPECT_EQ(MemoryMap::liveCount.load(), base + 1);  // one mapping, not two
}

#endif // _WIN32
