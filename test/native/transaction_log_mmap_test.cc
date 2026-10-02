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
#include "core/exception.h"
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

// A current segment that one batch pushed past the size it was mapped at (one
// transaction written to an empty segment may exceed transactionLogMaxSize):
// the mapping publishes the append-owned extent past its own length so a reader
// can tell it no longer covers the file, and the next request replaces it rather
// than reusing it (#889).
TEST(TransactionLogMmapOwnership, OutgrownCurrentMapReportsFullExtentAndIsReplaced) {
	const int64_t base = MemoryMap::liveCount.load();
	auto log = makeLog(8192);
	auto small = log->getMemoryMap(8192, /*isCurrent=*/true);
	ASSERT_NE(small, nullptr);
	EXPECT_EQ(small->mapSize, 8192u);
	EXPECT_EQ(small->fileSize, 8192u);
	EXPECT_EQ(small->readableExtent.load(), 8192u);

	std::vector<char> more(8192, 'y');
	ASSERT_EQ(::pwrite(log->fd, more.data(), more.size(), 8192), static_cast<ssize_t>(more.size()));
	log->size.store(16384, std::memory_order_relaxed);
	{
		std::lock_guard<std::mutex> lock(log->fileMutex);
		log->publishReadableExtentLocked();
	}
	EXPECT_EQ(small->readableExtent.load(), 16384u);
	EXPECT_EQ(small->mapSize, 8192u);

	auto big = log->getMemoryMap(8192, /*isCurrent=*/true);
	ASSERT_NE(big, nullptr);
	EXPECT_NE(big.get(), small.get());
	EXPECT_EQ(big->mapSize, 16384u);
	EXPECT_EQ(big->fileSize, 16384u);
	EXPECT_EQ(big->readableExtent.load(), 16384u);
	EXPECT_EQ(log->memoryMap.get(), big.get());
	EXPECT_EQ(MemoryMap::liveCount.load(), base + 2);
	EXPECT_EQ(static_cast<const char*>(big->map)[8192], 'y');

	small.reset();
	EXPECT_EQ(MemoryMap::liveCount.load(), base + 1);
}

// The capacity asked for only sizes a new mapping: a live one that still covers the
// file is reused however large the request, so a mapping is replaced only once the
// file outgrew it — the point at which its overlay is complete and its published extent
// already exceeds its length.
TEST(TransactionLogMmapOwnership, CoveringMapIsReusedForALargerRequest) {
	const int64_t base = MemoryMap::liveCount.load();
	auto log = makeLog(8192);
	auto map = log->getMemoryMap(8192, /*isCurrent=*/true);
	ASSERT_NE(map, nullptr);
	EXPECT_EQ(log->getMemoryMap(32768, /*isCurrent=*/true).get(), map.get());
	EXPECT_EQ(MemoryMap::liveCount.load(), base + 1);

	std::vector<char> more(100, 'z');
	ASSERT_EQ(::pwrite(log->fd, more.data(), more.size(), 8192), static_cast<ssize_t>(more.size()));
	log->size.store(8292, std::memory_order_relaxed);
	{
		std::lock_guard<std::mutex> lock(log->fileMutex);
		log->publishReadableExtentLocked();
	}
	auto grown = log->getMemoryMap(32768, /*isCurrent=*/true);
	ASSERT_NE(grown, nullptr);
	EXPECT_NE(grown.get(), map.get());
	EXPECT_EQ(grown->mapSize, 32768u);
	EXPECT_EQ(grown->fileSize, 32768u);
	EXPECT_EQ(map->readableExtent.load(), 8292u);  // published before the replacement
	EXPECT_EQ(static_cast<const char*>(grown->map)[8192], 'z');
	// a frozen handout is exactly the file's size, whatever the mapping's capacity
	log->downgradeMapToFrozen();
	EXPECT_EQ(log->getMemoryMap(8292, /*isCurrent=*/false)->fileSize, 8292u);
}

// A request smaller than the file (a store-side capacity snapshot taken before an
// append landed) still maps the whole file: the capacity is raised under fileMutex.
TEST(TransactionLogMmapOwnership, StaleCapacityRequestStillCoversTheFile) {
	auto log = makeLog(16384);
	auto map = log->getMemoryMap(8192, /*isCurrent=*/true);
	ASSERT_NE(map, nullptr);
	EXPECT_GE(map->mapSize, 16384u);
	EXPECT_EQ(map->fileSize, 16384u);
	EXPECT_EQ(map->readableExtent.load(), 16384u);
}

// A file with entries that cannot be mapped is a failure, not "every timestamp is past
// this file": the sentinel would start a reader at the file's end, past every unread
// entry. An empty file keeps the sentinel.
TEST(TransactionLogMmapOwnership, IndexWalkReportsAMappingFailureInsteadOfEndOfFile) {
	TransactionLogFile::forceMapFailureForTests.store(true);
	auto log = makeLog(8192);
	EXPECT_THROW(log->findPositionByTimestamp(1.0, 8192, /*isCurrent=*/true), rocksdb_js::DBException);
	auto empty = makeLog(0);
	EXPECT_EQ(empty->findPositionByTimestamp(1.0, 0, /*isCurrent=*/true), 0xFFFFFFFFu);
	TransactionLogFile::forceMapFailureForTests.store(false);
	EXPECT_NE(log->findPositionByTimestamp(1.0, 8192, /*isCurrent=*/true), 0xFFFFFFFFu);
}

#endif // _WIN32
