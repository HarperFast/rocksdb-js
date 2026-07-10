#include <gtest/gtest.h>
#include <filesystem>
#include <string>
#include "core/file_lock.h"

using rocksdb_js::releaseFileLock;
using rocksdb_js::tryAcquireFileLock;

namespace {

std::string makeTempDir(const char* name) {
	auto dir = std::filesystem::temp_directory_path() / name;
	std::filesystem::remove_all(dir);
	std::filesystem::create_directories(dir);
	return dir.string();
}

} // namespace

TEST(FileLock, ExclusiveWhileHeldThenReacquirable) {
	std::string dir = makeTempDir("rocksdb-js-file-lock-exclusive");
	std::string file = (std::filesystem::path(dir) / ".lock").string();

	uint32_t first = tryAcquireFileLock(file);
	EXPECT_NE(first, 0u);
	// A second acquisition of the same file opens its own handle on it and must
	// fail to lock while the first is held — the exclusion two concurrent
	// backup processes rely on. 0 means "already locked".
	EXPECT_EQ(tryAcquireFileLock(file), 0u);

	// Releasing closes the handle, which releases the kernel lock, so the
	// file can be locked again.
	releaseFileLock(first);
	uint32_t second = tryAcquireFileLock(file);
	EXPECT_NE(second, 0u);
	releaseFileLock(second);

	std::filesystem::remove_all(dir);
}

TEST(FileLock, ReacquirableAcrossManyCycles) {
	std::string dir = makeTempDir("rocksdb-js-file-lock-reacquire");
	std::string file = (std::filesystem::path(dir) / ".lock").string();

	// Sequential acquire/release cycles — the pattern every backup op follows —
	// must always succeed, and tokens must be distinct and non-zero.
	uint32_t prev = 0;
	for (int i = 0; i < 5; i++) {
		uint32_t token = tryAcquireFileLock(file);
		EXPECT_NE(token, 0u);
		EXPECT_NE(token, prev);
		prev = token;
		releaseFileLock(token);
	}

	std::filesystem::remove_all(dir);
}

TEST(FileLock, SharedHoldersCoexistButExcludeExclusive) {
	std::string dir = makeTempDir("rocksdb-js-file-lock-shared");
	std::string file = (std::filesystem::path(dir) / ".lock").string();

	// Shared holders coexist — the pattern concurrent restores rely on.
	uint32_t reader1 = tryAcquireFileLock(file, true);
	uint32_t reader2 = tryAcquireFileLock(file, true);
	EXPECT_NE(reader1, 0u);
	EXPECT_NE(reader2, 0u);

	// A writer (exclusive) must fail while any shared holder remains — this is
	// what stops a purge from deleting files out from under a running restore.
	EXPECT_EQ(tryAcquireFileLock(file), 0u);
	releaseFileLock(reader1);
	EXPECT_EQ(tryAcquireFileLock(file), 0u);
	releaseFileLock(reader2);

	uint32_t writer = tryAcquireFileLock(file);
	EXPECT_NE(writer, 0u);
	// And a shared acquire must fail while the exclusive holder remains.
	EXPECT_EQ(tryAcquireFileLock(file, true), 0u);
	releaseFileLock(writer);

	std::filesystem::remove_all(dir);
}

TEST(FileLock, AcquirableOnNonAsciiPath) {
	// Node passes UTF-8 paths via N-API; on Windows tryAcquireFileLock converts to
	// UTF-16 before CreateFileW. Build the path as UTF-8 bytes, not path::string()
	// (which uses the ANSI code page on Windows).
	auto dir = std::filesystem::temp_directory_path() / u8"rocksdb-js-file-lock-caf\u00e9";
	std::filesystem::remove_all(dir);
	std::filesystem::create_directories(dir);
	auto lockPath = dir / u8".lock";
	std::u8string lockPathUtf8 = lockPath.u8string();
	std::string file(reinterpret_cast<const char*>(lockPathUtf8.data()), lockPathUtf8.size());

	uint32_t token = tryAcquireFileLock(file);
	EXPECT_NE(token, 0u);
	EXPECT_EQ(tryAcquireFileLock(file), 0u);

	releaseFileLock(token);
	std::filesystem::remove_all(dir);
}

TEST(FileLock, CreatesMissingParentDirectories) {
	// The caller's intent is "make this lock exist" — missing parents are
	// created, not surfaced as errors. (Backup ops that must reject a missing
	// directory check its existence explicitly in withBackupDirLock.)
	auto missingDir = std::filesystem::temp_directory_path() / "rocksdb-js-file-lock-missing-xyz";
	std::filesystem::remove_all(missingDir);
	auto file = (missingDir / "nested" / ".lock").string();

	uint32_t token = tryAcquireFileLock(file);
	EXPECT_NE(token, 0u);
	EXPECT_TRUE(std::filesystem::exists(file));
	releaseFileLock(token);

	std::filesystem::remove_all(missingDir);
}

TEST(FileLock, ReleaseOfUnknownTokenIsNoop) {
	// Release must tolerate token 0 and stale/unknown tokens without crashing —
	// callers run it in a finally and it must never throw.
	releaseFileLock(0);
	releaseFileLock(0xFFFFFFFF);
}
