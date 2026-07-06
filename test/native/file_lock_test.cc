#include <gtest/gtest.h>
#include <filesystem>
#include <string>
#include "core/exception.h"
#include "core/file_lock.h"

using rocksdb_js::DBException;
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

TEST(FileLock, ThrowsWhenParentDirectoryMissing) {
	// delete/purge do not create the directory; a lock file whose parent
	// directory is missing must surface a clear throw, not a silent lock on a
	// phantom path.
	auto missingDir = std::filesystem::temp_directory_path() / "rocksdb-js-file-lock-missing-xyz";
	std::filesystem::remove_all(missingDir);
	auto file = (missingDir / ".lock").string();
	EXPECT_THROW(tryAcquireFileLock(file), DBException);
}

TEST(FileLock, ReleaseOfUnknownTokenIsNoop) {
	// Release must tolerate token 0 and stale/unknown tokens without crashing —
	// callers run it in a finally and it must never throw.
	releaseFileLock(0);
	releaseFileLock(0xFFFFFFFF);
}
