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

	uint32_t first = tryAcquireFileLock(dir);
	EXPECT_NE(first, 0u);
	// A second acquisition of the same directory opens its own handle on the same
	// file and must fail to lock while the first is held — the exclusion two
	// concurrent backup processes rely on. 0 means "already locked".
	EXPECT_EQ(tryAcquireFileLock(dir), 0u);

	// Releasing closes the handle, which releases the kernel lock, so the
	// directory can be locked again.
	releaseFileLock(first);
	uint32_t second = tryAcquireFileLock(dir);
	EXPECT_NE(second, 0u);
	releaseFileLock(second);

	std::filesystem::remove_all(dir);
}

TEST(FileLock, ReacquirableAcrossManyCycles) {
	std::string dir = makeTempDir("rocksdb-js-file-lock-reacquire");

	// Sequential acquire/release cycles — the pattern every backup op follows —
	// must always succeed, and tokens must be distinct and non-zero.
	uint32_t prev = 0;
	for (int i = 0; i < 5; i++) {
		uint32_t token = tryAcquireFileLock(dir);
		EXPECT_NE(token, 0u);
		EXPECT_NE(token, prev);
		prev = token;
		releaseFileLock(token);
	}

	std::filesystem::remove_all(dir);
}

TEST(FileLock, ThrowsWhenDirectoryMissing) {
	// delete/purge do not create the directory; a missing directory must surface
	// a clear throw, not a silent lock on a phantom path.
	auto missing = (std::filesystem::temp_directory_path() / "rocksdb-js-file-lock-missing-xyz").string();
	std::filesystem::remove_all(missing);
	EXPECT_THROW(tryAcquireFileLock(missing), DBException);
}

TEST(FileLock, ReleaseOfUnknownTokenIsNoop) {
	// Release must tolerate token 0 and stale/unknown tokens without crashing —
	// releaseBackupDirLock runs in a finally and must never throw.
	releaseFileLock(0);
	releaseFileLock(0xFFFFFFFF);
}
