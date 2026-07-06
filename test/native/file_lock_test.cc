#include <gtest/gtest.h>
#include <cstdio>
#include <filesystem>
#include "core/exception.h"
#include "core/platform.h"
#ifdef _WIN32
#include <io.h>
#define ROCKSDB_JS_FILENO ::_fileno
#else
#include <unistd.h>
#define ROCKSDB_JS_FILENO ::fileno
#endif

using rocksdb_js::DBException;
using rocksdb_js::tryLockFileExclusive;

TEST(FileLock, ExclusiveAcrossOpenFileDescriptions) {
	auto path = std::filesystem::temp_directory_path() / "rocksdb-js-file-lock-exclusive.lock";
	// Two distinct open file descriptions on the same file — the granularity the
	// lock works at, and what two backup processes on one directory would each open.
	std::FILE* holder = std::fopen(path.string().c_str(), "ab+");
	ASSERT_NE(holder, nullptr);
	std::FILE* contender = std::fopen(path.string().c_str(), "ab+");
	ASSERT_NE(contender, nullptr);

	EXPECT_TRUE(tryLockFileExclusive(ROCKSDB_JS_FILENO(holder)));
	// A second open file description — same process or another — must not be able
	// to take the lock while the first holds it.
	EXPECT_FALSE(tryLockFileExclusive(ROCKSDB_JS_FILENO(contender)));

	// Closing the holder's descriptor is the release; no unlock call exists.
	std::fclose(holder);
	EXPECT_TRUE(tryLockFileExclusive(ROCKSDB_JS_FILENO(contender)));
	std::fclose(contender);

	std::filesystem::remove(path);
}

TEST(FileLock, ReacquirableAfterRelease) {
	auto path = std::filesystem::temp_directory_path() / "rocksdb-js-file-lock-reacquire.lock";

	// Sequential acquire/release cycles on fresh descriptors — the pattern every
	// backup operation follows — must always succeed.
	for (int i = 0; i < 3; i++) {
		std::FILE* file = std::fopen(path.string().c_str(), "ab+");
		ASSERT_NE(file, nullptr);
		EXPECT_TRUE(tryLockFileExclusive(ROCKSDB_JS_FILENO(file)));
		std::fclose(file);
	}

	std::filesystem::remove(path);
}

TEST(FileLock, ThrowsOnInvalidDescriptor) {
	// A genuinely bad descriptor is a programming error, not contention: it must
	// surface as a throw (EBADF on POSIX, invalid handle on Windows), not be
	// silently swallowed as "acquired" or "locked".
	EXPECT_THROW(tryLockFileExclusive(-1), DBException);
}
