#include <gtest/gtest.h>
#include "core/platform.h"

using rocksdb_js::deriveMaxOpenFiles;
using rocksdb_js::getEffectiveOpenFileLimit;

TEST(FdLimit, DeriveUnknownLimitIsUnlimited) {
	EXPECT_EQ(deriveMaxOpenFiles(0), -1);
}

TEST(FdLimit, DeriveClampsToFloor) {
	// An eighth of a small limit still yields the floor so RocksDB has a
	// workable table cache even under a constrained ulimit.
	EXPECT_EQ(deriveMaxOpenFiles(256), 1024);
	EXPECT_EQ(deriveMaxOpenFiles(8192), 1024);
}

TEST(FdLimit, DeriveIsAnEighthOfTheLimitInRange) {
	EXPECT_EQ(deriveMaxOpenFiles(65536), 8192);
	EXPECT_EQ(deriveMaxOpenFiles(92160), 11520);
}

TEST(FdLimit, DeriveClampsToCeiling) {
	EXPECT_EQ(deriveMaxOpenFiles(4194304), 262144);
	EXPECT_EQ(deriveMaxOpenFiles(UINT64_MAX), 262144);
}

#ifndef _WIN32
TEST(FdLimit, EffectiveLimitIsPositiveOnUnix) {
	// The exact value depends on the environment; it just has to be a real
	// limit rather than 0/unknown on POSIX systems.
	EXPECT_GT(getEffectiveOpenFileLimit(), 0u);
}
#endif

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>
#include <string>
#include <filesystem>

TEST(SyncDescriptor, SyncsRegularFileAndDirectory) {
	auto dir = std::filesystem::temp_directory_path() /
		("rocksdb-js-sync-descriptor-test-" + std::to_string(::getpid()));
	std::filesystem::create_directories(dir);
	auto file = dir / "f";
	int fd = ::open(file.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600);
	ASSERT_GE(fd, 0);
	ASSERT_EQ(::write(fd, "x", 1), 1);
	EXPECT_EQ(rocksdb_js::syncDescriptor(fd, false), 0);
	EXPECT_EQ(rocksdb_js::syncDescriptor(fd, true), 0);
	::close(fd);
	EXPECT_TRUE(rocksdb_js::syncDirectory(dir, /*allowUnsupported=*/true));
	std::filesystem::remove_all(dir);
}

TEST(SyncDescriptor, FailsOnInvalidDescriptor) {
	EXPECT_EQ(rocksdb_js::syncDescriptor(-1, false), -1);
}
#endif
