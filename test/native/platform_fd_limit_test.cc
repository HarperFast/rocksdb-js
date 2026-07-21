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
