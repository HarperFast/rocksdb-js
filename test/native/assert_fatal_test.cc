#include <gtest/gtest.h>

#include "core/assert.h"

static_assert(ROCKSDB_JS_ASSERTS_ARE_FATAL, "the native-test target must build fatal");

TEST(AssertFatal, PassingCheckContinues) {
	int evaluations = 0;
	ROCKSDB_JS_ASSERT(++evaluations == 1, "fatal passing site");
	EXPECT_EQ(evaluations, 1);
}

#if GTEST_HAS_DEATH_TEST
TEST(AssertFatalDeathTest, AssertAbortsWithLocation) {
	GTEST_FLAG_SET(death_test_style, "threadsafe");
	EXPECT_DEATH(ROCKSDB_JS_ASSERT(false, "fatal assert site"), "fatal internal check failed: fatal assert site \\(false\\) at .*assert_fatal_test.cc:");
}

TEST(AssertFatalDeathTest, ReportsEveryFailureAtOneSite) {
	GTEST_FLAG_SET(death_test_style, "threadsafe");
	std::atomic<bool> reported{ true };
	EXPECT_DEATH(rocksdb_js::assertionFailed("file.cc", 7, "x", "already reported", true, reported), "already reported \\(x\\) at file.cc:7");
}
#endif
