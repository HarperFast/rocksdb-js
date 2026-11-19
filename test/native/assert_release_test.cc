#include <gtest/gtest.h>
#include <barrier>
#include <cerrno>
#include <string>
#include <thread>
#include <vector>

// Compile this file as a published release build does, even though the test target defines
// ROCKSDB_JS_FATAL_ASSERTS. NDEBUG is defined on purpose: the release mode must not depend on it.
#undef DEBUG
#undef ROCKSDB_JS_FATAL_ASSERTS
#define NDEBUG 1
#include "core/assert.h"
#include "core/inline_vector.h"

static_assert(!ROCKSDB_JS_ASSERTS_ARE_FATAL, "this file must exercise the release mode");

namespace {

size_t countOccurrences(const std::string& haystack, const std::string& needle) {
	size_t count = 0;
	for (size_t at = haystack.find(needle); at != std::string::npos; at = haystack.find(needle, at + needle.size())) {
		++count;
	}
	return count;
}

int firstSite(bool ok) {
	ROCKSDB_JS_ASSERT(ok, "release first site");
	return 1;
}

int secondSite(bool ok) {
	ROCKSDB_JS_ASSERT(ok, "release second site");
	return 2;
}

void contendedSite() {
	ROCKSDB_JS_ASSERT(false, "release contended site");
}

} // namespace

TEST(AssertRelease, PassingCheckEvaluatesOnceAndWritesNothing) {
	int evaluations = 0;
	testing::internal::CaptureStderr();
	ROCKSDB_JS_ASSERT(++evaluations == 1, "release passing site");
	EXPECT_EQ(testing::internal::GetCapturedStderr(), "");
	EXPECT_EQ(evaluations, 1);
}

TEST(AssertRelease, WarnsOncePerCallSiteAndContinues) {
	testing::internal::CaptureStderr();
	EXPECT_EQ(firstSite(false), 1);
	EXPECT_EQ(firstSite(false), 1);
	EXPECT_EQ(secondSite(false), 2);
	EXPECT_EQ(secondSite(false), 2);
	const std::string output = testing::internal::GetCapturedStderr();

	EXPECT_EQ(countOccurrences(output, "release first site"), 1u) << output;
	EXPECT_EQ(countOccurrences(output, "release second site"), 1u) << output;
	EXPECT_NE(output.find("rocksdb-js: internal check failed, continuing: release first site (ok) at "), std::string::npos) << output;
	EXPECT_NE(output.find("assert_release_test.cc:"), std::string::npos) << output;
}

TEST(AssertRelease, ConcurrentFirstFailuresWarnOnce) {
	constexpr int threadCount = 8;
	std::barrier start(threadCount);
	std::vector<std::thread> threads;
	testing::internal::CaptureStderr();
	for (int i = 0; i < threadCount; ++i) {
		threads.emplace_back([&start]() {
			start.arrive_and_wait();
			contendedSite();
		});
	}
	for (auto& thread : threads) {
		thread.join();
	}
	const std::string output = testing::internal::GetCapturedStderr();
	EXPECT_EQ(countOccurrences(output, "release contended site"), 1u) << output;
}

TEST(AssertRelease, PreservesErrno) {
	testing::internal::CaptureStderr();
	errno = ERANGE;
	ROCKSDB_JS_ASSERT(false, "release errno site");
	const int observed = errno;
	testing::internal::GetCapturedStderr();
	EXPECT_EQ(observed, ERANGE);
}

TEST(AssertRelease, TruncatesOversizedReportToOneLine) {
	const std::string longMessage(4096, 'm');
	testing::internal::CaptureStderr();
	ROCKSDB_JS_ASSERT(false, longMessage.c_str());
	const std::string output = testing::internal::GetCapturedStderr();
	ASSERT_FALSE(output.empty());
	EXPECT_LT(output.size(), 1024u);
	EXPECT_EQ(output.back(), '\n');
	EXPECT_EQ(countOccurrences(output, "\n"), 1u);
}

#if GTEST_HAS_DEATH_TEST
TEST(AssertReleaseDeathTest, CheckIsFatalInReleaseBuilds) {
	GTEST_FLAG_SET(death_test_style, "threadsafe");
	EXPECT_DEATH(ROCKSDB_JS_CHECK(false, "release check site"), "fatal internal check failed: release check site");
}
#endif

TEST(InlineVector, LastIsNullOnlyWhenEmpty) {
	rocksdb_js::InlineVector<int, 2> values;
	EXPECT_EQ(values.last(), nullptr);

	values.add(1);
	ASSERT_NE(values.last(), nullptr);
	EXPECT_EQ(*values.last(), 1);

	values.add(2);
	EXPECT_EQ(*values.last(), 2);

	values.add(3);
	EXPECT_EQ(*values.last(), 3);

	values.popBack();
	EXPECT_EQ(*values.last(), 2);
	values.popBack();
	EXPECT_EQ(*values.last(), 1);
	values.popBack();
	EXPECT_EQ(values.last(), nullptr);

	values.popBack();
	EXPECT_EQ(values.size(), 0u);
	EXPECT_EQ(values.last(), nullptr);
}
