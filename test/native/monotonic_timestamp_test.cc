#include <gtest/gtest.h>
#include <cmath>
#include <limits>
#include <string>
#include "core/platform.h"

using rocksdb_js::getMonotonicTimestamp;
using rocksdb_js::getWallClockTimestamp;
using rocksdb_js::MAX_CLOCK_FLOOR_SKEW_MS;
using rocksdb_js::MAX_TIMESTAMP_MS;
using rocksdb_js::parseDurationMs;
using rocksdb_js::raiseMonotonicTimestampFloor;

// Tests share the process-global floor, so they derive targets from its current value.

TEST(MonotonicTimestamp, IssuesStrictlyIncreasingValues) {
	double first = getMonotonicTimestamp();
	double second = getMonotonicTimestamp();
	EXPECT_GT(second, first);
}

TEST(MonotonicTimestamp, TracksTheWallClockWithoutASeed) {
	double now = getWallClockTimestamp();
	double issued = getMonotonicTimestamp();
	EXPECT_GT(issued, now - 60000.0);
	EXPECT_LT(issued, now + 60000.0);
}

TEST(MonotonicTimestamp, RefusesAFloorOutsideTheDomain) {
	EXPECT_FALSE(raiseMonotonicTimestampFloor(std::numeric_limits<double>::quiet_NaN()));
	EXPECT_FALSE(raiseMonotonicTimestampFloor(std::numeric_limits<double>::infinity()));
	EXPECT_FALSE(raiseMonotonicTimestampFloor(0));
	EXPECT_FALSE(raiseMonotonicTimestampFloor(-1));
	EXPECT_FALSE(raiseMonotonicTimestampFloor(MAX_TIMESTAMP_MS));
}

TEST(MonotonicTimestamp, RefusesAFloorTooFarAheadOfTheWallClock) {
	double tooFar = getWallClockTimestamp() + MAX_CLOCK_FLOOR_SKEW_MS * 2;
	EXPECT_FALSE(raiseMonotonicTimestampFloor(tooFar));
	EXPECT_LT(getMonotonicTimestamp(), tooFar);
}

TEST(MonotonicTimestamp, RaisesAndThenIssuesAboveTheFloor) {
	double floor = getMonotonicTimestamp() + 5.0;
	EXPECT_TRUE(raiseMonotonicTimestampFloor(floor));

	double issued = getMonotonicTimestamp();
	EXPECT_GT(issued, floor);
	EXPECT_LT(issued, floor + 1000.0);
}

TEST(MonotonicTimestamp, IsRaiseOnly) {
	double floor = getMonotonicTimestamp() + 10.0;
	ASSERT_TRUE(raiseMonotonicTimestampFloor(floor));

	EXPECT_FALSE(raiseMonotonicTimestampFloor(floor - 60000.0));
	EXPECT_GT(getMonotonicTimestamp(), floor);
}

TEST(MonotonicTimestamp, HonorsACallerSuppliedPlausibleBound) {
	double bound = getMonotonicTimestamp() + 15.0;
	EXPECT_FALSE(raiseMonotonicTimestampFloor(bound + 1000.0, bound));
	EXPECT_TRUE(raiseMonotonicTimestampFloor(bound, bound));
}

// parseDurationMs backs ROCKSDB_JS_TIMESTAMP_FLOOR_SCAN_MS, whose documented cap
// an integration test cannot distinguish from the default (both finish).

TEST(ParseDurationMs, ReadsAPlainValue) {
	EXPECT_EQ(parseDurationMs("5000", 2000, 86400000), 5000u);
}

TEST(ParseDurationMs, HonorsZeroLiterally) {
	EXPECT_EQ(parseDurationMs("0", 2000, 86400000), 0u);
}

TEST(ParseDurationMs, ClampsAboveTheCap) {
	EXPECT_EQ(parseDurationMs("99999999999999", 2000, 86400000), 86400000u);
}

TEST(ParseDurationMs, ClampsAValueTooLargeForTheIntegerType) {
	// The whole point of not using stoll: this used to throw out_of_range and
	// silently become the default, contradicting the documented cap.
	std::string huge(100, '9');
	EXPECT_EQ(parseDurationMs(huge.c_str(), 2000, 86400000), 86400000u);
}

TEST(ParseDurationMs, TreatsAHugeNegativeAsMalformed) {
	std::string huge = "-" + std::string(100, '9');
	EXPECT_EQ(parseDurationMs(huge.c_str(), 2000, 86400000), 2000u);
	EXPECT_EQ(parseDurationMs("-1", 2000, 86400000), 2000u);
}

TEST(ParseDurationMs, RequiresFullConsumption) {
	EXPECT_EQ(parseDurationMs("100abc", 2000, 86400000), 2000u);
	EXPECT_EQ(parseDurationMs("1.5", 2000, 86400000), 2000u);
	EXPECT_EQ(parseDurationMs(" 100", 2000, 86400000), 2000u);
	EXPECT_EQ(parseDurationMs("+100", 2000, 86400000), 2000u);
}

TEST(ParseDurationMs, FallsBackOnNothingToParse) {
	EXPECT_EQ(parseDurationMs(nullptr, 2000, 86400000), 2000u);
	EXPECT_EQ(parseDurationMs("", 2000, 86400000), 2000u);
}
