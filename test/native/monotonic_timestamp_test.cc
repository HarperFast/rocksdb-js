#include <gtest/gtest.h>
#include <cmath>
#include <limits>
#include "core/platform.h"

using rocksdb_js::getMonotonicTimestamp;
using rocksdb_js::getWallClockTimestamp;
using rocksdb_js::MAX_CLOCK_FLOOR_SKEW_MS;
using rocksdb_js::MAX_TIMESTAMP_MS;
using rocksdb_js::raiseMonotonicTimestampFloor;

// The floor is one process-wide value, so these cases share it and are written
// to hold in any order: each raises it further, or asserts a refusal.

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
	// A key that far ahead is corruption, not a rollback to recover from, and
	// honoring it would move every timestamp this process issues by that much.
	double tooFar = getWallClockTimestamp() + MAX_CLOCK_FLOOR_SKEW_MS * 2;
	EXPECT_FALSE(raiseMonotonicTimestampFloor(tooFar));
	EXPECT_LT(getMonotonicTimestamp(), tooFar);
}

TEST(MonotonicTimestamp, RaisesAndThenIssuesAboveTheFloor) {
	double floor = getWallClockTimestamp() + 60.0 * 60.0 * 1000.0;
	EXPECT_TRUE(raiseMonotonicTimestampFloor(floor));

	double issued = getMonotonicTimestamp();
	EXPECT_GT(issued, floor);
	// The floor is the seed, not an open-ended jump past it.
	EXPECT_LT(issued, floor + 1000.0);
}

TEST(MonotonicTimestamp, IsRaiseOnly) {
	double floor = getWallClockTimestamp() + 2.0 * 60.0 * 60.0 * 1000.0;
	ASSERT_TRUE(raiseMonotonicTimestampFloor(floor));

	// Below the floor already in force: refused, and the clock stays above it.
	EXPECT_FALSE(raiseMonotonicTimestampFloor(floor - 60000.0));
	EXPECT_GT(getMonotonicTimestamp(), floor);
}

TEST(MonotonicTimestamp, HonorsACallerSuppliedPlausibleBound) {
	double bound = getWallClockTimestamp() + 3.0 * 60.0 * 60.0 * 1000.0;
	EXPECT_FALSE(raiseMonotonicTimestampFloor(bound + 1000.0, bound));
	EXPECT_TRUE(raiseMonotonicTimestampFloor(bound, bound));
}
