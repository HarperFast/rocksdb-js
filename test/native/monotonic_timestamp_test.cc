#include <gtest/gtest.h>
#include <cmath>
#include <cstdlib>
#include <limits>
#include "core/exception.h"
#include "core/platform.h"

using rocksdb_js::DBException;
using rocksdb_js::getMonotonicTimestamp;
using rocksdb_js::MAX_TIMESTAMP_MS;
using rocksdb_js::raiseMonotonicTimestampFloor;

TEST(MonotonicTimestamp, RaisingTheFloorMakesTheNextClaimItsSuccessor) {
	// An hour past wherever the clock already is (other tests in this binary
	// raise it too), so the claim is decided by the floor alone.
	const double floor = getMonotonicTimestamp() + 3600.0 * 1000.0;
	EXPECT_TRUE(raiseMonotonicTimestampFloor(floor));
	const double next = getMonotonicTimestamp();
	EXPECT_EQ(next, std::nextafter(floor, std::numeric_limits<double>::infinity()));
	EXPECT_GT(getMonotonicTimestamp(), next);
}

TEST(MonotonicTimestamp, FloorIsRaiseOnly) {
	const double current = getMonotonicTimestamp();
	EXPECT_FALSE(raiseMonotonicTimestampFloor(current - 1000.0));
	EXPECT_FALSE(raiseMonotonicTimestampFloor(current));
	EXPECT_GT(getMonotonicTimestamp(), current);
}

TEST(MonotonicTimestamp, FloorIgnoresValuesOutsideTheDomain) {
	const double before = getMonotonicTimestamp();
	EXPECT_FALSE(raiseMonotonicTimestampFloor(std::numeric_limits<double>::quiet_NaN()));
	EXPECT_FALSE(raiseMonotonicTimestampFloor(std::numeric_limits<double>::infinity()));
	EXPECT_FALSE(raiseMonotonicTimestampFloor(-std::numeric_limits<double>::infinity()));
	EXPECT_FALSE(raiseMonotonicTimestampFloor(0.0));
	EXPECT_FALSE(raiseMonotonicTimestampFloor(-1.0));
	EXPECT_FALSE(raiseMonotonicTimestampFloor(MAX_TIMESTAMP_MS));
	EXPECT_FALSE(raiseMonotonicTimestampFloor(MAX_TIMESTAMP_MS * 2));
	// None of those moved the clock past where it already was heading.
	EXPECT_LT(getMonotonicTimestamp(), before + 60.0 * 1000.0);
}

// A death test: the floor cannot be lowered again, so driving the clock to the
// cap happens in a child process rather than for the rest of this binary.
TEST(MonotonicTimestampDeathTest, ClaimsAtTheDomainCapThrow) {
	EXPECT_EXIT({
		// The largest accepted floor is one ulp below the cap (ulp == 1 there),
		// so the very next claim lands on the cap itself.
		if (!raiseMonotonicTimestampFloor(MAX_TIMESTAMP_MS - 1.0)) {
			std::_Exit(3);
		}
		try {
			getMonotonicTimestamp();
			std::_Exit(2);
		} catch (const DBException&) {
			std::_Exit(0);
		}
	}, ::testing::ExitedWithCode(0), "");
}
