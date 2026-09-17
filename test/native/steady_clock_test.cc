#include <gtest/gtest.h>
#include "core/platform.h"

#include <array>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <thread>
#include <vector>

using rocksdb_js::getMonotonicTimestamp;
using rocksdb_js::getSteadyClockNow;
using rocksdb_js::steadyClockMilliseconds;

namespace {

using steady_duration = std::chrono::steady_clock::duration;
static_assert(std::chrono::steady_clock::is_steady);

steady_duration fromNanoseconds(int64_t nanoseconds) {
	return std::chrono::duration_cast<steady_duration>(std::chrono::nanoseconds(nanoseconds));
}

constexpr int64_t kNanosPerMilli = 1000000;
constexpr int64_t kNanosPerYear = 365LL * 24 * 3600 * 1000 * kNanosPerMilli;

} // namespace

TEST(SteadyClock, ConversionIsExactOnMillisecondBoundaries) {
	EXPECT_EQ(steadyClockMilliseconds(steady_duration::zero()), 0.0);
	EXPECT_EQ(steadyClockMilliseconds(fromNanoseconds(kNanosPerMilli)), 1.0);
	EXPECT_EQ(steadyClockMilliseconds(fromNanoseconds(1500 * kNanosPerMilli)), 1500.0);
	EXPECT_EQ(steadyClockMilliseconds(fromNanoseconds(10 * kNanosPerYear)), 315360000000.0);
}

TEST(SteadyClock, ConversionCarriesSubMillisecondFraction) {
	// Representation precision does not imply the OS clock achieves this resolution.
	EXPECT_DOUBLE_EQ(steadyClockMilliseconds(fromNanoseconds(1000)), 0.001);
	EXPECT_DOUBLE_EQ(steadyClockMilliseconds(fromNanoseconds(100)), 0.0001);
	EXPECT_DOUBLE_EQ(steadyClockMilliseconds(fromNanoseconds(kNanosPerMilli + 100)), 1.0001);
}

TEST(SteadyClock, MicrosecondStepsStayStrictlyIncreasingAcrossACentury) {
	// The ulp of the millisecond double grows with distance from the origin:
	// ~61 ns at 10 years, ~0.5 µs at 100 years. 1 µs steps must therefore still
	// map to strictly larger values at both, which is the precision the contract
	// promises (sub-microsecond for any realistic uptime).
	for (int64_t years : {0LL, 1LL, 10LL, 100LL}) {
		const int64_t base = years * kNanosPerYear + 123456789;
		double previous = steadyClockMilliseconds(fromNanoseconds(base));
		for (int step = 1; step <= 1000; ++step) {
			const double next = steadyClockMilliseconds(fromNanoseconds(base + step * 1000));
			EXPECT_GT(next, previous) << "years=" << years << " step=" << step;
			previous = next;
		}
	}
}

TEST(SteadyClock, ConversionIsNonDecreasingOnAdjacentTicks) {
	// Positive scaling + round-to-nearest is monotone; adjacent clock ticks may
	// collapse to equality far from the origin but never invert. Sample the
	// boundary regions where the double's spacing crosses the tick period.
	for (int64_t base : std::array<int64_t, 4>{
		0LL,
		(1LL << 33) * kNanosPerMilli - 5000,  // ~99 days: spacing reaches ~2 ns
		(1LL << 38) * kNanosPerMilli - 5000,  // ~8.7 years
		100 * kNanosPerYear,
	}) {
		double previous = steadyClockMilliseconds(fromNanoseconds(base));
		for (int64_t tick = 1; tick <= 10000; ++tick) {
			const double next = steadyClockMilliseconds(fromNanoseconds(base + tick));
			EXPECT_GE(next, previous) << "base=" << base << " tick=" << tick;
			previous = next;
		}
	}
}

TEST(SteadyClock, NowTracksSteadyClockDirectly) {
	const auto before = std::chrono::steady_clock::now();
	const double sample = getSteadyClockNow();
	const auto after = std::chrono::steady_clock::now();
	EXPECT_GE(sample, steadyClockMilliseconds(before.time_since_epoch()));
	EXPECT_LE(sample, steadyClockMilliseconds(after.time_since_epoch()));
	EXPECT_TRUE(std::isfinite(sample));
}

TEST(SteadyClock, MeasuresElapsedTimeAcrossASleep) {
	const double start = getSteadyClockNow();
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	EXPECT_GE(getSteadyClockNow() - start, 19.0);
}

TEST(SteadyClock, AdjacentTicksCanCompareEqual) {
	const auto base = fromNanoseconds(100 * kNanosPerYear);
	EXPECT_EQ(steadyClockMilliseconds(base), steadyClockMilliseconds(base + steady_duration(1)));
}

TEST(SteadyClock, NeverDecreasesInATightLoop) {
	double previous = getSteadyClockNow();
	for (int i = 0; i < 100000; ++i) {
		const double next = getSteadyClockNow();
		ASSERT_GE(next, previous) << "iteration " << i;
		previous = next;
	}
}

TEST(SteadyClock, ThreadsShareOneDomain) {
	// Samples taken on any thread fall inside the main thread's before/after
	// bracket, and a thread's own samples across a sleep show elapsed progress.
	constexpr int kThreads = 8;
	std::vector<std::thread> threads;
	std::vector<double> first(kThreads), second(kThreads);

	const double before = getSteadyClockNow();
	std::this_thread::sleep_for(std::chrono::milliseconds(10));
	for (int i = 0; i < kThreads; ++i) {
		threads.emplace_back([&, i] {
			first[i] = getSteadyClockNow();
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
			second[i] = getSteadyClockNow();
		});
	}
	for (auto& thread : threads) {
		thread.join();
	}
	const double after = getSteadyClockNow();

	for (int i = 0; i < kThreads; ++i) {
		// A thread-relative origin would read near 0 here, far below `before + 10`.
		EXPECT_GE(first[i], before + 9.0) << "thread " << i;
		EXPECT_GE(second[i] - first[i], 9.0) << "thread " << i;
		EXPECT_LE(second[i], after) << "thread " << i;
	}
}

TEST(SteadyClock, DoesNotDisturbTheWallClockRatchet) {
	// Regression for the separate contract: getMonotonicTimestamp() stays in
	// Unix-epoch milliseconds and strictly increasing while steady samples are
	// interleaved with it.
	const auto epochMs = [] {
		return static_cast<double>(std::chrono::duration_cast<std::chrono::milliseconds>(
			std::chrono::system_clock::now().time_since_epoch()
		).count());
	};

	double previous = getMonotonicTimestamp();
	for (int i = 0; i < 1000; ++i) {
		(void)getSteadyClockNow();
		const double next = getMonotonicTimestamp();
		ASSERT_GT(next, previous) << "iteration " << i;
		previous = next;
	}
	EXPECT_LE(std::fabs(previous - epochMs()), 1000.0);
}
