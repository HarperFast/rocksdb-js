#include <gtest/gtest.h>

#include <atomic>
#include <cmath>
#include <cstdlib>
#include <limits>
#include <new>
#include <set>
#include <thread>
#include <vector>

#include "core/local_stamp.h"

using rocksdb_js::LOCAL_STAMP_MAX;
using rocksdb_js::LOCAL_STAMP_MAX_KEPT_SKEW_MS;
using rocksdb_js::localStampFromBits;
using rocksdb_js::localStampReserveTarget;
using rocksdb_js::localStampToBits;
using rocksdb_js::StampClaim;
using rocksdb_js::StampClaimStatus;
using rocksdb_js::tryClaimLocalStamp;

// ---- global allocation counter (B5: the keep path performs zero heap allocations) ----

static std::atomic<uint64_t> allocationCount{0};

void* operator new(std::size_t size) {
	allocationCount.fetch_add(1, std::memory_order_relaxed);
	if (void* p = std::malloc(size)) return p;
	throw std::bad_alloc();
}

void* operator new[](std::size_t size) { return ::operator new(size); }

void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

namespace {

constexpr double FULL_RESERVE = 8.639e15; // ample ceiling, below LOCAL_STAMP_MAX

std::atomic<uint64_t>& nowCallCount() {
	static std::atomic<uint64_t> count{0};
	return count;
}

double countingNow() {
	nowCallCount().fetch_add(1, std::memory_order_relaxed);
	return 1000000.0;
}

double fixedNow() { return 1000000.0; }

double lowNow() { return 10.0; }

struct Clocks {
	std::atomic<uint64_t> watermark{0};
	std::atomic<uint64_t> reserve{localStampToBits(FULL_RESERVE)};
};

} // namespace

TEST(LocalStamp, KeepsCandidateAboveWatermark) {
	Clocks c;
	auto claim = tryClaimLocalStamp(c.watermark, c.reserve, 500.0, true, fixedNow);
	EXPECT_EQ(claim.status, StampClaimStatus::Claimed);
	EXPECT_EQ(claim.value, 500.0);
	EXPECT_EQ(localStampFromBits(c.watermark.load()), 500.0);
}

TEST(LocalStamp, DuplicateCandidateRestamps) {
	Clocks c;
	auto first = tryClaimLocalStamp(c.watermark, c.reserve, 500.0, true, fixedNow);
	auto second = tryClaimLocalStamp(c.watermark, c.reserve, 500.0, true, fixedNow);
	EXPECT_EQ(first.status, StampClaimStatus::Claimed);
	EXPECT_EQ(second.status, StampClaimStatus::Claimed);
	EXPECT_GT(second.value, first.value);
}

TEST(LocalStamp, ReceiverCandidateSkipsClockRead) {
	Clocks c;
	nowCallCount().store(0);
	auto claim = tryClaimLocalStamp(c.watermark, c.reserve, 500.0, true, countingNow);
	EXPECT_EQ(claim.status, StampClaimStatus::Claimed);
	EXPECT_EQ(nowCallCount().load(), 0u) << "keep path must not read the clock";
}

TEST(LocalStamp, CallerCandidateBeyondSkewRestampsAtReceiverTime) {
	Clocks c;
	const double farFuture = fixedNow() + LOCAL_STAMP_MAX_KEPT_SKEW_MS * 10;
	auto claim = tryClaimLocalStamp(c.watermark, c.reserve, farFuture, false, fixedNow);
	EXPECT_EQ(claim.status, StampClaimStatus::Claimed);
	EXPECT_EQ(claim.value, fixedNow()) << "hostile future timestamp must not own the clock";
}

TEST(LocalStamp, CallerCandidateWithinSkewKeeps) {
	Clocks c;
	const double nearFuture = fixedNow() + LOCAL_STAMP_MAX_KEPT_SKEW_MS / 2;
	auto claim = tryClaimLocalStamp(c.watermark, c.reserve, nearFuture, false, fixedNow);
	EXPECT_EQ(claim.status, StampClaimStatus::Claimed);
	EXPECT_EQ(claim.value, nearFuture);
}

TEST(LocalStamp, ClockRollbackBeyondSkewTerminates) {
	// The rev-2 spin case: recovered floor far above wall clock. The logical
	// clock must advance from nextafter(wm) instead of spinning.
	Clocks c;
	c.watermark.store(localStampToBits(5000000.0));
	auto claim = tryClaimLocalStamp(c.watermark, c.reserve, 20.0, true, lowNow);
	EXPECT_EQ(claim.status, StampClaimStatus::Claimed);
	EXPECT_GT(claim.value, 5000000.0);
	EXPECT_EQ(claim.value, std::nextafter(5000000.0, std::numeric_limits<double>::infinity()));
}

TEST(LocalStamp, InvalidCandidatesRestamp) {
	for (double bad : {
			std::numeric_limits<double>::quiet_NaN(),
			std::numeric_limits<double>::infinity(),
			-std::numeric_limits<double>::infinity(),
			-5.0,
			0.0,
			LOCAL_STAMP_MAX,
			LOCAL_STAMP_MAX * 2,
		}) {
		Clocks c;
		auto claim = tryClaimLocalStamp(c.watermark, c.reserve, bad, false, fixedNow);
		EXPECT_EQ(claim.status, StampClaimStatus::Claimed) << "candidate " << bad;
		EXPECT_TRUE(std::isfinite(claim.value)) << "candidate " << bad;
		EXPECT_GT(claim.value, 0.0) << "candidate " << bad;
		EXPECT_LT(claim.value, LOCAL_STAMP_MAX) << "candidate " << bad;
	}
}

TEST(LocalStamp, ExhaustionIsTerminalNotASpin) {
	Clocks c;
	const double nearMax =
		std::nextafter(LOCAL_STAMP_MAX, -std::numeric_limits<double>::infinity());
	c.watermark.store(localStampToBits(nearMax));
	auto claim = tryClaimLocalStamp(c.watermark, c.reserve, 1.0, true, fixedNow);
	EXPECT_EQ(claim.status, StampClaimStatus::Exhausted);
}

TEST(LocalStamp, NeedsReserveThenClaimAfterExtension) {
	Clocks c;
	c.reserve.store(localStampToBits(100.0));
	auto blocked = tryClaimLocalStamp(c.watermark, c.reserve, 500.0, true, fixedNow);
	ASSERT_EQ(blocked.status, StampClaimStatus::NeedsReserve);
	EXPECT_EQ(blocked.value, 500.0);
	// Watermark must not have advanced past the ceiling.
	EXPECT_EQ(localStampFromBits(c.watermark.load()), 0.0);

	const double target = localStampReserveTarget(blocked.value, fixedNow(), 300000.0);
	ASSERT_GE(target, blocked.value);
	c.reserve.store(localStampToBits(target));
	auto claim = tryClaimLocalStamp(c.watermark, c.reserve, 500.0, true, fixedNow);
	EXPECT_EQ(claim.status, StampClaimStatus::Claimed);
	EXPECT_EQ(claim.value, 500.0);
}

TEST(LocalStamp, ReserveTargetIsCheckedNearDomainBound) {
	const double nearMax = LOCAL_STAMP_MAX - 1.0;
	const double target = localStampReserveTarget(nearMax, nearMax, 300000.0);
	EXPECT_LT(target, LOCAL_STAMP_MAX);
	// Exhausted domain: target below the requested value tells the caller to
	// fail open rather than persist.
	const double edge = std::nextafter(LOCAL_STAMP_MAX, -std::numeric_limits<double>::infinity());
	EXPECT_LE(localStampReserveTarget(edge, edge, 300000.0), edge);
}

TEST(LocalStamp, KeepPathPerformsZeroAllocations) {
	Clocks c;
	// Warm up so lazily initialized state is out of the way.
	tryClaimLocalStamp(c.watermark, c.reserve, 10.0, true, fixedNow);
	const uint64_t before = allocationCount.load(std::memory_order_relaxed);
	for (int i = 0; i < 1000; i++) {
		auto claim = tryClaimLocalStamp(
			c.watermark, c.reserve, 100.0 + static_cast<double>(i), true, fixedNow);
		ASSERT_EQ(claim.status, StampClaimStatus::Claimed);
	}
	// Re-stamp path too: candidates below the watermark.
	for (int i = 0; i < 1000; i++) {
		auto claim = tryClaimLocalStamp(c.watermark, c.reserve, 1.0, false, fixedNow);
		ASSERT_EQ(claim.status, StampClaimStatus::Claimed);
	}
	EXPECT_EQ(allocationCount.load(std::memory_order_relaxed), before);
}

TEST(LocalStamp, UniqueAndMonotonicUnderThreads) {
	Clocks c;
	constexpr int THREADS = 8;
	constexpr int CLAIMS = 5000;
	std::vector<std::vector<double>> results(THREADS);
	std::vector<std::thread> threads;
	threads.reserve(THREADS);
	for (int t = 0; t < THREADS; t++) {
		threads.emplace_back([&c, &results, t] {
			auto& mine = results[t];
			mine.reserve(CLAIMS);
			for (int i = 0; i < CLAIMS; i++) {
				// Mix keep-shaped, duplicate, and stale candidates.
				const double candidate = (i % 3 == 0) ? 250.0 : 500.0 + static_cast<double>(i);
				auto claim = tryClaimLocalStamp(c.watermark, c.reserve, candidate, i % 2 == 0, fixedNow);
				ASSERT_EQ(claim.status, StampClaimStatus::Claimed);
				mine.push_back(claim.value);
			}
		});
	}
	for (auto& thread : threads) thread.join();

	std::set<double> all;
	for (const auto& mine : results) {
		double last = 0.0;
		for (double value : mine) {
			EXPECT_GT(value, last) << "claims must be strictly increasing per thread";
			last = value;
			all.insert(value);
		}
	}
	EXPECT_EQ(all.size(), static_cast<size_t>(THREADS * CLAIMS)) << "every claim must be unique";
}
