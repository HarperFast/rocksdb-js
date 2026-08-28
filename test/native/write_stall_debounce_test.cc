#include <gtest/gtest.h>
#include <chrono>
#include "core/write_stall_debounce.h"

using rocksdb_js::WriteStallDebounce;

namespace {

using Clock = WriteStallDebounce::Clock;

// A fixed base so tests can express times as millisecond offsets.
Clock::time_point at(long long ms) {
	return Clock::time_point{} + std::chrono::milliseconds(ms);
}

constexpr uint64_t kWindow = 1000;

} // namespace

TEST(WriteStallDebounce, RisingEmitsFallingDoesNot) {
	WriteStallDebounce d;
	EXPECT_TRUE(d.onTransition("cf", true, at(0), kWindow));   // rising -> emit
	EXPECT_FALSE(d.onTransition("cf", false, at(50), kWindow)); // falling -> no emit
}

TEST(WriteStallDebounce, StaysStalledDoesNotReEmit) {
	WriteStallDebounce d;
	EXPECT_TRUE(d.onTransition("cf", true, at(0), kWindow));
	// A second "stalled" with no intervening normal is not a rising edge.
	EXPECT_FALSE(d.onTransition("cf", true, at(10), kWindow));
}

TEST(WriteStallDebounce, OscillationIsRateLimitedToOnePerWindow) {
	WriteStallDebounce d;
	EXPECT_TRUE(d.onTransition("cf", true, at(0), kWindow)); // first rising emits
	// Dips and re-entries inside the window are suppressed.
	EXPECT_FALSE(d.onTransition("cf", false, at(50), kWindow));
	EXPECT_FALSE(d.onTransition("cf", true, at(100), kWindow));
	EXPECT_FALSE(d.onTransition("cf", false, at(150), kWindow));
	EXPECT_FALSE(d.onTransition("cf", true, at(900), kWindow));
	// A rising a full window after the last emit is allowed through (heartbeat).
	EXPECT_FALSE(d.onTransition("cf", false, at(1050), kWindow));
	EXPECT_TRUE(d.onTransition("cf", true, at(1100), kWindow));
}

TEST(WriteStallDebounce, BlipDoesNotSwallowTheNextRealStall) {
	WriteStallDebounce d;
	// Short blip: stall then quick recovery within the window.
	EXPECT_TRUE(d.onTransition("cf", true, at(0), kWindow));
	EXPECT_FALSE(d.onTransition("cf", false, at(200), kWindow));
	// A genuine, much-later stall must still alert (the regression: it used to be
	// swallowed because the reported-stalled bit was stranded).
	EXPECT_TRUE(d.onTransition("cf", true, at(600000), kWindow));
}

TEST(WriteStallDebounce, ZeroWindowEmitsEveryRisingEdge) {
	WriteStallDebounce d;
	EXPECT_TRUE(d.onTransition("cf", true, at(0), 0));
	EXPECT_FALSE(d.onTransition("cf", false, at(1), 0));
	EXPECT_TRUE(d.onTransition("cf", true, at(2), 0));
	EXPECT_FALSE(d.onTransition("cf", false, at(3), 0));
	EXPECT_TRUE(d.onTransition("cf", true, at(4), 0));
}

TEST(WriteStallDebounce, ForgetResetsState) {
	WriteStallDebounce d;
	EXPECT_TRUE(d.onTransition("cf", true, at(0), kWindow));
	// Without forget, a re-stall inside the window would be suppressed.
	d.forget("cf");
	// After forget, the CF is fresh: the next rising emits immediately.
	EXPECT_TRUE(d.onTransition("cf", true, at(10), kWindow));
}

TEST(WriteStallDebounce, ColumnFamiliesAreIndependent) {
	WriteStallDebounce d;
	EXPECT_TRUE(d.onTransition("a", true, at(0), kWindow));
	// A different CF has its own state and emits its own rising edge.
	EXPECT_TRUE(d.onTransition("b", true, at(1), kWindow));
	// Neither re-emits without a falling edge first.
	EXPECT_FALSE(d.onTransition("a", true, at(2), kWindow));
	EXPECT_FALSE(d.onTransition("b", true, at(3), kWindow));
}
