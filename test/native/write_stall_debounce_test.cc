#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#include "core/write_stall_debounce.h"

using rocksdb_js::WriteStallDebounce;

namespace {

using Clock = WriteStallDebounce::Clock;

// A fixed base so tests can express times as millisecond offsets.
Clock::time_point at(long long ms) {
	return Clock::time_point{} + std::chrono::milliseconds(ms);
}

constexpr uint64_t kWindow = 1000;

// Most tests only care about the emit decision (the bool), not the callback.
auto noop = [] {};

} // namespace

TEST(WriteStallDebounce, RisingEmitsFallingDoesNot) {
	WriteStallDebounce d;
	EXPECT_TRUE(d.onTransition("cf", true, at(0), kWindow, noop));   // rising -> emit
	EXPECT_FALSE(d.onTransition("cf", false, at(50), kWindow, noop)); // falling -> no emit
}

TEST(WriteStallDebounce, StaysStalledDoesNotReEmit) {
	WriteStallDebounce d;
	EXPECT_TRUE(d.onTransition("cf", true, at(0), kWindow, noop));
	// A second "stalled" with no intervening normal is not a rising edge.
	EXPECT_FALSE(d.onTransition("cf", true, at(10), kWindow, noop));
}

TEST(WriteStallDebounce, OscillationIsRateLimitedToOnePerWindow) {
	WriteStallDebounce d;
	EXPECT_TRUE(d.onTransition("cf", true, at(0), kWindow, noop)); // first rising emits
	// Dips and re-entries inside the window are suppressed.
	EXPECT_FALSE(d.onTransition("cf", false, at(50), kWindow, noop));
	EXPECT_FALSE(d.onTransition("cf", true, at(100), kWindow, noop));
	EXPECT_FALSE(d.onTransition("cf", false, at(150), kWindow, noop));
	EXPECT_FALSE(d.onTransition("cf", true, at(900), kWindow, noop));
	// A rising a full window after the last emit is allowed through (heartbeat).
	EXPECT_FALSE(d.onTransition("cf", false, at(1050), kWindow, noop));
	EXPECT_TRUE(d.onTransition("cf", true, at(1100), kWindow, noop));
}

TEST(WriteStallDebounce, BlipDoesNotSwallowTheNextRealStall) {
	WriteStallDebounce d;
	// Short blip: stall then quick recovery within the window.
	EXPECT_TRUE(d.onTransition("cf", true, at(0), kWindow, noop));
	EXPECT_FALSE(d.onTransition("cf", false, at(200), kWindow, noop));
	// A genuine, much-later stall must still alert (the regression: it used to be
	// swallowed because the reported-stalled bit was stranded).
	EXPECT_TRUE(d.onTransition("cf", true, at(600000), kWindow, noop));
}

TEST(WriteStallDebounce, ZeroWindowEmitsEveryRisingEdge) {
	WriteStallDebounce d;
	EXPECT_TRUE(d.onTransition("cf", true, at(0), 0, noop));
	EXPECT_FALSE(d.onTransition("cf", false, at(1), 0, noop));
	EXPECT_TRUE(d.onTransition("cf", true, at(2), 0, noop));
	EXPECT_FALSE(d.onTransition("cf", false, at(3), 0, noop));
	EXPECT_TRUE(d.onTransition("cf", true, at(4), 0, noop));
}

TEST(WriteStallDebounce, ForgetResetsState) {
	WriteStallDebounce d;
	EXPECT_TRUE(d.onTransition("cf", true, at(0), kWindow, noop));
	// Without forget, a re-stall inside the window would be suppressed.
	d.forget("cf");
	// After forget, the CF is fresh: the next rising emits immediately.
	EXPECT_TRUE(d.onTransition("cf", true, at(10), kWindow, noop));
}

TEST(WriteStallDebounce, ColumnFamiliesAreIndependent) {
	WriteStallDebounce d;
	EXPECT_TRUE(d.onTransition("a", true, at(0), kWindow, noop));
	// A different CF has its own state and emits its own rising edge.
	EXPECT_TRUE(d.onTransition("b", true, at(1), kWindow, noop));
	// Neither re-emits without a falling edge first.
	EXPECT_FALSE(d.onTransition("a", true, at(2), kWindow, noop));
	EXPECT_FALSE(d.onTransition("b", true, at(3), kWindow, noop));
}

TEST(WriteStallDebounce, EmitRunsUnderTheLockAndNeverOverlaps) {
	// RocksDB may fire a CF's callbacks from multiple threads without serializing
	// them; the FSM must still run each decision->emit atomically so enqueues can't
	// interleave and reorder. Hammer one CF from many threads and assert the emit
	// callback is never entered concurrently.
	WriteStallDebounce d;
	std::atomic<int> concurrentlyInEmit{0};
	std::atomic<bool> overlapSeen{false};
	std::atomic<int> emitCount{0};
	auto emit = [&]() {
		if (concurrentlyInEmit.fetch_add(1, std::memory_order_acq_rel) != 0) {
			overlapSeen.store(true);
		}
		std::this_thread::yield(); // widen any overlap window
		if (concurrentlyInEmit.fetch_sub(1, std::memory_order_acq_rel) != 1) {
			overlapSeen.store(true);
		}
		emitCount.fetch_add(1, std::memory_order_relaxed);
	};

	std::vector<std::thread> threads;
	for (int t = 0; t < 8; t++) {
		threads.emplace_back([&, t]() {
			for (int i = 0; i < 2000; i++) {
				const bool stalled = (i & 1) == 0;
				// windowMs = 0 so every rising edge attempts an emit (max contention).
				d.onTransition("cf", stalled, at(t * 100000LL + i), 0, emit);
			}
		});
	}
	for (auto& th : threads) {
		th.join();
	}

	EXPECT_FALSE(overlapSeen.load()) << "emit callback ran concurrently — decision/enqueue not atomic";
	EXPECT_GT(emitCount.load(), 0) << "no emits observed under contention";
}
