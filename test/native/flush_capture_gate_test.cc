#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <functional>
#include <thread>
#include "database/flush_capture_gate.h"

namespace {

bool waitUntil(const std::function<bool()>& condition) {
	for (int i = 0; i < 5000; i++) {
		if (condition()) {
			return true;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	return false;
}

} // namespace

TEST(FlushCaptureGate, FlushPassesWhenNoCaptureHoldsTheGate) {
	rocksdb_js::FlushCaptureGate gate;
	gate.enterFlushBegin();
}

TEST(FlushCaptureGate, ParksFlushesUntilTheHoldIsReleased) {
	rocksdb_js::FlushCaptureGate gate;
	std::atomic<int> passed{ 0 };
	std::thread first;
	std::thread second;
	{
		rocksdb_js::FlushCaptureHold hold(gate);
		first = std::thread([&] {
			gate.enterFlushBegin();
			passed++;
		});
		second = std::thread([&] {
			gate.enterFlushBegin();
			passed++;
		});
		EXPECT_TRUE(waitUntil([&] { return hold.parkedFlushes() == 2; }));
		std::this_thread::sleep_for(std::chrono::milliseconds(20));
		EXPECT_EQ(passed.load(), 0);
	}
	first.join();
	second.join();
	EXPECT_EQ(passed.load(), 2);

	rocksdb_js::FlushCaptureHold hold(gate);
	EXPECT_EQ(hold.parkedFlushes(), 0u);
}
