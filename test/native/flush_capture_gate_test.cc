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

TEST(FlushCaptureGate, AFlushParkedTooLongProceedsAndMarksTheHold) {
	rocksdb_js::FlushCaptureGate gate(std::chrono::milliseconds(20));
	rocksdb_js::FlushCaptureHold hold(gate);
	EXPECT_FALSE(hold.flushEscaped());
	std::thread flush([&] { gate.enterFlushBegin(); });
	flush.join();
	EXPECT_TRUE(hold.flushEscaped());
	EXPECT_EQ(hold.parkedFlushes(), 0u);
}

TEST(FlushCaptureGate, AFlushStillParkedWhenTheNextHoldIsRaisedStaysParkedUnderIt) {
	rocksdb_js::FlushCaptureGate gate(std::chrono::milliseconds(400));
	std::atomic<bool> passed{ false };
	std::thread flush;
	{
		rocksdb_js::FlushCaptureHold first(gate);
		flush = std::thread([&] {
			gate.enterFlushBegin();
			passed = true;
		});
		EXPECT_TRUE(waitUntil([&] { return first.parkedFlushes() == 1; }));
		std::this_thread::sleep_for(std::chrono::milliseconds(250));
		{
			// The next hold's generation, raised before the flush could leave the first.
			rocksdb_js::FlushCaptureHold second(gate);
			// Past the first hold's deadline, so only a fresh one keeps it parked.
			std::this_thread::sleep_for(std::chrono::milliseconds(250));
			EXPECT_EQ(second.parkedFlushes(), 1u);
			EXPECT_FALSE(second.flushEscaped());
			EXPECT_FALSE(passed.load());
		}
	}
	flush.join();
	EXPECT_TRUE(passed.load());
}

TEST(FlushCaptureGate, OnceAFlushEscapesNoOtherParksUnderThatHold) {
	rocksdb_js::FlushCaptureGate gate(std::chrono::milliseconds(200));
	rocksdb_js::FlushCaptureHold hold(gate);
	std::thread waiting([&] { gate.enterFlushBegin(); });
	EXPECT_TRUE(waitUntil([&] { return hold.parkedFlushes() == 1; }));
	std::thread escaping([&] { gate.enterFlushBegin(); });

	const auto start = std::chrono::steady_clock::now();
	escaping.join();
	waiting.join();
	for (int i = 0; i < 3; i++) {
		gate.enterFlushBegin();
	}
	EXPECT_TRUE(hold.flushEscaped());
	EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(390));
}
