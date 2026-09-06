#include "core/operation_gate.h"
#include "gtest/gtest.h"
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

namespace rocksdb_js {
namespace {

std::mutex hookMutex;
std::condition_variable hookCv;
bool hookEntered = false;
bool hookReleased = false;

void pauseBeforeNotify() {
	std::unique_lock lock(hookMutex);
	hookEntered = true;
	hookCv.notify_all();
	hookCv.wait(lock, [] { return hookReleased; });
}

class OperationGateTest : public ::testing::Test {
protected:
	void SetUp() override {
		std::lock_guard lock(hookMutex);
		hookEntered = false;
		hookReleased = false;
		OperationGate::setBeforeNotifyHookForTest(nullptr);
	}

	void TearDown() override {
		{
			std::lock_guard lock(hookMutex);
			hookReleased = true;
		}
		hookCv.notify_all();
		OperationGate::setBeforeNotifyHookForTest(nullptr);
	}
};

TEST_F(OperationGateTest, ClaimTracksActiveOperations) {
	OperationGate gate;
	EXPECT_EQ(gate.activeCount(), 0u);
	{
		auto claim = OperationClaim::acquireBorrowed(gate);
		ASSERT_TRUE(claim);
		EXPECT_EQ(gate.activeCount(), 1u);
	}
	EXPECT_EQ(gate.activeCount(), 0u);
}

TEST_F(OperationGateTest, ClosingRejectsNewClaims) {
	OperationGate gate;
	EXPECT_TRUE(gate.beginClose());
	EXPECT_FALSE(gate.beginClose());
	EXPECT_FALSE(OperationClaim::acquireBorrowed(gate));
	EXPECT_EQ(gate.activeCount(), 0u);
}

TEST_F(OperationGateTest, CloseWaitsForExistingClaim) {
	OperationGate gate;
	auto claim = OperationClaim::acquireBorrowed(gate);
	ASSERT_TRUE(claim);
	ASSERT_TRUE(gate.beginClose());

	std::atomic<bool> drained = false;
	std::thread waiter([&] {
		gate.waitForDrain();
		drained = true;
	});

	EXPECT_FALSE(drained.load());
	claim = {};
	waiter.join();
	EXPECT_TRUE(drained.load());
}

TEST_F(OperationGateTest, WaitCannotMissFinalReleaseNotification) {
	OperationGate gate;
	auto claim = OperationClaim::acquireBorrowed(gate);
	ASSERT_TRUE(claim);
	ASSERT_TRUE(gate.beginClose());
	OperationGate::setBeforeNotifyHookForTest(pauseBeforeNotify);

	std::atomic<bool> drained = false;
	std::thread waiter([&] {
		gate.waitForDrain();
		drained = true;
	});
	std::thread releaser([&] { claim = {}; });

	{
		std::unique_lock lock(hookMutex);
		hookCv.wait(lock, [] { return hookEntered; });
	}
	EXPECT_EQ(gate.activeCount(), 0u);
	{
		std::lock_guard lock(hookMutex);
		hookReleased = true;
	}
	hookCv.notify_all();

	releaser.join();
	waiter.join();
	EXPECT_TRUE(drained.load());
}

TEST_F(OperationGateTest, SharedClaimOwnsGateThroughFinalRelease) {
	auto gate = std::make_shared<OperationGate>();
	std::weak_ptr<OperationGate> weakGate = gate;
	auto claim = OperationClaim::acquireShared(gate);
	ASSERT_TRUE(claim);
	gate.reset();
	EXPECT_FALSE(weakGate.expired());
	claim = {};
	EXPECT_TRUE(weakGate.expired());
}

} // namespace
} // namespace rocksdb_js
