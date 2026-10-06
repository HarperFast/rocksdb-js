#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>
#include "database/commit_worker.h"

namespace {

// Each task waits until `parties` tasks are running at once, so the burst only completes if
// the worker runs that many concurrently.
struct Rendezvous {
	std::mutex mutex;
	std::condition_variable arrived;
	unsigned count = 0;

	bool arriveAndWait(unsigned parties) {
		std::unique_lock<std::mutex> lock(this->mutex);
		++this->count;
		this->arrived.notify_all();
		return this->arrived.wait_for(lock, std::chrono::seconds(5), [&]() { return this->count >= parties; });
	}
};

void waitFor(std::atomic<unsigned>& counter, unsigned target) {
	for (int i = 0; i < 5000 && counter.load() < target; i++) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
}

} // namespace

TEST(CommitWorker, BurstGrowsPastAnIdleThreadStillWaking) {
	for (int round = 0; round < 20; round++) {
		rocksdb_js::CommitWorker worker("test-commit", 4);
		std::atomic<unsigned> done{ 0 };
		worker.enqueue([&]() { done++; });
		waitFor(done, 1);
		// One started thread is now idle. Every task of the burst is queued before it can wake.
		Rendezvous rendezvous;
		std::atomic<unsigned> met{ 0 };
		for (int i = 0; i < 4; i++) {
			worker.enqueue([&]() {
				if (rendezvous.arriveAndWait(4)) {
					met++;
				}
				done++;
			});
		}
		waitFor(done, 5);
		EXPECT_EQ(met.load(), 4u) << "round " << round;
		EXPECT_EQ(worker.threadCount(), 4u);
		worker.shutdown();
	}
}

TEST(CommitWorker, SingleThreadRunsTasksInOrder) {
	rocksdb_js::CommitWorker worker("test-commit", 1);
	std::mutex mutex;
	std::vector<int> order;
	for (int i = 0; i < 100; i++) {
		worker.enqueue([&, i]() {
			std::lock_guard<std::mutex> lock(mutex);
			order.push_back(i);
		});
	}
	worker.shutdown();
	ASSERT_EQ(order.size(), 100u);
	for (int i = 0; i < 100; i++) {
		EXPECT_EQ(order[i], i);
	}
	EXPECT_EQ(worker.threadCount(), 0u);
}

TEST(CommitWorker, ShutdownDrainsQueuedTasksAndLaterTasksRunInline) {
	rocksdb_js::CommitWorker worker("test-commit", 3);
	std::atomic<unsigned> done{ 0 };
	for (int i = 0; i < 50; i++) {
		worker.enqueue([&]() {
			std::this_thread::sleep_for(std::chrono::microseconds(100));
			done++;
		});
	}
	worker.shutdown();
	EXPECT_EQ(done.load(), 50u);
	const auto caller = std::this_thread::get_id();
	std::thread::id ranOn;
	worker.enqueue([&]() { ranOn = std::this_thread::get_id(); });
	EXPECT_EQ(ranOn, caller);
}
