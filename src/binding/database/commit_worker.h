#ifndef __COMMIT_WORKER_H__
#define __COMMIT_WORKER_H__

#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>
#include "core/debug.h"
#include "core/platform.h"

namespace rocksdb_js {

/**
 * Dedicated worker threads with a shared task queue, used for the
 * per-database commit pipeline. Async transaction commits execute on these
 * instead of the libuv threadpool so that slow commits (write stalls, large
 * batches, transaction-log contention) cannot occupy libuv slots and starve
 * unrelated async work (fs, dns, crypto, async gets). Completion is marshalled
 * back to the calling env via a threadsafe function.
 *
 * Each database has a commit worker and a transaction-log lane (see
 * commitThreadMode() in transaction.cpp). A worker limited to one thread is a
 * lane: it runs tasks in dispatch order. With more threads, tasks run and
 * finish in no particular order. Concurrent commits cannot deadlock:
 * pessimistic locks are acquired at put time, and optimistic parallel
 * validation takes its database's commit lock buckets in sorted order and
 * releases them when the RocksDB write returns.
 *
 * A thread starts only when queued tasks outnumber idle threads, up to
 * `maxThreads`, so a database that never has concurrent commits owns one.
 * Shutdown drains every queued task and joins every thread.
 */
struct CommitWorker final {
	const char* threadName;
	std::mutex mutex;
	std::condition_variable cv;
	std::deque<std::function<void()>> queue;
	std::vector<std::thread> threads;
	const unsigned maxThreads;
	unsigned idleThreads = 0;
	bool stopped = false;

	explicit CommitWorker(const char* threadName, unsigned maxThreads = 1)
		: threadName(threadName), maxThreads(maxThreads < 1 ? 1 : maxThreads) {}

	~CommitWorker() {
		this->shutdown();
	}

	/**
	 * Enqueues a task. If the worker has already been shut down (descriptor
	 * closing), or no thread exists and none can be started, the task runs
	 * inline on the calling thread; a commit will fail fast on the closing
	 * checks.
	 */
	void enqueue(std::function<void()> task) {
		bool runInline = false;
		bool wake = false;
		{
			std::lock_guard<std::mutex> lock(this->mutex);
			if (this->stopped) {
				runInline = true;
			} else {
				this->queue.push_back(std::move(task));
				wake = this->idleThreads > 0;
				// A woken thread counts as idle until it retakes the mutex, so
				// compare queued work with idle threads rather than testing for
				// an idle thread: otherwise a burst enqueued before the wakeup
				// lands is drained by that one thread.
				if (this->queue.size() > this->idleThreads && this->threads.size() < this->maxThreads) {
					try {
						this->threads.emplace_back([this]() { this->run(); });
					} catch (const std::exception& e) {
						DEBUG_LOG("%p CommitWorker::enqueue Failed to start a thread: %s\n", this, e.what());
						if (this->threads.empty()) {
							task = std::move(this->queue.back());
							this->queue.pop_back();
							runInline = true;
						}
					}
				}
			}
		}
		if (runInline) {
			DEBUG_LOG("%p CommitWorker::enqueue Running task inline\n", this);
			task();
		} else if (wake) {
			// Busy threads re-check the queue before they wait, so only an idle
			// thread needs a signal. Profiling showed a per-enqueue
			// pthread_cond_signal as a measurable JS-thread cost under load.
			this->cv.notify_one();
		}
	}

	/**
	 * Number of queued (not yet started) tasks. Diagnostic only — the value is
	 * stale the moment it is read.
	 */
	size_t depth() {
		std::lock_guard<std::mutex> lock(this->mutex);
		return this->queue.size();
	}

	/**
	 * Number of threads started so far. Diagnostic only.
	 */
	size_t threadCount() {
		std::lock_guard<std::mutex> lock(this->mutex);
		return this->threads.size();
	}

	/**
	 * Drains any remaining queued tasks and joins every thread. Idempotent;
	 * called from DBDescriptor::finishClose() and the destructor.
	 */
	void shutdown() {
		std::vector<std::thread> toJoin;
		{
			std::lock_guard<std::mutex> lock(this->mutex);
			this->stopped = true;
			toJoin.swap(this->threads);
		}
		this->cv.notify_all();
		for (auto& thread : toJoin) {
			if (thread.joinable()) {
				DEBUG_LOG("%p CommitWorker::shutdown Draining and joining worker thread\n", this);
				thread.join();
			}
		}
	}

private:
	void run() {
		setThreadName(this->threadName);
		std::unique_lock<std::mutex> lock(this->mutex);
		for (;;) {
			while (this->queue.empty() && !this->stopped) {
				++this->idleThreads;
				this->cv.wait(lock);
				--this->idleThreads;
			}
			if (this->queue.empty()) {
				// stopped and fully drained
				return;
			}
			if (this->maxThreads == 1) {
				// A lane drains the whole queue per wakeup instead of retaking the
				// mutex per task.
				std::deque<std::function<void()>> batch;
				batch.swap(this->queue);
				lock.unlock();
				for (auto& task : batch) {
					task();
				}
			} else {
				std::function<void()> task = std::move(this->queue.front());
				this->queue.pop_front();
				lock.unlock();
				task();
			}
			lock.lock();
		}
	}
};

} // namespace rocksdb_js

#endif
