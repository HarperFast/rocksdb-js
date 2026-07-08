#ifndef __COMMIT_WORKER_H__
#define __COMMIT_WORKER_H__

#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include "core/debug.h"
#include "core/platform.h"

namespace rocksdb_js {

/**
 * A dedicated worker thread with a task queue, used for the per-database
 * commit pipeline. Async transaction commits execute on these instead of the
 * libuv threadpool so that slow commits (write stalls, large batches,
 * transaction-log contention) cannot occupy libuv slots and starve unrelated
 * async work (fs, dns, crypto, async gets). Completion is marshalled back to
 * the calling env via a threadsafe function.
 *
 * Each database has a commit lane and, in two-lane mode, a transaction-log
 * lane that writes the log batch before forwarding to the commit lane (see
 * commitThreadMode() in transaction.cpp). Each lane preserves dispatch order —
 * which keeps per-database commit order stable and serializes the
 * transaction-log write mutex for free. RocksDB commits never block on one
 * another's locks (pessimistic locks are acquired at put time, optimistic
 * validation does not block), so a single commit lane cannot deadlock.
 *
 * The thread is started lazily on the first task and joined on shutdown after
 * draining any queued tasks. Each run-loop wakeup drains the entire queue
 * snapshot in one pass — the structural hook that later lets the log lane see
 * every pending batch and aggregate them into one writev.
 */
struct CommitWorker final {
	const char* threadName;
	std::mutex mutex;
	std::condition_variable cv;
	std::deque<std::function<void()>> queue;
	std::thread thread;
	bool started = false;
	bool stopped = false;

	explicit CommitWorker(const char* threadName) : threadName(threadName) {}

	~CommitWorker() {
		this->shutdown();
	}

	/**
	 * Enqueues a task, lazily starting the worker thread. If the worker has
	 * already been shut down (descriptor closing), the task runs inline on the
	 * calling thread; a commit will fail fast on the closing checks.
	 */
	void enqueue(std::function<void()> task) {
		bool runInline = false;
		bool wasEmpty = false;
		{
			std::lock_guard<std::mutex> lock(this->mutex);
			if (this->stopped) {
				runInline = true;
			} else {
				if (!this->started) {
					this->started = true;
					this->thread = std::thread([this]() { this->run(); });
				}
				wasEmpty = this->queue.empty();
				this->queue.push_back(std::move(task));
			}
		}
		if (runInline) {
			DEBUG_LOG("%p CommitWorker::enqueue Worker stopped, running task inline\n", this);
			task();
		} else if (wasEmpty) {
			// Only the empty->non-empty transition needs a wakeup: the worker
			// drains the whole queue per wakeup and its wait predicate re-checks
			// emptiness before blocking, so tasks appended to a non-empty queue
			// are picked up without a signal. Profiling showed the per-enqueue
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
	 * Drains any remaining queued tasks and joins the thread. Idempotent;
	 * called from DBDescriptor::finishClose() and the destructor.
	 */
	void shutdown() {
		std::thread toJoin;
		{
			std::lock_guard<std::mutex> lock(this->mutex);
			this->stopped = true;
			if (this->started) {
				toJoin = std::move(this->thread);
				this->started = false;
			}
		}
		this->cv.notify_all();
		if (toJoin.joinable()) {
			DEBUG_LOG("%p CommitWorker::shutdown Draining and joining worker thread\n", this);
			toJoin.join();
		}
	}

private:
	void run() {
		setThreadName(this->threadName);
		std::unique_lock<std::mutex> lock(this->mutex);
		for (;;) {
			this->cv.wait(lock, [this] { return this->stopped || !this->queue.empty(); });
			if (this->queue.empty()) {
				// stopped and fully drained
				return;
			}
			// Drain the whole queue in one pass so tasks run without retaking
			// the mutex per item (and so a future log-lane aggregation step can
			// see the full pending batch).
			std::deque<std::function<void()>> batch;
			batch.swap(this->queue);
			lock.unlock();
			for (auto& task : batch) {
				task();
			}
			lock.lock();
		}
	}
};

} // namespace rocksdb_js

#endif
