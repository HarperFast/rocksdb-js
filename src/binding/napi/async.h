#ifndef __NAPI_ASYNC_H__
#define __NAPI_ASYNC_H__

#include <cassert>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include "core/debug.h"
#include "napi/binding.h"
#include "napi/status_macros.h"
#include "napi/macros.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"

namespace rocksdb_js {

template<typename T>
struct BaseAsyncState {
	napi_env env;
	T handle;
	napi_async_work asyncWork;
	napi_ref resolveRef;
	napi_ref rejectRef;
	rocksdb::Status status;

	std::atomic<bool> completed{false};

	BaseAsyncState(
		napi_env env,
		T handle
	) :
		env(env),
		handle(handle),
		asyncWork(nullptr),
		resolveRef(nullptr),
		rejectRef(nullptr) {}

	virtual ~BaseAsyncState() {
		this->resolveRef = nullptr;
		this->rejectRef = nullptr;

		this->signalExecuteCompleted();

		assert(this->asyncWork == nullptr && "Async work was not deleted before destructor!");
	}

	void deleteAsyncWork() {
		if (this->asyncWork != nullptr) {
			DEBUG_LOG("%p BaseAsyncState::~BaseAsyncState Deleting async work %p\n", this, this->asyncWork);
			napi_status status = ::napi_delete_async_work(this->env, this->asyncWork);
			if (status == napi_ok) {
				DEBUG_LOG("%p BaseAsyncState::~BaseAsyncState Successfully deleted async work\n", this);
				this->asyncWork = nullptr;
			} else {
				DEBUG_LOG("%p BaseAsyncState::~BaseAsyncState Failed to delete async work (status=%d)\n", this, status);
			}
		} else {
			DEBUG_LOG("%p BaseAsyncState::~BaseAsyncState Async work was already null!\n", this);
		}
	}

	void callResolve(napi_value result = nullptr) {
		if (this->resolveRef == nullptr) {
			DEBUG_LOG("%p BaseAsyncState::callResolve resolveRef is null\n", this);
			return;
		}

		if (this->rejectRef != nullptr) {
			DEBUG_LOG("%p BaseAsyncState::callResolve Deleting usused reject reference\n", this);
			NAPI_STATUS_THROWS_ERROR_VOID(::napi_delete_reference(this->env, this->rejectRef), "Failed to delete reference to reject function");
			DEBUG_LOG("%p BaseAsyncState::callResolve Reject reference deleted successfully\n", this);
			this->rejectRef = nullptr;
		}

		napi_value global;
		NAPI_STATUS_THROWS_ERROR_VOID(::napi_get_global(this->env, &global), "Failed to get global object");

		napi_value resolve;
		DEBUG_LOG("%p BaseAsyncState::callResolve Getting resolve from reference...\n", this);
		NAPI_STATUS_THROWS_ERROR_VOID(::napi_get_reference_value(this->env, this->resolveRef, &resolve), "Failed to get reference to resolve function");

		DEBUG_LOG("%p BaseAsyncState::callResolve Calling resolve function...\n", this);
		NAPI_STATUS_THROWS_ERROR_VOID(::napi_call_function(this->env, global, resolve, result ? 1 : 0, result ? &result : nullptr, nullptr), "Failed to call resolve function");
		DEBUG_LOG("%p BaseAsyncState::callResolve Resolve function completed successfully\n", this);

		DEBUG_LOG("%p BaseAsyncState::callResolve Deleting resolve reference\n", this);
		NAPI_STATUS_THROWS_ERROR_VOID(::napi_delete_reference(this->env, this->resolveRef), "Failed to delete reference to resolve function");
		DEBUG_LOG("%p BaseAsyncState::callResolve Resolve reference deleted successfully\n", this);
		this->resolveRef = nullptr;
	}

	void callReject(napi_value error) {
		if (this->rejectRef == nullptr) {
			DEBUG_LOG("%p BaseAsyncState::callReject rejectRef is null\n", this);
			return;
		}

		if (this->resolveRef != nullptr) {
			DEBUG_LOG("%p BaseAsyncState::callReject Deleting usused resolve reference\n", this);
			NAPI_STATUS_THROWS_ERROR_VOID(::napi_delete_reference(this->env, this->resolveRef), "Failed to delete reference to resolve function");
			DEBUG_LOG("%p BaseAsyncState::callReject Resolve reference deleted successfully\n", this);
			this->resolveRef = nullptr;
		}

		napi_value global;
		NAPI_STATUS_THROWS_VOID(::napi_get_global(this->env, &global));

		napi_value reject;
		DEBUG_LOG("%p BaseAsyncState::callReject Getting reject from reference...\n", this);
		NAPI_STATUS_THROWS_ERROR_VOID(::napi_get_reference_value(this->env, this->rejectRef, &reject), "Failed to get reference to reject function");

		DEBUG_LOG("%p BaseAsyncState::callReject Calling reject function...\n", this);
		NAPI_STATUS_THROWS_ERROR_VOID(::napi_call_function(this->env, global, reject, 1, &error, nullptr), "Failed to call reject function");
		DEBUG_LOG("%p BaseAsyncState::callReject Reject function completed successfully\n", this);

		DEBUG_LOG("%p BaseAsyncState::callReject Deleting reject reference\n", this);
		NAPI_STATUS_THROWS_ERROR_VOID(::napi_delete_reference(this->env, this->rejectRef), "Failed to delete reference to reject function");
		DEBUG_LOG("%p BaseAsyncState::callReject Reject reference deleted successfully\n", this);
		this->rejectRef = nullptr;
	}

	void signalExecuteCompleted() {
		if (!this->completed.load()) {
			DEBUG_LOG("%p BaseAsyncState::signalExecuteCompleted Unregistering async work\n", this);
			if (this->handle) {
				this->handle->unregisterAsyncWork();
			} else {
				DEBUG_LOG("%p BaseAsyncState::signalExecuteCompleted Handle is null\n", this);
			}
			this->completed.store(true);
		} else {
			DEBUG_LOG("%p BaseAsyncState::signalExecuteCompleted Execute already completed\n", this);
		}
	}
};

struct AsyncWorkHandle {
	std::atomic<bool> cancelled{false};
	std::atomic<int32_t> activeAsyncWorkCount{0};
	std::mutex waitMutex;
	std::condition_variable asyncWorkComplete;

	void registerAsyncWork() {
		++this->activeAsyncWorkCount;
	}

	void unregisterAsyncWork() {
		auto activeAsyncWorkCount = --this->activeAsyncWorkCount;
		if (activeAsyncWorkCount > 0) {
			DEBUG_LOG("%p AsyncWorkHandle::unregisterAsyncWork Still have %u active async work tasks\n", this, activeAsyncWorkCount);
		} else if (activeAsyncWorkCount == 0) {
			DEBUG_LOG("%p AsyncWorkHandle::unregisterAsyncWork All async work has completed, notifying\n", this);
			this->asyncWorkComplete.notify_one();
		}
	}

	void cancelAllAsyncWork() {
		this->cancelled.store(true);
	}

	void waitForAsyncWorkCompletion(
		std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)
	) {
		auto start = std::chrono::steady_clock::now();
		const auto pollInterval = std::chrono::milliseconds(10);
		std::unique_lock<std::mutex> lock(this->waitMutex);
		auto activeAsyncWorkCount = this->activeAsyncWorkCount.load();

		if (activeAsyncWorkCount == 0) {
			DEBUG_LOG("%p AsyncWorkHandle::waitForAsyncWorkCompletion no async work to wait for\n", this);
			return;
		}

		while (activeAsyncWorkCount > 0) {
			auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
			if (elapsed >= timeout) {
				DEBUG_LOG("%p AsyncWorkHandle::waitForAsyncWorkCompletion timeout waiting for async work completion, %u items remaining\n", this, activeAsyncWorkCount);
				return;
			}

			auto remainingTime = timeout - elapsed;
			auto waitTime = std::min(pollInterval, remainingTime);

			DEBUG_LOG("%p AsyncWorkHandle::waitForAsyncWorkCompletion waiting for %u active work items\n", this, activeAsyncWorkCount);

			bool completed = this->asyncWorkComplete.wait_for(lock, waitTime, [this] {
				return this->activeAsyncWorkCount.load() == 0;
			});

			if (completed) {
				DEBUG_LOG("%p AsyncWorkHandle::waitForAsyncWorkCompletion all async work execution completed\n", this);
				return;
			}
		}
	}

	bool isCancelled() const {
		return this->cancelled.load();
	}

	void resetCancelled() {
		this->cancelled.store(false);
	}
};

} // namespace rocksdb_js

#endif
