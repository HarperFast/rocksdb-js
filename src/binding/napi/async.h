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

	/**
	 * Admits one unit of async work. Returns false, without incrementing the
	 * count, once cancellation has been published — the caller must not queue
	 * the async work or otherwise touch native state in that case. Admission
	 * and `cancelAllAsyncWork()` share `waitMutex` so the two can never
	 * interleave: either this call is fully visible to the wait below before
	 * cancellation publishes, or it is refused. Without that, a registration
	 * racing a close's cancel+wait could land after the wait already observed
	 * zero, letting new work run concurrently with (or after) the native
	 * state that close() goes on to release.
	 */
	[[nodiscard]] bool registerAsyncWork() {
		std::lock_guard<std::mutex> lock(this->waitMutex);
		if (this->cancelled.load()) {
			return false;
		}
		++this->activeAsyncWorkCount;
		return true;
	}

	void unregisterAsyncWork() {
		std::lock_guard<std::mutex> lock(this->waitMutex);
		auto activeAsyncWorkCount = --this->activeAsyncWorkCount;
		if (activeAsyncWorkCount > 0) {
			DEBUG_LOG("%p AsyncWorkHandle::unregisterAsyncWork Still have %u active async work tasks\n", this, activeAsyncWorkCount);
		} else if (activeAsyncWorkCount == 0) {
			DEBUG_LOG("%p AsyncWorkHandle::unregisterAsyncWork All async work has completed, notifying\n", this);
			this->asyncWorkComplete.notify_all();
		}
	}

	void cancelAllAsyncWork() {
		std::lock_guard<std::mutex> lock(this->waitMutex);
		this->cancelled.store(true);
	}

	/**
	 * Blocks until every admitted unit of async work has completed. This must
	 * not time out: the caller is about to release native state (a
	 * `rocksdb::DB`, a column family, a transaction) that admitted work may
	 * still be using. A flush legitimately waiting out a write stall (see
	 * AGENTS.md invariant 16) can run far longer than any fixed bound, and a
	 * bounded wait that gives up anyway turns into a use-after-free once the
	 * caller proceeds to tear down that state.
	 *
	 * Supersedes the bounded, bool-returning version this replaced (a 5s
	 * timeout with a "leak instead of free" fallback in
	 * `TransactionHandle::close()`) — that was a deliberate stopgap tracked as
	 * HarperFast/rocksdb-js#784, this unbounded wait *is* #784. Every caller
	 * can now assume the drain always completes; there is no timed-out case
	 * left to handle.
	 */
	void waitForAsyncWorkCompletion() {
		std::unique_lock<std::mutex> lock(this->waitMutex);
		if (this->activeAsyncWorkCount.load() == 0) {
			DEBUG_LOG("%p AsyncWorkHandle::waitForAsyncWorkCompletion no async work to wait for\n", this);
			return;
		}
		DEBUG_LOG("%p AsyncWorkHandle::waitForAsyncWorkCompletion waiting for active work items\n", this);
		this->asyncWorkComplete.wait(lock, [this] {
			return this->activeAsyncWorkCount.load() == 0;
		});
		DEBUG_LOG("%p AsyncWorkHandle::waitForAsyncWorkCompletion all async work execution completed\n", this);
	}

	bool isCancelled() const {
		return this->cancelled.load();
	}

	void resetCancelled() {
		std::lock_guard<std::mutex> lock(this->waitMutex);
		this->cancelled.store(false);
	}
};

/**
 * Admits `state`'s async work onto `handle`. On success, returns true and the
 * caller proceeds to `napi_queue_async_work()` as usual. On refusal
 * (cancellation already published by a concurrent close), tears down the
 * async work object created for `state` (if any) and the promise references
 * already captured on it, rejects the promise with `message`, deletes
 * `state`, and returns false — the caller must return to JS immediately
 * without dereferencing any native state `state` was set up to use.
 */
template<typename State>
bool admitAsyncWorkOrReject(napi_env env, AsyncWorkHandle* handle, State* state, const char* message) {
	if (handle->registerAsyncWork()) {
		return true;
	}

	// Nothing was incremented, so mark completed before the destructor's
	// signalExecuteCompleted() runs — otherwise it would call
	// unregisterAsyncWork() for a registration that never succeeded.
	state->completed.store(true);

	if (state->asyncWork) {
		if (::napi_delete_async_work(env, state->asyncWork) == napi_ok) {
			state->asyncWork = nullptr;
		}
	}

	napi_value error = nullptr;
	napi_value messageValue;
	if (::napi_create_string_utf8(env, message, NAPI_AUTO_LENGTH, &messageValue) == napi_ok) {
		::napi_create_error(env, nullptr, messageValue, &error);
	}
	if (error == nullptr) {
		::napi_get_undefined(env, &error);
	}
	state->callReject(error);

	delete state;
	return false;
}

} // namespace rocksdb_js

#endif
