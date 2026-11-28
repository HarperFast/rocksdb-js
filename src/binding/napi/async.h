#ifndef __NAPI_ASYNC_H__
#define __NAPI_ASYNC_H__

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <new>
#include "core/assert.h"
#include "core/debug.h"
#include "core/test_seam.h"
#include "napi/binding.h"
#include "napi/status_macros.h"
#include "napi/macros.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"

namespace rocksdb_js {

// Every state is freed from its complete callback or before its work was queued, so a work item
// still set at destruction has no callback pending: napi_delete_async_work() failed and it leaks.
// Not a template, so one report covers every BaseAsyncState instantiation.
inline void checkAsyncWorkDeleted(bool deleted) noexcept {
	ROCKSDB_JS_ASSERT(deleted, "Async work was not deleted before destructor");
}

template <typename T>
struct BaseAsyncState {
	napi_env env;
	T handle;
	napi_async_work asyncWork;
	napi_ref resolveRef;
	napi_ref rejectRef;
	rocksdb::Status status;

	std::atomic<bool> completed{ false };

	BaseAsyncState(
		napi_env env,
		T handle
	) :
		env(env),
		handle(handle),
		asyncWork(nullptr),
		resolveRef(nullptr),
		rejectRef(nullptr) {}

	static void* operator new(std::size_t size) {
		if (consumeAsyncExceptionForTesting(AsyncExceptionStage::State)) {
			throw std::bad_alloc();
		}
		return ::operator new(size);
	}

	static void operator delete(void* pointer) noexcept {
		::operator delete(pointer);
	}

	virtual ~BaseAsyncState() {
		this->resolveRef = nullptr;
		this->rejectRef = nullptr;

		this->signalExecuteCompleted();

		checkAsyncWorkDeleted(this->asyncWork == nullptr);
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

		napi_value global;
		NAPI_STATUS_THROWS_ERROR_VOID(::napi_get_global(this->env, &global), "Failed to get global object");

		napi_value resolve;
		DEBUG_LOG("%p BaseAsyncState::callResolve Getting resolve from reference...\n", this);
		NAPI_STATUS_THROWS_ERROR_VOID(::napi_get_reference_value(this->env, this->resolveRef, &resolve), "Failed to get reference to resolve function");

		DEBUG_LOG("%p BaseAsyncState::callResolve Calling resolve function...\n", this);
		NAPI_STATUS_THROWS_ERROR_VOID(::napi_call_function(this->env, global, resolve, result ? 1 : 0, result ? &result : nullptr, nullptr), "Failed to call resolve function");
		DEBUG_LOG("%p BaseAsyncState::callResolve Resolve function completed successfully\n", this);

		this->deleteCallbackRefs();
	}

	void callReject(napi_value error) {
		if (this->rejectRef == nullptr) {
			DEBUG_LOG("%p BaseAsyncState::callReject rejectRef is null\n", this);
			return;
		}

		napi_value global;
		NAPI_STATUS_THROWS_VOID(::napi_get_global(this->env, &global));

		napi_value reject;
		DEBUG_LOG("%p BaseAsyncState::callReject Getting reject from reference...\n", this);
		NAPI_STATUS_THROWS_ERROR_VOID(::napi_get_reference_value(this->env, this->rejectRef, &reject), "Failed to get reference to reject function");

		DEBUG_LOG("%p BaseAsyncState::callReject Calling reject function...\n", this);
		NAPI_STATUS_THROWS_ERROR_VOID(::napi_call_function(this->env, global, reject, 1, &error, nullptr), "Failed to call reject function");
		DEBUG_LOG("%p BaseAsyncState::callReject Reject function completed successfully\n", this);

		this->deleteCallbackRefs();
	}

	void deleteCallbackRefs() noexcept {
		if (this->resolveRef != nullptr) {
			::napi_delete_reference(this->env, this->resolveRef);
			this->resolveRef = nullptr;
		}
		if (this->rejectRef != nullptr) {
			::napi_delete_reference(this->env, this->rejectRef);
			this->rejectRef = nullptr;
		}
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

constexpr const char* OUT_OF_MEMORY_CODE = "ERR_OUT_OF_MEMORY";

/**
 * Wraps an async N-API entry point: N-API calls it from C, so a C++ exception that escaped it
 * would terminate the process. Its JS caller runs it inside a `new Promise` executor, so the
 * thrown JS error rejects that promise. Only wrap entry points whose setup releases what it took
 * (registrations, in-flight claims, state) by RAII, since the exception unwinds through it.
 */
template <napi_callback Entry>
napi_value guardAsyncEntry(napi_env env, napi_callback_info info) noexcept {
	try {
		return Entry(env, info);
	} catch (const std::bad_alloc&) {
		::napi_throw_error(env, OUT_OF_MEMORY_CODE, "Out of memory");
	} catch (const std::exception& e) {
		::napi_throw_error(env, nullptr, e.what());
	} catch (...) {
		::napi_throw_error(env, nullptr, "Unexpected native error");
	}
	return nullptr;
}

/**
 * Runs the work of an async execute callback on its libuv thread, turning an exception into a
 * message-free `state->status` (building a message could throw again) for the completion to reject
 * with. Steps that must run whether or not the work threw — releasing pins and claims,
 * `signalExecuteCompleted()` — belong after this call.
 */
template <typename State, typename Work>
void runAsyncExecute(State* state, Work&& work) noexcept {
	try {
		if (consumeAsyncExceptionForTesting(AsyncExceptionStage::Execute)) {
			throw std::bad_alloc();
		}
		work();
	} catch (const std::bad_alloc&) {
		state->status = rocksdb::Status::MemoryLimit();
	} catch (...) {
		state->status = rocksdb::Status::Aborted();
	}
}

/**
 * Rejects `state`'s promise using N-API calls alone, without the throwing status macros, so it can
 * run inside a `noexcept` handler. A pending JS exception is taken as the error, since N-API calls
 * fail while one is pending.
 */
template <typename State>
void rejectAsyncState(napi_env env, State* state, const char* code, const char* message) noexcept {
	if (state->rejectRef == nullptr) {
		return;
	}
	napi_value error = nullptr;
	bool pending = false;
	if (::napi_is_exception_pending(env, &pending) != napi_ok || !pending) {
		napi_value codeValue = nullptr;
		napi_value messageValue;
		if (code != nullptr && ::napi_create_string_utf8(env, code, NAPI_AUTO_LENGTH, &codeValue) != napi_ok) {
			codeValue = nullptr;
		}
		if (::napi_create_string_utf8(env, message, NAPI_AUTO_LENGTH, &messageValue) != napi_ok ||
			::napi_create_error(env, codeValue, messageValue, &error) != napi_ok) {
			error = nullptr;
		}
	}
	if (error == nullptr && ::napi_get_and_clear_last_exception(env, &error) != napi_ok) {
		::napi_get_undefined(env, &error);
	}
	napi_value global;
	napi_value reject;
	// Without a call the references are kept rather than dropped, so nothing claims the promise
	// settled.
	if (error != nullptr && ::napi_get_global(env, &global) == napi_ok &&
		::napi_get_reference_value(env, state->rejectRef, &reject) == napi_ok &&
		::napi_call_function(env, global, reject, 1, &error, nullptr) == napi_ok) {
		state->deleteCallbackRefs();
	}
}

/**
 * Runs the settling part of an async complete callback. When it throws, or returns without
 * settling (a status macro bailed out), the promise is rejected instead. Releasing the state belongs
 * after this call.
 */
template <typename State, typename Settle>
void runAsyncComplete(napi_env env, State* state, Settle&& settle) noexcept {
	try {
		if (consumeAsyncExceptionForTesting(AsyncExceptionStage::Complete)) {
			throw std::bad_alloc();
		}
		settle();
	} catch (const std::bad_alloc&) {
		rejectAsyncState(env, state, OUT_OF_MEMORY_CODE, "Out of memory");
		return;
	} catch (...) {
		rejectAsyncState(env, state, nullptr, "Unexpected native error");
		return;
	}
	rejectAsyncState(env, state, nullptr, "Failed to settle the operation");
}

struct AsyncWorkHandle {
	std::atomic<bool> cancelled{ false };
	std::atomic<int32_t> activeAsyncWorkCount{ 0 };
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

	/**
	 * Admits a synchronous call without taking `waitMutex`. The count is published before
	 * `cancelled` is read, and cancellation is published before the drain reads the count (both
	 * seq_cst), so either the drain waits for this call or the call sees the cancellation. Only for
	 * the owning thread of a handle whose wrapper is live: a refused call's transient count can make
	 * `cancelIfIdle()`, an orphan close or `closeDeferredAbort()` see work in flight, and none of them
	 * can run alongside it then.
	 */
	[[nodiscard]] bool enterSync() {
		this->activeAsyncWorkCount.fetch_add(1);
		if (this->cancelled.load()) {
			this->exitSync();
			return false;
		}
		return true;
	}

	void exitSync() {
		if (this->activeAsyncWorkCount.fetch_sub(1) == 1 && this->cancelled.load()) {
			{
				// Under the mutex, so a drain between its predicate check and its wait cannot miss this.
				std::lock_guard<std::mutex> lock(this->waitMutex);
				this->asyncWorkComplete.notify_all();
			}
			signalCancelledIdle();
		}
	}

	void unregisterAsyncWork() {
		bool cancelledIdle = false;
		{
			std::lock_guard<std::mutex> lock(this->waitMutex);
			auto activeAsyncWorkCount = --this->activeAsyncWorkCount;
			if (activeAsyncWorkCount > 0) {
				DEBUG_LOG("%p AsyncWorkHandle::unregisterAsyncWork Still have %u active async work tasks\n", this, activeAsyncWorkCount);
			} else if (activeAsyncWorkCount == 0) {
				DEBUG_LOG("%p AsyncWorkHandle::unregisterAsyncWork All async work has completed, notifying\n", this);
				this->asyncWorkComplete.notify_all();
				// Every writer of `cancelled` holds `waitMutex`; the decrement above orders the signal.
				cancelledIdle = this->cancelled.load(std::memory_order_relaxed);
			}
		}
		if (cancelledIdle) {
			signalCancelledIdle();
		}
	}

	/**
	 * Lets one thread wait until any cancelled handle's admitted work drains to zero, without polling
	 * each handle. Register before checking the handles, then wait on `cancelledIdleGeneration` as
	 * read before that check: a handle that drains after the check sees the registration and bumps
	 * the generation. The registration, snapshot, scan, decrement and bump are seq_cst; the
	 * `cancelled` read between them may be relaxed where `waitMutex` is held.
	 */
	struct CancelledIdleWaiter final {
		CancelledIdleWaiter() { cancelledIdleWaiters.fetch_add(1); }
		~CancelledIdleWaiter() { cancelledIdleWaiters.fetch_sub(1); }
		CancelledIdleWaiter(const CancelledIdleWaiter&) = delete;
		CancelledIdleWaiter& operator=(const CancelledIdleWaiter&) = delete;
	};

	inline static std::atomic<uint32_t> cancelledIdleWaiters{ 0 };
	inline static std::atomic<uint32_t> cancelledIdleGeneration{ 0 };

	static void signalCancelledIdle() {
		if (cancelledIdleWaiters.load() != 0) {
			cancelledIdleGeneration.fetch_add(1);
			cancelledIdleGeneration.notify_all();
		}
	}

	void cancelAllAsyncWork() {
		std::lock_guard<std::mutex> lock(this->waitMutex);
		this->cancelled.store(true);
	}

	enum class IdleCancel {
		Cancelled,
		AlreadyCancelled,
		Busy
	};

	/**
	 * Publishes cancellation only when no work is admitted. Deciding both under
	 * `waitMutex` means no registration can land between the idle check and the
	 * cancellation, from this thread or any other.
	 */
	IdleCancel cancelIfIdle() {
		std::lock_guard<std::mutex> lock(this->waitMutex);
		if (this->cancelled.load()) {
			return IdleCancel::AlreadyCancelled;
		}
		if (this->activeAsyncWorkCount.load() > 0) {
			return IdleCancel::Busy;
		}
		this->cancelled.store(true);
		return IdleCancel::Cancelled;
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
 * Holds one async-work admission on `handle` until it is released to its new
 * owner or goes out of scope. Transaction reads use it to keep a target
 * DBHandle open while they pin its column descriptor; async setup uses it so a
 * failure, including an exception, before the state takes the admission over
 * does not strand it.
 *
 * Admission can be refused if `handle`'s cancellation was already published
 * (a concurrent close); `admitted` tracks that so the destructor only
 * unregisters a claim it actually holds — unregistering an admission that
 * never succeeded would underflow the target's active-work count and could
 * make a later close() stop waiting while real work is still outstanding.
 * Callers must check `ok()` after construction and treat a false result the
 * same as the target already being closed.
 */
struct ScopedAsyncWorkRegistration {
	AsyncWorkHandle* handle;
	bool admitted;

	explicit ScopedAsyncWorkRegistration(AsyncWorkHandle* handle) : handle(handle), admitted(false) {
		if (this->handle) {
			this->admitted = this->handle->registerAsyncWork();
		}
	}

	~ScopedAsyncWorkRegistration() {
		if (this->handle && this->admitted) {
			this->handle->unregisterAsyncWork();
		}
	}

	ScopedAsyncWorkRegistration(const ScopedAsyncWorkRegistration&) = delete;
	ScopedAsyncWorkRegistration& operator=(const ScopedAsyncWorkRegistration&) = delete;

	bool ok() const {
		return !this->handle || this->admitted;
	}

	void release() {
		this->handle = nullptr;
	}
};

/**
 * Owns a not-yet-queued `state` during N-API setup: an early return deletes it with its promise
 * references and async work. Set `state->completed` while this owns a state that holds no async
 * work registration, so the destructor does not release one; `release()` once the work is queued.
 */
template <typename State>
struct PendingAsyncState {
	napi_env env;
	State* state;

	PendingAsyncState(napi_env env, State* state) : env(env), state(state) {}

	~PendingAsyncState() {
		if (!this->state) return;
		if (this->state->resolveRef) {
			::napi_delete_reference(this->env, this->state->resolveRef);
			this->state->resolveRef = nullptr;
		}
		if (this->state->rejectRef) {
			::napi_delete_reference(this->env, this->state->rejectRef);
			this->state->rejectRef = nullptr;
		}
		this->state->deleteAsyncWork();
		delete this->state;
	}

	void release() {
		this->state = nullptr;
	}

	PendingAsyncState(const PendingAsyncState&) = delete;
	PendingAsyncState& operator=(const PendingAsyncState&) = delete;
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
template <typename State>
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

	rejectAsyncState(env, state, nullptr, message);

	delete state;
	return false;
}

/**
 * Queues `state`'s async work after a successful `admitAsyncWorkOrReject()`.
 * On success, returns true and the caller returns its normal pending value.
 * On the rare `napi_queue_async_work()` failure, the admission above already
 * incremented the target's active-work count with nothing left to run it
 * down — a bare `NAPI_STATUS_THROWS` here would return without releasing
 * that claim, leaking `state` and leaving the count permanently off by one.
 * Since a bounded `waitForAsyncWorkCompletion()` no longer exists to paper
 * over a stuck count (see the note on that method), a single stranded claim
 * blocks that handle's close forever, which in turn blocks every later
 * `OpenDB()` for its path. This releases the claim via
 * `signalExecuteCompleted()` (unlike `admitAsyncWorkOrReject()`'s refusal
 * path, this one actually decrements — the claim was real), deletes the
 * async work object and `state`, rejects the promise with `message`, and
 * returns false.
 *
 * `admitted` must be false when the caller skipped `admitAsyncWorkOrReject()`
 * for this `state` (e.g. backup.cpp's `queueBackupWork(..., registerWork)`
 * with `registerWork` false) — otherwise `signalExecuteCompleted()` would
 * unregister a claim that was never taken, underflowing the count.
 */
template <typename State>
bool queueAsyncWorkOrReject(napi_env env, State* state, const char* message, bool admitted = true) {
	if (::napi_queue_async_work(env, state->asyncWork) == napi_ok) {
		return true;
	}

	if (admitted) {
		state->signalExecuteCompleted();
	} else {
		state->completed.store(true);
	}

	if (state->asyncWork) {
		if (::napi_delete_async_work(env, state->asyncWork) == napi_ok) {
			state->asyncWork = nullptr;
		}
	}

	rejectAsyncState(env, state, nullptr, message);

	delete state;
	return false;
}

} // namespace rocksdb_js

#endif
