#ifndef __UTIL_H__
#define __UTIL_H__

#include <atomic>
#include <cstdint>
#include <mutex>
#include <node_api.h>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include "binding.h"
#include "macros.h"
#include "rocksdb/status.h"

/**
 * This file contains various napi helper functions.
 *
 * Note: These function must go in a header file because the compiler doesn't
 * know the data type sizes until link time.
 */

namespace rocksdb_js {

struct Closable {
	virtual ~Closable() = default;
	virtual void close() = 0;
};

void createRocksDBError(napi_env env, rocksdb::Status status, const char* msg, napi_value& error);

void debugLog(const char* msg, ...);

void debugLogNapiValue(napi_env env, napi_value value, uint16_t indent = 0, bool isObject = false);

napi_status getKeyFromProperty(
	napi_env env,
	napi_value obj,
	const char* prop,
	const char* errorMsg,
	const char*& keyStr,
	uint32_t& start,
	uint32_t& end
);

const char* getNapiBufferFromArg(
	napi_env env,
	napi_value arg,
	uint32_t& start,
	uint32_t& end,
	size_t& length,
	const char* errorMsg
);

std::string getNapiExtendedError(napi_env env, napi_status& status, const char* errorMsg = nullptr);

[[maybe_unused]] static napi_status getString(napi_env env, napi_value from, std::string& to) {
	napi_valuetype type;
	NAPI_STATUS_RETURN(::napi_typeof(env, from, &type));

	if (type == napi_string) {
		size_t length = 0;
		NAPI_STATUS_RETURN(::napi_get_value_string_utf8(env, from, nullptr, 0, &length));
		to.resize(length, '\0');
		NAPI_STATUS_RETURN(::napi_get_value_string_utf8(env, from, &to[0], length + 1, &length));
	} else {
		bool isBuffer;
		NAPI_STATUS_RETURN(::napi_is_buffer(env, from, &isBuffer));

		if (isBuffer) {
			char* buf = nullptr;
			size_t length = 0;
			NAPI_STATUS_RETURN(::napi_get_buffer_info(env, from, reinterpret_cast<void**>(&buf), &length));
			to.assign(buf, length);
		} else {
			return napi_invalid_arg;
		}
	}

	return napi_ok;
}

[[maybe_unused]] static napi_status getValue(napi_env env, napi_value value, bool& result) {
	return ::napi_get_value_bool(env, value, &result);
}

[[maybe_unused]] static napi_status getValue(napi_env env, napi_value value, int32_t& result) {
	return ::napi_get_value_int32(env, value, &result);
}

[[maybe_unused]] static napi_status getValue(napi_env env, napi_value value, uint32_t& result) {
	return ::napi_get_value_uint32(env, value, &result);
}

[[maybe_unused]] static napi_status getValue(napi_env env, napi_value value, int64_t& result) {
	return ::napi_get_value_int64(env, value, &result);
}

[[maybe_unused]] static napi_status getValue(napi_env env, napi_value value, uint64_t& result) {
	int64_t result2;
	NAPI_STATUS_RETURN(::napi_get_value_int64(env, value, &result2));
	result = static_cast<uint64_t>(result2);
	return napi_ok;
}

// Only define size_t overload when size_t is different from uint64_t
template<typename T = size_t>
[[maybe_unused]] static typename std::enable_if<!std::is_same<T, uint64_t>::value, napi_status>::type
getValue(napi_env env, napi_value value, size_t& result) {
	int64_t result2;
	NAPI_STATUS_RETURN(::napi_get_value_int64(env, value, &result2));
	result = static_cast<size_t>(result2);
	return napi_ok;
}

[[maybe_unused]] static napi_status getValue(napi_env env, napi_value value, std::string& result) {
	return getString(env, value, result);
}

template <typename T>
[[maybe_unused]] static napi_status getValue(napi_env env, napi_value value, std::optional<T>& result) {
	result = T{};
	return getValue(env, value, *result);
}

template <typename T>
[[maybe_unused]] static napi_status getProperty(
	napi_env env,
	napi_value obj,
	const char* prop,
	T& result,
	bool required = false
) {
	napi_valuetype objType;
	NAPI_STATUS_RETURN(::napi_typeof(env, obj, &objType));

	if (objType == napi_undefined || objType == napi_null) {
		return required ? napi_invalid_arg : napi_ok;
	}

	if (objType != napi_object) {
		return napi_invalid_arg;
	}

	bool has = false;
	NAPI_STATUS_RETURN(::napi_has_named_property(env, obj, prop, &has));

	if (!has) {
		return required ? napi_invalid_arg : napi_ok;
	}

	napi_value value;
	NAPI_STATUS_RETURN(::napi_get_named_property(env, obj, prop, &value));

	napi_valuetype valueType;
	NAPI_STATUS_RETURN(::napi_typeof(env, value, &valueType));

	if (valueType == napi_null || valueType == napi_undefined) {
		return required ? napi_invalid_arg : napi_ok;
	}

	return getValue(env, value, result);
}

/**
 * Base class for async state.
 */
template<typename T>
struct BaseAsyncState {
	BaseAsyncState(
		napi_env env,
		T handle
	) :
		env(env),
		handle(handle),
		asyncWork(nullptr),
		resolveRef(nullptr),
		rejectRef(nullptr),
		executeCompleted(false) {}

	virtual ~BaseAsyncState() {
		NAPI_STATUS_THROWS_VOID(::napi_delete_reference(env, resolveRef))
		NAPI_STATUS_THROWS_VOID(::napi_delete_reference(env, rejectRef))

		// unregister async work when state is destroyed
		if (asyncWork && handle) {
			handle->unregisterAsyncWork(asyncWork);
		}
	}

	// Call this from the execute handler to signal completion
	void signalExecuteCompleted() {
		if (handle) {
			handle->signalAsyncWorkExecuteCompleted(asyncWork);
			handle = nullptr;
		}
		executeCompleted = true;
	}

	napi_env env;
	T handle;
	napi_async_work asyncWork;
	napi_ref resolveRef;
	napi_ref rejectRef;
	rocksdb::Status status;
	std::atomic<bool> executeCompleted;
};

#define ASSERT_OPENED_AND_NOT_CANCELLED(handle, operation) \
	if (!handle || !handle->opened() || handle->isCancelled()) { \
		DEBUG_LOG("%p Database closed during %s operation\n", handle, operation) \
		return rocksdb::Status::Aborted("Database closed during " operation " operation"); \
	}

/**
 * Base class for managing async work tasks. All handles that are passed into
 * async work tasks should inherit from this class.
 *
 * @example
 * ```
 * struct MyHandle : AsyncWorkHandle {
 *     // ...
 * };
 *
 * struct MyAsyncState : BaseAsyncState<MyHandle> {
 *     MyAsyncState(napi_env env, MyHandle handle) : BaseAsyncState(env, handle) {}
 *     // ...
 * };
 * ```
 */
struct AsyncWorkHandle {
	/**
	 * A flag set by the main thread to signal the async worker that it has
	 * been cancelled.
	 */
	std::atomic<bool> cancelled{false};

	/**
	 * A set of all active async work tasks that are not fully completed. This
	 * means the async work is queued to be executed, currently executing,
	 * or the completion handler to run on the main thread hasn't run yet.
	 *
	 * Whenever `napi_create_async_work()` is called, register the async work
	 * reference using `registerAsyncWork()`. If `cancelAllAsyncWork()` is
	 * called, it loops over this set and calls `::napi_cancel_async_work()` on
	 * each async work task.
	 */
	std::set<napi_async_work> activeAsyncWork;

	/**
	 * A set of all async work tasks that have completed execution, but the
	 * completion handler hasn't run on the main thread yet. This is used to
	 * track which in-flight async work tasks have completed execution and its
	 * safe to null out the async work's associated handle.
	 */
	std::set<napi_async_work> executeCompletedWork;

	/**
	 * A mutex to protect the `activeAsyncWork` and `executeCompletedWork` sets.
	 */
	std::mutex asyncWorkMutex;

	/**
	 * A flag which is set by an async worker after it finishes executing its
	 * task and calls `signalAsyncWorkExecuteCompleted()`.
	 */
	std::condition_variable asyncWorkComplete;

	/**
	 * Registers an async work task with this handle.
	 */
	void registerAsyncWork(napi_async_work work) {
		std::lock_guard<std::mutex> lock(this->asyncWorkMutex);
		this->activeAsyncWork.insert(work);
	}

	/**
	 * Unregisters an async work task with this handle.
	 */
	void unregisterAsyncWork(napi_async_work work) {
		std::lock_guard<std::mutex> lock(this->asyncWorkMutex);
		this->activeAsyncWork.erase(work);
		this->executeCompletedWork.erase(work);

		DEBUG_LOG("%p AsyncWorkHandle::unregisterAsyncWork() work=%p activeAsyncWork.size()=%zu\n", this, work, this->activeAsyncWork.size())

		// notify if all work is complete
		if (this->activeAsyncWork.empty()) {
			this->asyncWorkComplete.notify_all();
		}
	}

	/**
	 * Called by an async worker after it finishes executing its task.
	 */
	void signalAsyncWorkExecuteCompleted(napi_async_work work) {
		std::lock_guard<std::mutex> lock(this->asyncWorkMutex);
		this->executeCompletedWork.insert(work);

		DEBUG_LOG("%p AsyncWorkHandle::signalAsyncWorkExecuteCompleted() work=%p executeCompletedWork.size()=%zu\n", this, work, this->executeCompletedWork.size())

		// check if all active work has completed execution
		bool allExecuteCompleted = true;
		for (auto activeWork : this->activeAsyncWork) {
			if (this->executeCompletedWork.find(activeWork) == this->executeCompletedWork.end()) {
				allExecuteCompleted = false;
				break;
			}
		}

		if (allExecuteCompleted) {
			this->asyncWorkComplete.notify_all();
		}
	}

	/**
	 * Cancels all active async work tasks. This called when the database is
	 * being closed.
	 */
	void cancelAllAsyncWork() {
		// set the cancellation flag
		this->cancelled.store(true);

		// cancel all active async work
		std::lock_guard<std::mutex> lock(this->asyncWorkMutex);
		if (this->activeAsyncWork.empty()) {
			DEBUG_LOG("%p AsyncWorkHandle::cancelAllAsyncWork() no active async work\n", this)
			return;
		}

		DEBUG_LOG("%p AsyncWorkHandle::cancelAllAsyncWork() %d active async work\n", this, this->activeAsyncWork.size())
		for (auto work : this->activeAsyncWork) {
			::napi_cancel_async_work(nullptr, work);
		}
	}

	/**
	 * After calling `cancelAllAsyncWork()`, this function waits for all active
	 * async work tasks to call `signalAsyncWorkExecuteCompleted()`. Note that
	 * if any in-flight async work tasks are cancelled, the async work complete
	 * handler will not yet have been called.
	 */
	void waitForAsyncWorkCompletion(
		std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)
	) {
		auto start = std::chrono::steady_clock::now();
		const auto pollInterval = std::chrono::milliseconds(10);

		std::unique_lock<std::mutex> lock(this->asyncWorkMutex);

		while (!this->activeAsyncWork.empty()) {
			auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
			if (elapsed >= timeout) {
				DEBUG_LOG("%p AsyncWorkHandle::waitForAsyncWorkCompletion() timeout waiting for async work completion, %zu items remaining\n", this, this->activeAsyncWork.size())
				return;
			}

			auto remainingTime = timeout - elapsed;
			auto waitTime = std::min(pollInterval, remainingTime);

			DEBUG_LOG("%p AsyncWorkHandle::waitForAsyncWorkCompletion() waiting for %zu active work items\n", this, this->activeAsyncWork.size())

			// wait for either all work to be unregistered OR all execute handlers to complete
			bool completed = this->asyncWorkComplete.wait_for(lock, waitTime, [this] {
				if (this->activeAsyncWork.empty()) {
					return true; // all work unregistered
				}

				// check if all active work has completed execution
				for (auto activeWork : this->activeAsyncWork) {
					if (this->executeCompletedWork.find(activeWork) == this->executeCompletedWork.end()) {
						return false; // at least one execute handler still running
					}
				}
				return true; // all execute handlers completed
			});

			if (completed) {
				DEBUG_LOG("%p AsyncWorkHandle::waitForAsyncWorkCompletion() all async work execution completed\n", this)
				return;
			}
		}
	}

	bool isCancelled() const {
		return this->cancelled.load();
	}
};

} // namespace rocksdb_js

#endif
