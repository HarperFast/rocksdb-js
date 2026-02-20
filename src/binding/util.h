#ifndef __UTIL_H__
#define __UTIL_H__

#include <atomic>
#include <chrono>
#include <cstdint>
#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <node_api.h>
#include <optional>
#include <set>
#include <string>
#include <sstream>
#include <thread>
#include "binding.h"
#include "macros.h"
#include "rocksdb/db.h"

/**
 * This file contains various napi helper functions.
 *
 * Note: These function must go in a header file because the compiler doesn't
 * know the data type sizes until link time.
 */

namespace rocksdb_js {

#define RANGE_CHECK(condition, errorMsg, rval) \
	do { \
		if (condition) { \
			std::stringstream ss; \
			ss << errorMsg; \
			::napi_throw_range_error(env, nullptr, ss.str().c_str()); \
			return rval; \
		} \
	} while (0)

struct Closable {
	virtual ~Closable() = default;
	virtual void close() = 0;
};

void createJSError(napi_env env, const char* code, const char* message, napi_value& error);

std::shared_ptr<rocksdb::ColumnFamilyHandle> createRocksDBColumnFamily(const std::shared_ptr<rocksdb::DB> db, const std::string& name);

void createRocksDBError(napi_env env, rocksdb::Status status, const char* msg, napi_value& error);

void debugLog(const bool showTimestamp, const char* msg, ...);

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

/**
 * This utility function is designed to support an argument that can be either a buffer that is translated to a slice,
 * or a number that is used as the length of the key if it was written into the shared buffer, which is then returned
 * as a slice. This provides a flexible way to handle a buffer that can leverage shared buffers with only passing a number
 * for high-performance, as well as a buffer for the flexibility of larger buffer.
 */
bool getSliceFromArg(napi_env env, napi_value arg, rocksdb::Slice& result, char* defaultBuffer, const char* errorMsg);

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
			uint32_t start = 0;
			uint32_t end = 0;
			size_t length = 0;
			NAPI_STATUS_RETURN(::napi_get_buffer_info(env, from, reinterpret_cast<void**>(&buf), &length));

			if (buf == nullptr) {
				// data is null because the buffer is empty
				to.assign("");
				return napi_ok;
			}

			bool hasStart;
			napi_value startValue;
			NAPI_STATUS_RETURN(::napi_has_named_property(env, from, "start", &hasStart));
			if (hasStart) {
				NAPI_STATUS_RETURN(::napi_get_named_property(env, from, "start", &startValue));
				NAPI_STATUS_RETURN(::napi_get_value_uint32(env, startValue, &start));
			}

			bool hasEnd;
			napi_value endValue;
			NAPI_STATUS_RETURN(::napi_has_named_property(env, from, "end", &hasEnd));
			if (hasEnd) {
				NAPI_STATUS_RETURN(::napi_get_named_property(env, from, "end", &endValue));
				NAPI_STATUS_RETURN(::napi_get_value_uint32(env, endValue, &end));
			} else {
				end = length;
			}

			RANGE_CHECK(start > end, "Buffer start greater than end (start=" << start << ", end=" << end << ")", napi_invalid_arg);
			RANGE_CHECK(start > length, "Buffer start greater than length (start=" << start << ", length=" << length << ")", napi_invalid_arg);
			RANGE_CHECK(end > length, "Buffer end greater than length (end=" << end << ", length=" << length << ")", napi_invalid_arg);

			to.assign(buf + start, end - start);
			return napi_ok;
		}

		return napi_invalid_arg;
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

[[maybe_unused]] static napi_status getValue(napi_env env, napi_value value, uint8_t& result) {
	uint32_t tmp;
	NAPI_STATUS_RETURN(::napi_get_value_uint32(env, value, &tmp));
	result = static_cast<uint8_t>(tmp);
	return napi_ok;
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

[[maybe_unused]] static napi_status getValue(napi_env env, napi_value value, float& result) {
	double result2;
	NAPI_STATUS_RETURN(::napi_get_value_double(env, value, &result2));
	result = static_cast<float>(result2);
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
 * Converts `std::filesystem::file_time_type` to a
 * `std::chrono::system_clock::time_point` with proper handling for different
 * platforms and C++ standard library versions.
 *
 * @param fileTime - The file time to convert.
 * @returns The system time.
 */
std::chrono::system_clock::time_point convertFileTimeToSystemTime(const std::filesystem::file_time_type& fileTime);

/**
 * Returns the current timestamp as a monotonically increasing timestamp in
 * nanoseconds (internally) and returns it as milliseconds (double).
 */
double getMonotonicTimestamp();

/**
 * Tries to create a directory. If the directory already exists, does nothing.
 * If the directory cannot be created, throws an error.
 *
 * @param path - The path to the directory to create.
 * @param permissions - The permissions to set on the directory (default: 0750).
 * @param retries - The number of times to retry if the directory cannot be
 * created.
 */
void tryCreateDirectory(
	const std::filesystem::path& path,
	std::filesystem::perms permissions =
		std::filesystem::perms::owner_read |
		std::filesystem::perms::owner_write |
		std::filesystem::perms::owner_exec |
		std::filesystem::perms::group_read |
		std::filesystem::perms::group_exec,
	uint8_t retries = 3
);

/**
 * Base class for async state.
 */
template<typename T>
struct BaseAsyncState {
	napi_env env;
	T handle;
	napi_async_work asyncWork;
	napi_ref resolveRef;
	napi_ref rejectRef;
	rocksdb::Status status;

	/**
	 * A flag set by the execute handler to signal that the async work has been
	 * completed. Note that we can't set `handle` to `nullptr` and use it as a
	 * a flag because that would cause the shared pointer to the handle (e.g.
	 * `TransactionHandle`) to destroy the handle before the async work complete
	 * handler has a chance to close the handle.
	 */
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

	/**
	 * Calls the resolve function. This function must be called from the main
	 * thread after the async work has completed.
	 *
	 * @param [result] An optionalresult to pass to the resolve function.
	 */
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

	/**
	 * Calls the reject function. This function must be called from the main
	 * thread after the async work has completed.
	 *
	 * @param error The error to pass to the reject function.
	 */
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
	 * been cancelled. The async worker calls `handle->isCancelled()` to check
	 * if it has been cancelled.
	 */
	std::atomic<bool> cancelled{false};

	/**
	 * Count of active async work tasks.
	 */
	std::atomic<int32_t> activeAsyncWorkCount{0};

	/**
	 * A mutex used to wait for all async work to complete.
	 */
	std::mutex waitMutex;

	/**
	 * A flag which is set by an async worker after it finishes executing its
	 * task and calls `unregisterAsyncWork()`.
	 */
	std::condition_variable asyncWorkComplete;

	/**
	 * Registers an async work task with this handle.
	 */
	void registerAsyncWork() {
		++this->activeAsyncWorkCount;
	}

	/**
	 * Unregisters an async work task with this handle.
	 */
	void unregisterAsyncWork() {
		auto activeAsyncWorkCount = --this->activeAsyncWorkCount;
		if (activeAsyncWorkCount > 0) {
			DEBUG_LOG("%p AsyncWorkHandle::unregisterAsyncWork Still have %u active async work tasks\n", this, activeAsyncWorkCount);
		} else if (activeAsyncWorkCount == 0) {
			// notify if all work is complete
			DEBUG_LOG("%p AsyncWorkHandle::unregisterAsyncWork All async work has completed, notifying\n", this);
			this->asyncWorkComplete.notify_one();
		}
	}

	/**
	 * Cancels all active async work tasks. This called when the database is
	 * being closed.
	 */
	void cancelAllAsyncWork() {
		this->cancelled.store(true);
	}

	/**
	 * After calling `cancelAllAsyncWork()`, this function waits for all active
	 * async work tasks to call `unregisterAsyncWork()`. Note that if any
	 * in-flight async work tasks are cancelled, the async work complete handler
	 * will not yet have been called.
	 */
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

			// wait for either all work to be unregistered OR all execute handlers to complete
			bool completed = this->asyncWorkComplete.wait_for(lock, waitTime, [this] {
				// check if all active work has completed execution
				// note: the mutex is already locked here
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
};

// Big-endian encoding/decoding helpers for transaction log format
inline void writeUint64BE(char* buffer, uint64_t value) {
	buffer[0] = static_cast<char>((value >> 56) & 0xFF);
	buffer[1] = static_cast<char>((value >> 48) & 0xFF);
	buffer[2] = static_cast<char>((value >> 40) & 0xFF);
	buffer[3] = static_cast<char>((value >> 32) & 0xFF);
	buffer[4] = static_cast<char>((value >> 24) & 0xFF);
	buffer[5] = static_cast<char>((value >> 16) & 0xFF);
	buffer[6] = static_cast<char>((value >> 8) & 0xFF);
	buffer[7] = static_cast<char>(value & 0xFF);
}

inline void writeUint32BE(char* buffer, uint32_t value) {
	buffer[0] = static_cast<char>((value >> 24) & 0xFF);
	buffer[1] = static_cast<char>((value >> 16) & 0xFF);
	buffer[2] = static_cast<char>((value >> 8) & 0xFF);
	buffer[3] = static_cast<char>(value & 0xFF);
}

inline void writeUint16BE(char* buffer, uint16_t value) {
	buffer[0] = static_cast<char>((value >> 8) & 0xFF);
	buffer[1] = static_cast<char>(value & 0xFF);
}

inline void writeUint8(char* buffer, uint8_t value) {
	buffer[0] = static_cast<char>(value);
}

inline uint64_t readUint64BE(const char* buffer) {
	return (static_cast<uint64_t>(static_cast<uint8_t>(buffer[0])) << 56) |
	       (static_cast<uint64_t>(static_cast<uint8_t>(buffer[1])) << 48) |
	       (static_cast<uint64_t>(static_cast<uint8_t>(buffer[2])) << 40) |
	       (static_cast<uint64_t>(static_cast<uint8_t>(buffer[3])) << 32) |
	       (static_cast<uint64_t>(static_cast<uint8_t>(buffer[4])) << 24) |
	       (static_cast<uint64_t>(static_cast<uint8_t>(buffer[5])) << 16) |
	       (static_cast<uint64_t>(static_cast<uint8_t>(buffer[6])) << 8) |
	       static_cast<uint64_t>(static_cast<uint8_t>(buffer[7]));
}

inline uint32_t readUint32BE(const char* buffer) {
	return (static_cast<uint32_t>(static_cast<uint8_t>(buffer[0])) << 24) |
	       (static_cast<uint32_t>(static_cast<uint8_t>(buffer[1])) << 16) |
	       (static_cast<uint32_t>(static_cast<uint8_t>(buffer[2])) << 8) |
	       static_cast<uint32_t>(static_cast<uint8_t>(buffer[3]));
}

inline uint16_t readUint16BE(const char* buffer) {
	return (static_cast<uint16_t>(static_cast<uint8_t>(buffer[0])) << 8) |
	       static_cast<uint16_t>(static_cast<uint8_t>(buffer[1]));
}

inline uint8_t readUint8(const char* buffer) {
	return static_cast<uint8_t>(buffer[0]);
}

inline void writeDoubleBE(char* buffer, double value) {
	// Interpret the double's bits as uint64_t and write in big endian
	union {
		double d;
		uint64_t u;
	} converter;
	converter.d = value;
	writeUint64BE(buffer, converter.u);
}

inline double readDoubleBE(const char* buffer) {
	// Read uint64_t in big endian and interpret as double
	union {
		double d;
		uint64_t u;
	} converter;
	converter.u = readUint64BE(buffer);
	return converter.d;
}

} // namespace rocksdb_js

#endif
