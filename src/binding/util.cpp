#include <node_api.h>
#include <string>
#include <sstream>
#include <cstdarg>
#include <thread>
#include <functional>
#include "util.h"

namespace rocksdb_js {

/**
 * Gets an error message from the NAPI error.
 */
std::string getNapiExtendedError(napi_env env, napi_status& status, const char* errorMsg) {
	if (errorMsg) {
		return std::string(errorMsg);
	}

	const napi_extended_error_info* error;
	if (::napi_get_last_error_info(env, &error) == napi_ok && error->error_message) {
		return std::string(error->error_message);
	}

	const char* errorStr = "unknown error";
	switch (status) {
		case napi_ok: errorStr = "No error"; break;
		case napi_invalid_arg: errorStr = "Invalid argument"; break;
		case napi_object_expected: errorStr = "An object was expected"; break;
		case napi_string_expected: errorStr = "A string was expected"; break;
		case napi_name_expected: errorStr = "A string or symbol was expected"; break;
		case napi_function_expected: errorStr = "A function was expected"; break;
		case napi_number_expected: errorStr = "A number was expected"; break;
		case napi_boolean_expected: errorStr = "A boolean was expected"; break;
		case napi_array_expected: errorStr = "An array was expected"; break;
		case napi_generic_failure: errorStr = "Unknown failure"; break;
		case napi_pending_exception: errorStr = "An exception is pending"; break;
		case napi_cancelled: errorStr = "The async work item was cancelled"; break;
		case napi_escape_called_twice: errorStr = "napi_escape_handle already called on scope"; break;
		case napi_handle_scope_mismatch: errorStr = "Invalid handle scope usage"; break;
		case napi_callback_scope_mismatch: errorStr = "Invalid callback scope usage"; break;
		case napi_queue_full: errorStr = "Thread-safe function queue is full"; break;
		case napi_closing: errorStr = "Thread-safe function handle is closing"; break;
		case napi_bigint_expected: errorStr = "A bigint was expected"; break;
		case napi_date_expected: errorStr = "A date was expected"; break;
		case napi_arraybuffer_expected: errorStr = "An arraybuffer was expected"; break;
		case napi_detachable_arraybuffer_expected: errorStr = "A detachable arraybuffer was expected"; break;
		case napi_would_deadlock: errorStr = "Main thread would deadlock"; break;
		case napi_no_external_buffers_allowed: errorStr = "External buffers are not allowed"; break;
		case napi_cannot_run_js: errorStr = "Cannot run JavaScript"; break;
	}
	return std::string(errorStr);
}

static const char* errorCodeStrings[] = {
	"ERR_UNKNOWN",
	"ERR_NOT_FOUND",
	"ERR_CORRUPTION",
	"ERR_NOT_SUPPORTED",
	"ERR_INVALID_ARGUMENT",
	"ERR_IO_ERROR",
	"ERR_MERGE_IN_PROGRESS",
	"ERR_INCOMPLETE",
	"ERR_SHUTDOWN_IN_PROGRESS",
	"ERR_TIMED_OUT",
	"ERR_ABORTED",
	"ERR_BUSY",
	"ERR_EXPIRED",
	"ERR_TRY_AGAIN",
	"ERR_COMPACTION_TOO_LARGE",
	"ERR_COLUMN_FAMILY_DROPPED"
};

/**
 * Creates a new JavaScript error object from a RocksDB status.
 */
void createRocksDBError(napi_env env, rocksdb::Status status, const char* msg, napi_value& error) {
	ROCKSDB_STATUS_FORMAT_ERROR(status, msg)

	napi_value global;
	napi_value objectCtor;
	napi_value objectCreateFn;
	napi_value errorCtor;
	napi_value errorProto;
	napi_value errorCode;
	napi_value errorMsg;

	NAPI_STATUS_THROWS_VOID(::napi_get_global(env, &global))
	NAPI_STATUS_THROWS_VOID(::napi_get_named_property(env, global, "Object", &objectCtor))
	NAPI_STATUS_THROWS_VOID(::napi_get_named_property(env, objectCtor, "create", &objectCreateFn))
	NAPI_STATUS_THROWS_VOID(::napi_get_named_property(env, global, "Error", &errorCtor))
	NAPI_STATUS_THROWS_VOID(::napi_get_named_property(env, errorCtor, "prototype", &errorProto))

	const char* codeStr;
	switch (status.code()) {
		case rocksdb::Status::Code::kNotFound: codeStr = errorCodeStrings[1]; break;
		case rocksdb::Status::Code::kCorruption: codeStr = errorCodeStrings[2]; break;
		case rocksdb::Status::Code::kNotSupported: codeStr = errorCodeStrings[3]; break;
		case rocksdb::Status::Code::kInvalidArgument: codeStr = errorCodeStrings[4]; break;
		case rocksdb::Status::Code::kIOError: codeStr = errorCodeStrings[5]; break;
		case rocksdb::Status::Code::kMergeInProgress: codeStr = errorCodeStrings[6]; break;
		case rocksdb::Status::Code::kIncomplete: codeStr = errorCodeStrings[7]; break;
		case rocksdb::Status::Code::kShutdownInProgress: codeStr = errorCodeStrings[8]; break;
		case rocksdb::Status::Code::kTimedOut: codeStr = errorCodeStrings[9]; break;
		case rocksdb::Status::Code::kAborted: codeStr = errorCodeStrings[10]; break;
		case rocksdb::Status::Code::kBusy: codeStr = errorCodeStrings[11]; break;
		case rocksdb::Status::Code::kExpired: codeStr = errorCodeStrings[12]; break;
		case rocksdb::Status::Code::kTryAgain: codeStr = errorCodeStrings[13]; break;
		case rocksdb::Status::Code::kCompactionTooLarge: codeStr = errorCodeStrings[14]; break;
		case rocksdb::Status::Code::kColumnFamilyDropped: codeStr = errorCodeStrings[15]; break;
		default: codeStr = errorCodeStrings[0]; break;
	}
	NAPI_STATUS_THROWS_VOID(::napi_create_string_utf8(env, codeStr, NAPI_AUTO_LENGTH, &errorCode))

	NAPI_STATUS_THROWS_VOID(::napi_create_string_utf8(env, errorStr.c_str(), errorStr.size(), &errorMsg))

	napi_value createArgs[1] = { errorProto };
	NAPI_STATUS_THROWS_VOID(::napi_call_function(env, objectCtor, objectCreateFn, 1, createArgs, &error))
	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, error, "code", errorCode))
	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, error, "message", errorMsg))
}

/**
 * Gets a buffer from a JavaScript function argument. Additionally, it sets
 * the `start` and `end` based on the `start` and `end` properties of the
 * buffer, otherwise it will set them based on the length of the buffer.
 */
const char* getNapiBufferFromArg(
	napi_env env,
	napi_value arg,
	uint32_t& start,
	uint32_t& end,
	size_t& length,
	const char* errorMsg
) {
	char* data = nullptr;

	start = 0;
	end = 0;
	length = 0;

	bool isBuffer;
	NAPI_STATUS_THROWS(::napi_is_buffer(env, arg, &isBuffer));
	if (!isBuffer) {
		::napi_throw_error(env, nullptr, errorMsg);
		return nullptr;
	}

	NAPI_STATUS_THROWS(::napi_get_buffer_info(env, arg, reinterpret_cast<void**>(&data), &length));

	bool hasStart;
	napi_value startValue;
	NAPI_STATUS_THROWS(::napi_has_named_property(env, arg, "start", &hasStart));
	if (hasStart) {
		NAPI_STATUS_THROWS(::napi_get_named_property(env, arg, "start", &startValue));
		NAPI_STATUS_THROWS(::napi_get_value_uint32(env, startValue, &start));
	}

	bool hasEnd;
	napi_value endValue;
	NAPI_STATUS_THROWS(::napi_has_named_property(env, arg, "end", &hasEnd));
	if (hasEnd) {
		NAPI_STATUS_THROWS(::napi_get_named_property(env, arg, "end", &endValue));
		NAPI_STATUS_THROWS(::napi_get_value_uint32(env, endValue, &end));
	} else {
		end = length;
	}

	if (start > end) {
		::napi_throw_error(env, nullptr, "Invalid buffer value start and end");
		return nullptr;
	}

	return data;
}

/**
 * Logs a debug message to stderr prefixed with the current thread id.
 */
void debugLog(const char* msg, ...) {
    va_list args;
    va_start(args, msg);
    fprintf(stderr, "[%04zu] ", std::hash<std::thread::id>{}(std::this_thread::get_id()) % 10000);
    vfprintf(stderr, msg, args);
    va_end(args);
}

/**
 * Gets a `key` from a JavaScript object property and stores it in the
 * specified `RocksDB::Slice`.
 */
void getKeyFromProperty(
	napi_env env,
	napi_value obj,
	const char* prop,
	rocksdb::Slice& result,
	const char* errorMsg,
	bool exclusiveStart,
	bool inclusiveEnd
) {
	uint32_t start = 0;
	uint32_t end = 0;
	size_t length = 0;

	napi_valuetype objType;
	NAPI_STATUS_THROWS_VOID(::napi_typeof(env, obj, &objType));
	if (objType != napi_object) {
		return;
	}

	napi_value value;
	NAPI_STATUS_THROWS_VOID(::napi_get_named_property(env, obj, prop, &value));

	bool has = false;
	NAPI_STATUS_THROWS_VOID(::napi_has_named_property(env, obj, prop, &has));
	if (!has) {
		return;
	}

	napi_valuetype valueType;
	NAPI_STATUS_THROWS_VOID(::napi_typeof(env, value, &valueType));
	if (valueType == napi_undefined) {
		return;
	}

	bool isBuffer;
	NAPI_STATUS_THROWS_VOID(::napi_is_buffer(env, value, &isBuffer));
	if (!isBuffer) {
		::napi_throw_error(env, nullptr, errorMsg);
		return;
	}

	if (inclusiveEnd) {
		// allocate one more byte for the null terminator
		length++;
	}

	const char* data = rocksdb_js::getNapiBufferFromArg(env, value, start, end, length, errorMsg);
	if (data == nullptr) {
		return;
	}

	if (exclusiveStart) {
		// TODO: how to make this exclusive?
		// have the iterator seek past the first key?
	}

	if (inclusiveEnd) {
		// TODO: how to make this inclusive?
		// add \0 to the end of the key?
	}

	result = rocksdb::Slice(data + start, end - start);
}

}
