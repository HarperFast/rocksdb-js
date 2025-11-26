#include <cmath>
#include <cinttypes>
#include <cstdarg>
#include <functional>
#include <limits>
#include <node_api.h>
#include <string>
#include <sstream>
#include <thread>
#include "util.h"

namespace rocksdb_js {

/**
 * Logs a debug message to stderr prefixed with the current thread id.
 */
void debugLog(const bool showThreadId, const char* msg, ...) {
	va_list args;
	va_start(args, msg);
	if (showThreadId) {
		fprintf(stderr, "[%04zu] ", std::hash<std::thread::id>{}(std::this_thread::get_id()) % 10000);
	}
	vfprintf(stderr, msg, args);
	va_end(args);
	fflush(stderr);
}

/**
 * Dumps the value of a napi_value to stderr.
 *
 * Note: Use the `DEBUG_LOG_NAPI_VALUE()` macro instead of calling this
 * directly.
 */
void debugLogNapiValue(napi_env env, napi_value value, uint16_t indent, bool isObject) {
	napi_valuetype type;
	if (indent > 5 || ::napi_typeof(env, value, &type) != napi_ok) {
		return;
	}

	if (!isObject) {
		for (uint16_t i = 0; i < indent; i++) {
			fprintf(stderr, "  ");
		}
	}

	switch (type) {
		case napi_undefined: fprintf(stderr, "undefined"); break;
		case napi_null: fprintf(stderr, "null"); break;
		case napi_boolean: {
			bool result;
			NAPI_STATUS_THROWS_VOID(::napi_get_value_bool(env, value, &result))
			if (result) {
				fprintf(stderr, "true");
			} else {
				fprintf(stderr, "false");
			}
			break;
		}
		case napi_number: {
			double result;
			NAPI_STATUS_THROWS_VOID(::napi_get_value_double(env, value, &result))
			fprintf(stderr, "%f", result);
			break;
		}
		case napi_string: {
			char buffer[1024];
			size_t result;
			NAPI_STATUS_THROWS_VOID(::napi_get_value_string_utf8(env, value, buffer, sizeof(buffer), &result))
			fprintf(stderr, "\"%s\"", buffer);
			break;
		}
		case napi_function:
		case napi_symbol: {
			napi_value toStringFn;
			NAPI_STATUS_THROWS_VOID(::napi_get_named_property(env, value, "toString", &toStringFn))

			napi_value resultValue;
			NAPI_STATUS_THROWS_VOID(::napi_call_function(env, value, toStringFn, 0, nullptr, &resultValue));

			char buffer[128];
			size_t result;
			NAPI_STATUS_THROWS_VOID(::napi_get_value_string_utf8(env, resultValue, buffer, sizeof(buffer), &result))

			fprintf(stderr, "%s", buffer);
			break;
		}
		case napi_object: {
			bool isArray;
			NAPI_STATUS_THROWS_VOID(::napi_is_array(env, value, &isArray));
			if (isArray) {
				uint32_t length;
				NAPI_STATUS_THROWS_VOID(::napi_get_array_length(env, value, &length))

				fprintf(stderr, "[");
				if (length > 0) {
					fprintf(stderr, "\n");
					for (uint32_t i = 0; i < length; i++) {
						napi_value element;
						NAPI_STATUS_THROWS_VOID(::napi_get_element(env, value, i, &element))
						debugLogNapiValue(env, element, indent + 1);
						if (i < length - 1) {
							fprintf(stderr, ", // %u\n", i);
						} else {
							fprintf(stderr, "  // %u\n", i);
						}
					}
				}
				fprintf(stderr, "]");
			} else {
				napi_value properties;
				NAPI_STATUS_THROWS_VOID(::napi_get_property_names(env, value, &properties))
				uint32_t length;
				NAPI_STATUS_THROWS_VOID(::napi_get_array_length(env, properties, &length))

				fprintf(stderr, "{");
				if (length > 0) {
					fprintf(stderr, "\n");
					for (uint32_t i = 0; i < length; i++) {
						napi_value propertyName;
						NAPI_STATUS_THROWS_VOID(::napi_get_element(env, properties, i, &propertyName))

						char nameBuffer[1024];
						size_t nameLength;
						NAPI_STATUS_THROWS_VOID(::napi_get_value_string_utf8(env, propertyName, nameBuffer, sizeof(nameBuffer), &nameLength))

						napi_value propertyValue;
						NAPI_STATUS_THROWS_VOID(::napi_get_property(env, value, propertyName, &propertyValue))

						for (uint16_t i = 0; i < indent; i++) {
							fprintf(stderr, "  ");
						}
						fprintf(stderr, "  %s: ", nameBuffer);
						debugLogNapiValue(env, propertyValue, indent + 1, true);
						if (i < length - 1) {
							fprintf(stderr, ",");
						}
						fprintf(stderr, "\n");
					}
				}

				for (uint16_t i = 0; i < indent; i++) {
					fprintf(stderr, "  ");
				}
				fprintf(stderr, "}");
			}
			break;
		}
		case napi_external: fprintf(stderr, "[external]"); break;
		case napi_bigint: {
			int64_t result;
			bool lossless;
			NAPI_STATUS_THROWS_VOID(::napi_get_value_bigint_int64(env, value, &result, &lossless))
			if (lossless) {
				fprintf(stderr, "%" PRId64, result);
			} else {
				fprintf(stderr, "%" PRId64 " (lossy)", result);
			}
			break;
		}
		default: fprintf(stderr, "[unknown]");
	}

	if (!isObject) {
		fprintf(stderr, "\n");
	}
}

/**
 * Gets a `key` from a JavaScript object property and stores it in the
 * specified `RocksDB::Slice`.
 */
napi_status getKeyFromProperty(
	napi_env env,
	napi_value obj,
	const char* prop,
	const char* errorMsg,
	const char*& keyStr,
	uint32_t& start,
	uint32_t& end
) {
	napi_value value;
	NAPI_STATUS_THROWS_RVAL(::napi_get_named_property(env, obj, prop, &value), napi_invalid_arg);

	bool has = false;
	NAPI_STATUS_THROWS_RVAL(::napi_has_named_property(env, obj, prop, &has), napi_invalid_arg);
	if (!has) {
		return napi_ok;
	}

	napi_valuetype valueType;
	NAPI_STATUS_THROWS_RVAL(::napi_typeof(env, value, &valueType), napi_invalid_arg);
	if (valueType == napi_undefined) {
		return napi_ok;
	}

	bool isBuffer;
	NAPI_STATUS_THROWS_RVAL(::napi_is_buffer(env, value, &isBuffer), napi_invalid_arg);
	if (!isBuffer) {
		::napi_throw_error(env, nullptr, errorMsg);
		return napi_invalid_arg;
	}

	size_t length = 0;
	keyStr = rocksdb_js::getNapiBufferFromArg(env, value, start, end, length, errorMsg);

	return napi_ok;
}

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

/**
 * Helper function to create a column family.
 *
 * @param db - The RocksDB database instance.
 * @param name - The name of the column family.
 */
std::shared_ptr<rocksdb::ColumnFamilyHandle> createRocksDBColumnFamily(const std::shared_ptr<rocksdb::DB> db, const std::string& name) {
	rocksdb::ColumnFamilyHandle* cfHandle;
	rocksdb::Status status = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), name, &cfHandle);
	if (!status.ok()) {
		throw std::runtime_error(status.ToString().c_str());
	}
	return std::shared_ptr<rocksdb::ColumnFamilyHandle>(cfHandle);
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
 * Creates a new JavaScript error object with custom code and message.
 */
void createJSError(napi_env env, const char* code, const char* message, napi_value& error) {
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

	NAPI_STATUS_THROWS_VOID(::napi_create_string_utf8(env, code, NAPI_AUTO_LENGTH, &errorCode))
	NAPI_STATUS_THROWS_VOID(::napi_create_string_utf8(env, message, NAPI_AUTO_LENGTH, &errorMsg))

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

	RANGE_CHECK(start > end, "Buffer start greater than end (start=" << start << ", end=" << end << ")", nullptr)
	RANGE_CHECK(start > length, "Buffer start greater than length (start=" << start << ", length=" << length << ")", nullptr)
	RANGE_CHECK(end > length, "Buffer end greater than length (end=" << end << ", length=" << length << ")", nullptr)

	if (data == nullptr) {
		// data is null because the buffer is empty
		return "";
	}

	return data;
}

/**
 * Converts std::filesystem::file_time_type to std::chrono::system_clock::time_point
 * with proper handling for different platforms and C++ standard versions.
 */
std::chrono::system_clock::time_point convertFileTimeToSystemTime(
	const std::filesystem::file_time_type& fileTime
) {
#ifdef _WIN32
	// Windows `file_time_type` uses 1601 epoch, `system_clock` uses 1970 epoch
	// the difference is 369 years = 11644473600 seconds
	constexpr auto epoch_diff = std::chrono::seconds(11644473600);
	return std::chrono::system_clock::time_point(
		std::chrono::duration_cast<std::chrono::system_clock::duration>(
			fileTime.time_since_epoch() - epoch_diff));
#else
	// On POSIX systems, file_time_type::clock may not be system_clock.
	// Use clock_cast if available (C++20), otherwise use a runtime-calculated offset.
	#if defined(__cpp_lib_chrono) && __cpp_lib_chrono >= 201907L
		// C++20 with proper clock_cast support
		return std::chrono::clock_cast<std::chrono::system_clock>(fileTime);
	#else
		// Fallback: calculate the offset between the two clock epochs.
		// This works by assuming both clocks advance at the same rate (which they do).
		// The offset is constant for the lifetime of the program.
		using file_clock = std::filesystem::file_time_type::clock;
		static const auto offset = []() -> std::chrono::nanoseconds {
			// Get "now" from both clocks and compute the difference
			auto sys_now = std::chrono::system_clock::now();
			auto file_now = file_clock::now();

			// The offset is the difference in their epochs
			auto sys_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(sys_now.time_since_epoch());
			auto file_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(file_now.time_since_epoch());

			return sys_ns - file_ns;
		}();

		// Apply the offset to convert from file_clock to system_clock
		auto file_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(fileTime.time_since_epoch());
		auto sys_ns = file_ns + offset;
		return std::chrono::system_clock::time_point(
			std::chrono::duration_cast<std::chrono::system_clock::duration>(sys_ns));
	#endif
#endif
}

static std::atomic<double> lastTimestamp{0.0};

double getTimestamp() {
	uint64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
		std::chrono::system_clock::now().time_since_epoch()
	).count();

	double result = static_cast<double>(now / 1000000) + static_cast<double>(now % 1000000) / 1000000.0;

	double last = lastTimestamp.load(std::memory_order_acquire);
	if (result <= last) {
		result = (double) ((uint64_t)result + 1);
	}

	while (!lastTimestamp.compare_exchange_strong(last, result, std::memory_order_acq_rel)) {
		if (result <= last) {
			result = (double) ((uint64_t)result + 1);
		}
	}

	return result;
}

}
