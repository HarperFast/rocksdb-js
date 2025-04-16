#include <node_api.h>
#include <string>
#include <sstream>
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

}
