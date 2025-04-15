#include <node_api.h>
#include <string>
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

}
