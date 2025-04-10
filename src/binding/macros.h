#ifndef __MACROS_H__
#define __MACROS_H__

#include <string>
#include <cstring>

/**
 * This file contains various preprocessor macros for common napi and RocksDB
 * operations.
 */

#define NAPI_STATUS_RETURN(call) \
	{ \
		napi_status status = (call); \
		if (status != napi_ok) { \
			return status; \
		} \
	}

#define NAPI_STATUS_THROWS_RVAL(call, rval) \
	{ \
		napi_status status = (call); \
		if (status != napi_ok) { \
			const napi_extended_error_info* error; \
			::napi_get_last_error_info(env, &error); \
			::napi_throw_error(env, nullptr, error->error_message ? error->error_message : getNapiStatusName(status)); \
			return rval; \
		} \
	}

#define NAPI_STATUS_THROWS(call) \
	NAPI_STATUS_THROWS_RVAL(call, nullptr)

#define NAPI_STATUS_THROWS_RUNTIME_ERROR(call) \
	{ \
		napi_status status = (call); \
		if (status != napi_ok) { \
			const napi_extended_error_info* error; \
			::napi_get_last_error_info(env, &error); \
			throw std::runtime_error(error->error_message); \
		} \
	}

#define NAPI_STATUS_THROWS_VOID(call) \
	{ \
		napi_status status = (call); \
		if (status != napi_ok) { \
			const napi_extended_error_info* error; \
			::napi_get_last_error_info(env, &error); \
			::napi_throw_error(env, nullptr, error->error_message); \
			return; \
		} \
	}

#define NAPI_RETURN_UNDEFINED() \
	napi_value undefined; \
	napi_get_undefined(env, &undefined); \
	return undefined;

#define NAPI_EXPORT_FUNCTION(name) \
	napi_value name##_fn; \
    NAPI_STATUS_THROWS(::napi_create_function(env, NULL, 0, name, NULL, &name##_fn)) \
    NAPI_STATUS_THROWS(::napi_set_named_property(env, exports, #name, name##_fn))

#define NAPI_CHECK_NEW_TARGET(className) \
	{ \
		napi_value newTarget; \
		::napi_get_new_target(env, info, &newTarget); \
		if (newTarget == nullptr) { \
			::napi_throw_error(env, nullptr, className " must be called with 'new'"); \
			return nullptr; \
		} \
	}

#define NAPI_CONSTRUCTOR(className) \
	NAPI_CHECK_NEW_TARGET(className) \
	napi_value jsThis; \
	NAPI_STATUS_THROWS(::napi_get_cb_info(env, info, nullptr, nullptr, &jsThis, nullptr))

#define NAPI_CONSTRUCTOR_ARGV(className, n) \
	NAPI_CHECK_NEW_TARGET(className) \
	napi_value args[n]; \
	size_t argc = n; \
	napi_value jsThis; \
	NAPI_STATUS_THROWS(::napi_get_cb_info(env, info, &argc, args, &jsThis, nullptr))

#define NAPI_METHOD() \
	napi_value jsThis; \
	NAPI_STATUS_THROWS(::napi_get_cb_info(env, info, nullptr, nullptr, &jsThis, nullptr))

#define NAPI_METHOD_ARGV(n) \
	napi_value argv[n]; \
	size_t argc = n; \
	napi_value jsThis; \
	NAPI_STATUS_THROWS(::napi_get_cb_info(env, info, &argc, argv, &jsThis, nullptr))

#define NAPI_GET_STRING(from, to) \
	std::string to; \
	rocksdb_js::getString(env, from, to);

#define _ROCKSDB_STATUS_FORMAT_ERROR(status, msg) \
	std::string errorStr; \
	{ \
		std::stringstream ss; \
		ss << msg << ": " << status.ToString(); \
		errorStr = ss.str(); \
		if (errorStr.size() > 2 && errorStr.compare(errorStr.size() - 2, 2, ": ") == 0) { \
			errorStr.erase(errorStr.size() - 2); \
		} \
	}

#define ROCKSDB_STATUS_CREATE_NAPI_ERROR(status, msg) \
	napi_value error; \
	{ \
		_ROCKSDB_STATUS_FORMAT_ERROR(status, msg) \
		napi_value errorMsg; \
		NAPI_STATUS_THROWS(::napi_create_string_utf8(env, errorStr.c_str(), errorStr.size(), &errorMsg)) \
		NAPI_STATUS_THROWS(::napi_create_error(env, nullptr, errorMsg, &error)) \
	}

#define ROCKSDB_STATUS_THROW_NAPI_ERROR(status, msg) \
	napi_value error; \
	{ \
		_ROCKSDB_STATUS_FORMAT_ERROR(status, msg) \
		napi_value errorMsg; \
		NAPI_STATUS_THROWS(::napi_create_string_utf8(env, errorStr.c_str(), errorStr.size(), &errorMsg)) \
		NAPI_STATUS_THROWS(::napi_create_error(env, nullptr, errorMsg, &error)) \
	}

#define ROCKSDB_STATUS_THROW_NAPI_ERROR_VOID(status, msg) \
	napi_value error; \
	{ \
		_ROCKSDB_STATUS_FORMAT_ERROR(status, msg) \
		napi_value errorMsg; \
		NAPI_STATUS_THROWS_VOID(::napi_create_string_utf8(env, errorStr.c_str(), errorStr.size(), &errorMsg)) \
		NAPI_STATUS_THROWS_VOID(::napi_create_error(env, nullptr, errorMsg, &error)) \
	}

#define ROCKSDB_STATUS_THROWS(call, msg) \
	{ \
		rocksdb::Status status = (call); \
		if (!status.ok()) { \
			ROCKSDB_STATUS_CREATE_NAPI_ERROR(status, msg) \
			::napi_throw(env, error); \
			return nullptr; \
		} \
	}

[[maybe_unused]] static const char* getNapiStatusName(napi_status status) {
    switch (status) {
        case napi_array_expected: return "napi_array_expected";
        case napi_arraybuffer_expected: return "napi_arraybuffer_expected";
        case napi_bigint_expected: return "napi_bigint_expected";
        case napi_boolean_expected: return "napi_boolean_expected";
        case napi_callback_scope_mismatch: return "napi_callback_scope_mismatch";
        case napi_cancelled: return "napi_cancelled";
        case napi_closing: return "napi_closing";
        case napi_date_expected: return "napi_date_expected";
        case napi_detachable_arraybuffer_expected: return "napi_detachable_arraybuffer_expected";
        case napi_escape_called_twice: return "napi_escape_called_twice";
        case napi_function_expected: return "napi_function_expected";
        case napi_generic_failure: return "napi_generic_failure";
        case napi_handle_scope_mismatch: return "napi_handle_scope_mismatch";
        case napi_invalid_arg: return "napi_invalid_arg";
        case napi_name_expected: return "napi_name_expected";
        case napi_number_expected: return "napi_number_expected";
        case napi_object_expected: return "napi_object_expected";
        case napi_ok: return "napi_ok";
        case napi_pending_exception: return "napi_pending_exception";
        case napi_queue_full: return "napi_queue_full";
        case napi_string_expected: return "napi_string_expected";
        default: return "unknown";
    }
}

#endif
