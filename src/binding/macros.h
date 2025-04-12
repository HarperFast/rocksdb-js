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
			::napi_throw_error(env, nullptr, error->error_message ? error->error_message : "unknown error"); \
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

#define ROCKSDB_STATUS_THROWS(call, msg) \
	{ \
		rocksdb::Status status = (call); \
		if (!status.ok()) { \
			ROCKSDB_STATUS_CREATE_NAPI_ERROR(status, msg) \
			::napi_throw(env, error); \
			return nullptr; \
		} \
	}

#define ROCKSDB_STATUS_THROWS_ERROR_LIKE(call, msg) \
	{ \
		rocksdb::Status status = (call); \
		if (!status.ok()) { \
			napi_value error; \
			_ROCKSDB_STATUS_FORMAT_ERROR(status, msg) \
			napi_value global; \
			napi_value objectCtor; \
			napi_value objectCreateFn; \
			napi_value errorCtor; \
			napi_value errorProto; \
			napi_value errorMsg; \
			NAPI_STATUS_THROWS(::napi_get_global(env, &global)) \
			NAPI_STATUS_THROWS(::napi_get_named_property(env, global, "Object", &objectCtor)) \
			NAPI_STATUS_THROWS(::napi_get_named_property(env, objectCtor, "create", &objectCreateFn)) \
			NAPI_STATUS_THROWS(::napi_get_named_property(env, global, "Error", &errorCtor)) \
			NAPI_STATUS_THROWS(::napi_get_prototype(env, errorCtor, &errorProto)) \
			napi_value createArgs[1] = { errorProto }; \
			NAPI_STATUS_THROWS(::napi_call_function(env, objectCtor, objectCreateFn, 1, createArgs, &error)) \
			NAPI_STATUS_THROWS(::napi_create_string_utf8(env, errorStr.c_str(), errorStr.size(), &errorMsg)) \
			NAPI_STATUS_THROWS(::napi_set_named_property(env, error, "message", errorMsg)) \
			::napi_throw(env, error); \
		} \
	}

#define ROCKSDB_STATUS_CREATE_ERROR_LIKE_VOID(status, msg) \
	napi_value error; \
	{ \
		_ROCKSDB_STATUS_FORMAT_ERROR(status, msg) \
		napi_value global; \
		napi_value objectCtor; \
		napi_value objectCreateFn; \
		napi_value errorCtor; \
		napi_value errorProto; \
		napi_value errorMsg; \
		NAPI_STATUS_THROWS_VOID(::napi_get_global(env, &global)) \
		NAPI_STATUS_THROWS_VOID(::napi_get_named_property(env, global, "Object", &objectCtor)) \
		NAPI_STATUS_THROWS_VOID(::napi_get_named_property(env, objectCtor, "create", &objectCreateFn)) \
		NAPI_STATUS_THROWS_VOID(::napi_get_named_property(env, global, "Error", &errorCtor)) \
		NAPI_STATUS_THROWS_VOID(::napi_get_prototype(env, errorCtor, &errorProto)) \
		napi_value createArgs[1] = { errorProto }; \
		NAPI_STATUS_THROWS_VOID(::napi_call_function(env, objectCtor, objectCreateFn, 1, createArgs, &error)) \
		NAPI_STATUS_THROWS_VOID(::napi_create_string_utf8(env, errorStr.c_str(), errorStr.size(), &errorMsg)) \
		NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, error, "message", errorMsg)) \
	}

#endif
