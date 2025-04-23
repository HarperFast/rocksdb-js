#ifndef __MACROS_H__
#define __MACROS_H__

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
			std::string errorStr = rocksdb_js::getNapiExtendedError(env, status); \
			::napi_throw_error(env, nullptr, errorStr.c_str()); \
			return rval; \
		} \
	}

#define NAPI_STATUS_THROWS_VOID(call) \
	{ \
		napi_status status = (call); \
		if (status != napi_ok) { \
			std::string errorStr = rocksdb_js::getNapiExtendedError(env, status); \
			::napi_throw_error(env, nullptr, errorStr.c_str()); \
			return; \
		} \
	}

#define NAPI_STATUS_THROWS(call) \
	NAPI_STATUS_THROWS_RVAL(call, nullptr)

#define NAPI_STATUS_THROWS_ERROR_RVAL(call, rval, errorMsg) \
	{ \
		napi_status status = (call); \
		if (status != napi_ok) { \
			std::string errorStr = rocksdb_js::getNapiExtendedError(env, status, errorMsg); \
			::napi_throw_error(env, nullptr, errorStr.c_str()); \
			return rval; \
		} \
	}

#define NAPI_STATUS_THROWS_ERROR(call, errorMsg) \
	NAPI_STATUS_THROWS_ERROR_RVAL(call, nullptr, errorMsg)

#define NAPI_STATUS_THROWS_RUNTIME_ERROR(call) \
	{ \
		napi_status status = (call); \
		if (status != napi_ok) { \
			std::string errorStr = rocksdb_js::getNapiExtendedError(env, status); \
			throw std::runtime_error(errorStr); \
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

#define NAPI_GET_BUFFER(from, to, errorMsg) \
	char* to = nullptr; \
	uint32_t to##Start = 0; \
	uint32_t to##End = 0; \
	size_t to##Length = 0; \
	{ \
		bool isBuffer; \
		NAPI_STATUS_THROWS(::napi_is_buffer(env, from, &isBuffer)); \
		if (!isBuffer) { \
			::napi_throw_error(env, nullptr, errorMsg); \
			return nullptr; \
		} \
		NAPI_STATUS_THROWS(::napi_get_buffer_info(env, from, reinterpret_cast<void**>(&to), &to##Length)); \
		bool hasStart; \
		napi_value start; \
		NAPI_STATUS_THROWS(::napi_has_named_property(env, from, "start", &hasStart)); \
		if (hasStart) { \
			NAPI_STATUS_THROWS(::napi_get_named_property(env, from, "start", &start)); \
			NAPI_STATUS_THROWS(::napi_get_value_uint32(env, start, &to##Start)); \
		} else { \
			to##Start = 0; \
		} \
		bool hasEnd; \
		napi_value end; \
		NAPI_STATUS_THROWS(::napi_has_named_property(env, from, "end", &hasEnd)); \
		if (hasEnd) { \
			NAPI_STATUS_THROWS(::napi_get_named_property(env, from, "end", &end)); \
			NAPI_STATUS_THROWS(::napi_get_value_uint32(env, end, &to##End)); \
		} else { \
			to##End = to##Length; \
		} \
	}

#define NAPI_GET_STRING(from, to, errorMsg) \
	std::string to; \
	NAPI_STATUS_THROWS_ERROR(rocksdb_js::getString(env, from, to), errorMsg)

#define ROCKSDB_STATUS_FORMAT_ERROR(status, msg) \
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
		ROCKSDB_STATUS_FORMAT_ERROR(status, msg) \
		napi_value errorMsg; \
		NAPI_STATUS_THROWS(::napi_create_string_utf8(env, errorStr.c_str(), errorStr.size(), &errorMsg)) \
		NAPI_STATUS_THROWS(::napi_create_error(env, nullptr, errorMsg, &error)) \
	}

#define ROCKSDB_STATUS_THROWS_ERROR_LIKE(call, msg) \
	{ \
		rocksdb::Status status = (call); \
		if (!status.ok()) { \
			napi_value error; \
			rocksdb_js::createRocksDBError(env, status, msg, error); \
			::napi_throw(env, error); \
		} \
	}

#define ROCKSDB_CREATE_ERROR_LIKE_VOID(error, status, msg) \
	{ \
		ROCKSDB_STATUS_FORMAT_ERROR(status, msg) \
		rocksdb_js::createRocksDBError(env, status, msg, error); \
	}

#endif