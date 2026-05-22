#ifndef __NAPI_MACROS_H__
#define __NAPI_MACROS_H__

#include <sstream>
#include <string>
#include "core/debug.h"
#include "core/exception.h"
#include "napi/binding.h"
#include "napi/helpers.h"
#include "napi/status_macros.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"

namespace rocksdb_js {
struct DBHandle;
}

#ifdef DEBUG
	#define DEBUG_LOG_NAPI_VALUE(value) \
		rocksdb_js::debugLogNapiValue(env, value)
#else
	#define DEBUG_LOG_NAPI_VALUE(value) do { ; } while (0)
#endif

#define NAPI_ASSERT_OBJECT_OR_UNDEFINED(obj, errorMsg) \
	do { \
		napi_valuetype objType; \
		NAPI_STATUS_THROWS_RVAL(::napi_typeof(env, obj, &objType), napi_invalid_arg); \
		if (objType != napi_object && objType != napi_undefined && objType != napi_null) { \
			::napi_throw_error(env, nullptr, errorMsg); \
			return napi_invalid_arg; \
		} \
	} while (0)

#define NAPI_RETURN_UNDEFINED() \
	napi_value undefined; \
	napi_get_undefined(env, &undefined); \
	return undefined

#define NAPI_EXPORT_FUNCTION(name) \
	napi_value name##_fn; \
	NAPI_STATUS_THROWS(::napi_create_function(env, NULL, 0, name, NULL, &name##_fn)); \
	NAPI_STATUS_THROWS(::napi_set_named_property(env, exports, #name, name##_fn))

#define NAPI_CHECK_NEW_TARGET(className) \
	do { \
		napi_value newTarget; \
		::napi_get_new_target(env, info, &newTarget); \
		if (newTarget == nullptr) { \
			::napi_throw_error(env, nullptr, className " must be called with 'new'"); \
			return nullptr; \
		} \
	} while (0)

#define NAPI_CONSTRUCTOR(className) \
	NAPI_CHECK_NEW_TARGET(className); \
	napi_value jsThis; \
	NAPI_STATUS_THROWS(::napi_get_cb_info(env, info, nullptr, nullptr, &jsThis, nullptr))

#define NAPI_CONSTRUCTOR_WITH_DATA(className) \
	NAPI_CHECK_NEW_TARGET(className); \
	napi_value jsThis; \
	void* data; \
	NAPI_STATUS_THROWS(::napi_get_cb_info(env, info, nullptr, nullptr, &jsThis, reinterpret_cast<void**>(&data)))

#define NAPI_CONSTRUCTOR_ARGV(className, n) \
	NAPI_CHECK_NEW_TARGET(className); \
	napi_value argv[n]; \
	size_t argc = n; \
	napi_value jsThis; \
	NAPI_STATUS_THROWS(::napi_get_cb_info(env, info, &argc, argv, &jsThis, nullptr))

#define NAPI_CONSTRUCTOR_ARGV_WITH_DATA(className, n) \
	NAPI_CHECK_NEW_TARGET(className); \
	napi_value argv[n]; \
	size_t argc = n; \
	napi_value jsThis; \
	void* data; \
	NAPI_STATUS_THROWS(::napi_get_cb_info(env, info, &argc, argv, &jsThis, reinterpret_cast<void**>(&data)))

#define NAPI_METHOD() \
	napi_value jsThis; \
	NAPI_STATUS_THROWS(::napi_get_cb_info(env, info, nullptr, nullptr, &jsThis, nullptr))

#define NAPI_METHOD_ARGV(n) \
	napi_value argv[n]; \
	size_t argc = n; \
	napi_value jsThis; \
	NAPI_STATUS_THROWS(::napi_get_cb_info(env, info, &argc, argv, &jsThis, nullptr))

#define NAPI_GET_BUFFER(from, to, errorMsg) \
	uint32_t to##Start = 0; \
	uint32_t to##End = 0; \
	size_t to##Length = 0; \
	const char* to = rocksdb_js::getNapiBufferFromArg(env, from, to##Start, to##End, to##Length, errorMsg); \
	if (to == nullptr) { \
		return nullptr; \
	}

#define NAPI_GET_STRING(from, to, errorMsg) \
	std::string to; \
	NAPI_STATUS_THROWS_ERROR(rocksdb_js::getString(env, from, to), errorMsg)

#define NAPI_GET_DB_HANDLE(from, exportsRef, to, errorMsg) \
	std::shared_ptr<DBHandle>* to; \
	do { \
		napi_value exports; \
		NAPI_STATUS_THROWS(::napi_get_reference_value(env, exportsRef, &exports)); \
		napi_value databaseCtor; \
		NAPI_STATUS_THROWS(::napi_get_named_property(env, exports, "Database", &databaseCtor)); \
		bool isDatabase = false; \
		NAPI_STATUS_THROWS(::napi_instanceof(env, from, databaseCtor, &isDatabase)); \
		if (!isDatabase) { \
			::napi_throw_type_error(env, nullptr, "Invalid argument, expected Database instance"); \
			return nullptr; \
		} \
		NAPI_STATUS_THROWS(::napi_unwrap(env, from, reinterpret_cast<void**>(&to))); \
		if (!to || !(*to)) { \
			::napi_throw_type_error(env, nullptr, "Invalid database handle"); \
			return nullptr; \
		} \
	} while (0)

#define ROCKSDB_STATUS_FORMAT_ERROR(status, msg) \
	std::string errorStr; \
	do { \
		std::stringstream ss; \
		if (status.code() == rocksdb::Status::Code::kAborted) { \
			ss << status.ToString(); \
		} else { \
			ss << (msg) << ": " << status.ToString(); \
		} \
		errorStr = ss.str(); \
		if (errorStr.size() > 2 && errorStr.compare(errorStr.size() - 2, 2, ": ") == 0) { \
			errorStr.erase(errorStr.size() - 2); \
		} \
	} while (0)

#define ROCKSDB_STATUS_CREATE_NAPI_ERROR(status, msg) \
	napi_value error; \
	do { \
		ROCKSDB_STATUS_FORMAT_ERROR(status, msg); \
		napi_value errorMsg; \
		NAPI_STATUS_THROWS(::napi_create_string_utf8(env, errorStr.c_str(), errorStr.size(), &errorMsg)); \
		NAPI_STATUS_THROWS(::napi_create_error(env, nullptr, errorMsg, &error)); \
	} while (0)

#define ROCKSDB_STATUS_CREATE_NAPI_ERROR_VOID(status, msg) \
	napi_value error; \
	do { \
		ROCKSDB_STATUS_FORMAT_ERROR(status, msg); \
		napi_value errorMsg; \
		NAPI_STATUS_THROWS_VOID(::napi_create_string_utf8(env, errorStr.c_str(), errorStr.size(), &errorMsg)); \
		NAPI_STATUS_THROWS_VOID(::napi_create_error(env, nullptr, errorMsg, &error)); \
	} while (0)

#define ROCKSDB_STATUS_THROWS_ERROR_LIKE(call, msg) \
	do { \
		rocksdb::Status status = (call); \
		if (!status.ok()) { \
			napi_value error; \
			rocksdb_js::createRocksDBError(env, status, msg, error); \
			::napi_throw(env, error); \
		} \
	} while (0)

#define ROCKSDB_CREATE_ERROR_LIKE_VOID(error, status, msg) \
	do { \
		ROCKSDB_STATUS_FORMAT_ERROR(status, msg); \
		rocksdb_js::createRocksDBError(env, status, msg, error); \
	} while (0)

#define THROW_IF_READONLY(handle, context) \
	do { \
		if ((handle) && (handle)->readOnly) { \
			::napi_throw_error(env, "ERR_DATABASE_READONLY", context "Unsupported operation in read-only mode"); \
			NAPI_RETURN_UNDEFINED(); \
		} \
	} while (0)

#endif
