#ifndef __NAPI_STATUS_MACROS_H__
#define __NAPI_STATUS_MACROS_H__

#include <string>
#include "core/exception.h"
#include "napi/binding.h"

namespace rocksdb_js {
std::string getNapiExtendedError(napi_env env, napi_status& status, const char* errorMsg);
}

#define NAPI_STATUS_RETURN(call) \
	do { \
		napi_status status = (call); \
		if (status != napi_ok) { \
			return status; \
		} \
	} while (0)

#define NAPI_STATUS_THROWS_RVAL(call, rval) \
	do { \
		napi_status status = (call); \
		if (status != napi_ok) { \
			std::string errorStr = rocksdb_js::getNapiExtendedError(env, status, nullptr); \
			::napi_throw_error(env, nullptr, errorStr.c_str()); \
			return rval; \
		} \
	} while (0)

#define NAPI_STATUS_THROWS_VOID(call) \
	NAPI_STATUS_THROWS_ERROR_VOID(call, nullptr)

#define NAPI_STATUS_THROWS_ERROR_VOID(call, errorMsg) \
	do { \
		napi_status status = (call); \
		if (status != napi_ok) { \
			std::string errorStr = rocksdb_js::getNapiExtendedError(env, status, errorMsg); \
			::napi_throw_error(env, nullptr, errorStr.c_str()); \
			return; \
		} \
	} while (0)

#define NAPI_STATUS_THROWS(call) \
	NAPI_STATUS_THROWS_RVAL(call, nullptr)

#define NAPI_STATUS_THROWS_ERROR_RVAL(call, rval, errorMsg) \
	do { \
		napi_status status = (call); \
		if (status != napi_ok) { \
			std::string errorStr = rocksdb_js::getNapiExtendedError(env, status, errorMsg); \
			::napi_throw_error(env, nullptr, errorStr.c_str()); \
			return rval; \
		} \
	} while (0)

#define NAPI_STATUS_THROWS_ERROR(call, errorMsg) \
	NAPI_STATUS_THROWS_ERROR_RVAL(call, nullptr, errorMsg)

#define NAPI_STATUS_THROWS_RUNTIME_ERROR(call) \
	do { \
		napi_status status = (call); \
		if (status != napi_ok) { \
			std::string errorStr = rocksdb_js::getNapiExtendedError(env, status, nullptr); \
			throw rocksdb_js::DBException(std::move(errorStr)); \
		} \
	} while (0)

#endif
