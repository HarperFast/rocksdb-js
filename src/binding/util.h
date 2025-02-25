#ifndef __UTIL_H__
#define __UTIL_H__

#include "binding.h"
#include "macros.h"

namespace rocksdb_js {

static napi_status getValue(napi_env env, napi_value value, bool& result) {
	return ::napi_get_value_bool(env, value, &result);
}

static napi_status getValue(napi_env env, napi_value value, uint32_t& result) {
	return ::napi_get_value_uint32(env, value, &result);
}

static napi_status getValue(napi_env env, napi_value value, int32_t& result) {
	return ::napi_get_value_int32(env, value, &result);
}

static napi_status getValue(napi_env env, napi_value value, int64_t& result) {
	return ::napi_get_value_int64(env, value, &result);
}

static napi_status getValue(napi_env env, napi_value value, uint64_t& result) {
	int64_t result2;
	CALL_NAPI_FUNCTION_RETURN_STATUS(::napi_get_value_int64(env, value, &result2));
	result = static_cast<uint64_t>(result2);
	return napi_ok;
}

template <typename T>
static napi_status getValue(napi_env env, napi_value value, std::optional<T>& result) {
	result = T{};
	return getValue(env, value, *result);
}

template <typename T>
static napi_status getProperty(napi_env env, napi_value obj, const char* prop, T& result, bool required = false) {
	napi_valuetype objType;
	CALL_NAPI_FUNCTION_RETURN_STATUS(::napi_typeof(env, obj, &objType));

	if (objType == napi_undefined || objType == napi_null) {
		return required ? napi_invalid_arg : napi_ok;
	}

	if (objType != napi_object) {
		return napi_invalid_arg;
	}

	bool has = false;
	CALL_NAPI_FUNCTION_RETURN_STATUS(::napi_has_named_property(env, obj, prop, &has));

	if (!has) {
		return required ? napi_invalid_arg : napi_ok;
	}

	napi_value value;
	CALL_NAPI_FUNCTION_RETURN_STATUS(::napi_get_named_property(env, obj, prop, &value));

	napi_valuetype valueType;
	CALL_NAPI_FUNCTION_RETURN_STATUS(::napi_typeof(env, value, &valueType));

	if (valueType == napi_null || valueType == napi_undefined) {
		return required ? napi_invalid_arg : napi_ok;
	}

	return getValue(env, value, result);
}

}

#endif