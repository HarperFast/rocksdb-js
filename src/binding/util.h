#ifndef __UTIL_H__
#define __UTIL_H__

#include "binding.h"
#include "macros.h"
#include <optional>

/**
 * This file contains various napi helper functions.
 *
 * Note: These function must go in a header file because the compiler doesn't
 * know the data type sizes until link time.
 */

namespace rocksdb_js {

[[maybe_unused]] static napi_status getString(napi_env env, napi_value from, std::string& to) {
	napi_valuetype type;
	NAPI_STATUS_RETURN(::napi_typeof(env, from, &type));

	if (type == napi_string) {
		size_t length = 0;
		NAPI_STATUS_RETURN(napi_get_value_string_utf8(env, from, nullptr, 0, &length));
		to.resize(length, '\0');
		NAPI_STATUS_RETURN(napi_get_value_string_utf8(env, from, &to[0], length + 1, &length));
	} else {
		bool isBuffer;
		NAPI_STATUS_RETURN(napi_is_buffer(env, from, &isBuffer));

		if (isBuffer) {
			char* buf = nullptr;
			size_t length = 0;
			NAPI_STATUS_RETURN(napi_get_buffer_info(env, from, reinterpret_cast<void**>(&buf), &length));
			to.assign(buf, length);
		} else {
			return napi_invalid_arg;
		}
	}

	return napi_ok;
}

[[maybe_unused]] static napi_status getValue(napi_env env, napi_value value, bool& result) {
	return ::napi_get_value_bool(env, value, &result);
}

[[maybe_unused]] static napi_status getValue(napi_env env, napi_value value, int32_t& result) {
	return ::napi_get_value_int32(env, value, &result);
}

[[maybe_unused]] static napi_status getValue(napi_env env, napi_value value, uint32_t& result) {
	return ::napi_get_value_uint32(env, value, &result);
}

[[maybe_unused]] static napi_status getValue(napi_env env, napi_value value, int64_t& result) {
	return ::napi_get_value_int64(env, value, &result);
}

[[maybe_unused]] static napi_status getValue(napi_env env, napi_value value, uint64_t& result) {
	int64_t result2;
	NAPI_STATUS_RETURN(::napi_get_value_int64(env, value, &result2));
	result = static_cast<uint64_t>(result2);
	return napi_ok;
}

[[maybe_unused]] static napi_status getValue(napi_env env, napi_value value, std::string& result) {
	return getString(env, value, result);
}

template <typename T>
[[maybe_unused]] static napi_status getValue(napi_env env, napi_value value, std::optional<T>& result) {
	result = T{};
	return getValue(env, value, *result);
}

template <typename T>
[[maybe_unused]] static napi_status getProperty(napi_env env, napi_value obj, const char* prop, T& result, bool required = false) {
	napi_valuetype objType;
	NAPI_STATUS_RETURN(::napi_typeof(env, obj, &objType));

	if (objType == napi_undefined || objType == napi_null) {
		return required ? napi_invalid_arg : napi_ok;
	}

	if (objType != napi_object) {
		return napi_invalid_arg;
	}

	bool has = false;
	NAPI_STATUS_RETURN(::napi_has_named_property(env, obj, prop, &has));

	if (!has) {
		return required ? napi_invalid_arg : napi_ok;
	}

	napi_value value;
	NAPI_STATUS_RETURN(::napi_get_named_property(env, obj, prop, &value));

	napi_valuetype valueType;
	NAPI_STATUS_RETURN(::napi_typeof(env, value, &valueType));

	if (valueType == napi_null || valueType == napi_undefined) {
		return required ? napi_invalid_arg : napi_ok;
	}

	return getValue(env, value, result);
}

}

#endif