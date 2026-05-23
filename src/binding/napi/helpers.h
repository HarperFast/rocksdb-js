#ifndef __NAPI_HELPERS_H__
#define __NAPI_HELPERS_H__

#include <optional>
#include <sstream>
#include <string>
#include <type_traits>
#include "core/closable.h"
#include "core/exception.h"
#include "napi/binding.h"
#include "napi/status_macros.h"
#include "rocksdb/db.h"

namespace rocksdb_js {

#define RANGE_CHECK(condition, errorMsg, rval) \
	do { \
		if (condition) { \
			std::stringstream ss; \
			ss << errorMsg; \
			::napi_throw_range_error(env, nullptr, ss.str().c_str()); \
			return rval; \
		} \
	} while (0)

void createJSError(napi_env env, const char* code, const char* message, napi_value& error);

std::shared_ptr<rocksdb::ColumnFamilyHandle> createRocksDBColumnFamily(const std::shared_ptr<rocksdb::DB> db, const std::string& name);

void createRocksDBError(napi_env env, rocksdb::Status status, const char* msg, napi_value& error);

void debugLogNapiValue(napi_env env, napi_value value, uint16_t indent = 0, bool isObject = false);

napi_status getKeyFromProperty(
	napi_env env,
	napi_value obj,
	const char* prop,
	const char* errorMsg,
	const char*& keyStr,
	uint32_t& start,
	uint32_t& end
);

const char* getNapiBufferFromArg(
	napi_env env,
	napi_value arg,
	uint32_t& start,
	uint32_t& end,
	size_t& length,
	const char* errorMsg
);

bool getSliceFromArg(napi_env env, napi_value arg, rocksdb::Slice& result, char* defaultBuffer, const char* errorMsg);

std::string getNapiExtendedError(napi_env env, napi_status& status, const char* errorMsg = nullptr);

[[maybe_unused]] static napi_status getString(napi_env env, napi_value from, std::string& to) {
	napi_valuetype type;
	NAPI_STATUS_RETURN(::napi_typeof(env, from, &type));

	if (type == napi_string) {
		size_t length = 0;
		NAPI_STATUS_RETURN(::napi_get_value_string_utf8(env, from, nullptr, 0, &length));
		to.resize(length, '\0');
		NAPI_STATUS_RETURN(::napi_get_value_string_utf8(env, from, &to[0], length + 1, &length));
	} else {
		bool isBuffer;
		NAPI_STATUS_RETURN(::napi_is_buffer(env, from, &isBuffer));

		if (isBuffer) {
			char* buf = nullptr;
			uint32_t start = 0;
			uint32_t end = 0;
			size_t length = 0;
			NAPI_STATUS_RETURN(::napi_get_buffer_info(env, from, reinterpret_cast<void**>(&buf), &length));

			if (buf == nullptr) {
				to.assign("");
				return napi_ok;
			}

			bool hasStart;
			napi_value startValue;
			NAPI_STATUS_RETURN(::napi_has_named_property(env, from, "start", &hasStart));
			if (hasStart) {
				NAPI_STATUS_RETURN(::napi_get_named_property(env, from, "start", &startValue));
				NAPI_STATUS_RETURN(::napi_get_value_uint32(env, startValue, &start));
			}

			bool hasEnd;
			napi_value endValue;
			NAPI_STATUS_RETURN(::napi_has_named_property(env, from, "end", &hasEnd));
			if (hasEnd) {
				NAPI_STATUS_RETURN(::napi_get_named_property(env, from, "end", &endValue));
				NAPI_STATUS_RETURN(::napi_get_value_uint32(env, endValue, &end));
			} else {
				end = length;
			}

			RANGE_CHECK(start > end, "Buffer start greater than end (start=" << start << ", end=" << end << ")", napi_invalid_arg);
			RANGE_CHECK(start > length, "Buffer start greater than length (start=" << start << ", length=" << length << ")", napi_invalid_arg);
			RANGE_CHECK(end > length, "Buffer end greater than length (end=" << end << ", length=" << length << ")", napi_invalid_arg);

			to.assign(buf + start, end - start);
			return napi_ok;
		}

		return napi_invalid_arg;
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

[[maybe_unused]] static napi_status getValue(napi_env env, napi_value value, uint8_t& result) {
	uint32_t tmp;
	NAPI_STATUS_RETURN(::napi_get_value_uint32(env, value, &tmp));
	result = static_cast<uint8_t>(tmp);
	return napi_ok;
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

[[maybe_unused]] static napi_status getValue(napi_env env, napi_value value, float& result) {
	double result2;
	NAPI_STATUS_RETURN(::napi_get_value_double(env, value, &result2));
	result = static_cast<float>(result2);
	return napi_ok;
}

template<typename T = size_t>
[[maybe_unused]] static typename std::enable_if<!std::is_same<T, uint64_t>::value, napi_status>::type
getValue(napi_env env, napi_value value, size_t& result) {
	int64_t result2;
	NAPI_STATUS_RETURN(::napi_get_value_int64(env, value, &result2));
	result = static_cast<size_t>(result2);
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
[[maybe_unused]] static napi_status getProperty(
	napi_env env,
	napi_value obj,
	const char* prop,
	T& result,
	bool required = false
) {
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

} // namespace rocksdb_js

#endif
