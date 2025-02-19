#ifndef __MACROS_H__
#define __MACROS_H__

#include <string>
#include <cstring>

#define ARG_GET_UTF8_STRING(variable_name, arg) \
	size_t variable_name##_length; \
	napi_get_value_string_utf8(env, arg, nullptr, 0, &variable_name##_length); \
	char* variable_name##_cstr = new char[variable_name##_length + 1]; \
	napi_get_value_string_utf8(env, arg, variable_name##_cstr, variable_name##_length + 1, &variable_name##_length); \
	std::string variable_name(variable_name##_cstr); \
	delete[] variable_name##_cstr;

#define RETURN_UNDEFINED() \
	napi_value undefined; \
	napi_get_undefined(env, &undefined); \
	return undefined;

#define CALL_NAPI_FUNCTION(fn) \
	{ \
		napi_status status = fn; \
		if (status != napi_ok) { \
			const napi_extended_error_info* error; \
			::napi_get_last_error_info(env, &error); \
			char msg[1024]; \
			::snprintf(msg, 1024, "%s (status=%d) ", error->error_message, status); \
			::napi_throw_error(env, nullptr, msg); \
		} \
	}

#endif
