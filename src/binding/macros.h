#ifndef __MACROS_H__
#define __MACROS_H__

#include <string>
#include <cstring>

#define NAPI_STATUS_RETURN(call) \
	{ \
		napi_status status = (call); \
		if (status != napi_ok) { \
			return status; \
		} \
	}

#define NAPI_STATUS_THROWS(call) \
	{ \
		napi_status status = (call); \
		if (status != napi_ok) { \
			const napi_extended_error_info* error; \
			::napi_get_last_error_info(env, &error); \
			::napi_throw_error(env, nullptr, error->error_message); \
			return nullptr; \
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

#define NAPI_METHOD() \
	napi_value jsThis; \
	NAPI_STATUS_THROWS(::napi_get_cb_info(env, info, nullptr, nullptr, &jsThis, nullptr))

#define NAPI_METHOD_ARGV(n) \
	napi_value argv[n]; \
	size_t argc = n; \
	napi_value jsThis; \
	NAPI_STATUS_THROWS(::napi_get_cb_info(env, info, &argc, argv, &jsThis, nullptr))

#define ASSERT_DB_INITIALIZED(env, database) \
	if (database == nullptr) { \
		::napi_throw_error(env, nullptr, "Database not initialized"); \
		NAPI_RETURN_UNDEFINED() \
	} \

#define ASSERT_DB_OPEN(env, database) \
	ASSERT_DB_INITIALIZED(env, database) \
	if (database->db == nullptr) { \
		::napi_throw_error(env, nullptr, "Database not open"); \
		NAPI_RETURN_UNDEFINED() \
	}

#define UNWRAP_DB() \
    std::shared_ptr<Database>* database_ptr; \
    NAPI_STATUS_THROWS(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&database_ptr))) \
    auto& database = *database_ptr;

#define NAPI_GET_STRING(from, to) \
	std::string to; \
	rocksdb_js::getString(env, from, to);

#endif
