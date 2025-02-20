#include "database.h"
#include "macros.h"
#include <unistd.h>

namespace rocksdb_js {

Database::Database(const std::string& path) :
	db(nullptr),
	path(path)
{
	//
}

Database::~Database() {
	if (this->db) {
		delete this->db;
		this->db = nullptr;
	}
}

napi_value Database::Close(napi_env env, napi_callback_info info) {
	napi_value jsThis;
	Database* database = nullptr;

	CALL_NAPI_FUNCTION(::napi_get_cb_info(env, info, nullptr, nullptr, &jsThis, nullptr))
	CALL_NAPI_FUNCTION(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&database)))
	
	if (database->db) {
		delete database->db;
		database->db = nullptr;
	}
	
	RETURN_UNDEFINED()
}

napi_value Database::Get(napi_env env, napi_callback_info info) {
	size_t argc = 1;
	napi_value argv[1];
	napi_value jsThis;
	Database* database = nullptr;

	CALL_NAPI_FUNCTION(::napi_get_cb_info(env, info, &argc, argv, &jsThis, nullptr))
	CALL_NAPI_FUNCTION(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&database)))

	ARG_GET_UTF8_STRING(key, argv[0])

	std::string value;
	rocksdb::Status status = database->db->Get(rocksdb::ReadOptions(), key, &value);

	if (!status.ok()) {
		::napi_throw_error(env, nullptr, status.ToString().c_str());
	}

	napi_value result;
	CALL_NAPI_FUNCTION(::napi_create_string_utf8(env, value.c_str(), value.size(), &result))

	return result;
}

void Database::Init(napi_env env, napi_value exports) {
	napi_property_descriptor properties[] = {
		{ "get", nullptr, Database::Get, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "open", nullptr, Database::Open, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "put", nullptr, Database::Put, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "close", nullptr, Database::Close, nullptr, nullptr, nullptr, napi_default, nullptr }
	};

	napi_value cons;
	CALL_NAPI_FUNCTION(::napi_define_class(
		env,
		"Database",             // className
		9,                      // length of class name
		Database::New,          // constructor
		nullptr,                // constructor arg
		4,                      // number of properties
		properties,             // properties array
		&cons                   // [out] constructor
	))

	CALL_NAPI_FUNCTION(::napi_set_named_property(env, exports, "Database", cons))
}

napi_value Database::New(napi_env env, napi_callback_info info) {
	size_t argc = 1;
	napi_value argv[1];
	napi_value jsThis;
	CALL_NAPI_FUNCTION(::napi_get_cb_info(env, info, &argc, argv, &jsThis, nullptr))

	ARG_GET_UTF8_STRING(path, argv[0])

	Database* database = new Database(path);
	CALL_NAPI_FUNCTION(::napi_wrap(
		env,
		jsThis,                                    // js object
		reinterpret_cast<void*>(database),         // native object
		[](napi_env env, void* data, void* hint) { // finalize_cb
			delete reinterpret_cast<Database*>(data);
		}, 
		nullptr,                                   // finalize_hint
		nullptr                                    // [out] result
	))

	return jsThis;
}

napi_value Database::Open(napi_env env, napi_callback_info info) {
	napi_value jsThis;
	Database* database = nullptr;

	CALL_NAPI_FUNCTION(::napi_get_cb_info(env, info, nullptr, nullptr, &jsThis, nullptr))
	CALL_NAPI_FUNCTION(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&database)))

	if (database->db) {
		RETURN_UNDEFINED()
	}

	rocksdb::Options options;
	options.create_if_missing = true;

	rocksdb::Status status = rocksdb::DB::Open(options, database->path, &database->db);

	if (!status.ok()) {
		::napi_throw_error(env, nullptr, status.ToString().c_str());
	}

	RETURN_UNDEFINED()
}

napi_value Database::Put(napi_env env, napi_callback_info info) {
	size_t argc = 2;
	napi_value argv[2];
	napi_value jsThis;
	Database* database = nullptr;

	CALL_NAPI_FUNCTION(::napi_get_cb_info(env, info, &argc, argv, &jsThis, nullptr))
	CALL_NAPI_FUNCTION(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&database)))

	ARG_GET_UTF8_STRING(key, argv[0])
	ARG_GET_UTF8_STRING(value, argv[1])

	rocksdb::Status status = database->db->Put(rocksdb::WriteOptions(), key, value);

	if (!status.ok()) {
		::napi_throw_error(env, nullptr, status.ToString().c_str());
	}

	RETURN_UNDEFINED()
}

} // namespace rocksdb_js
