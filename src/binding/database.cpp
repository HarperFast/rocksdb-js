#include "database.h"
#include "macros.h"
#include "util.h"
#include <thread>
#include <unistd.h>
#include <node_api.h>

namespace rocksdb_js {

napi_ref Database::constructor_ref = nullptr;

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
	NAPI_METHOD()
	UNWRAP_DB()

	if (database != nullptr && database->db != nullptr) {
		delete database->db;
		database->db = nullptr;
	}
	
	NAPI_RETURN_UNDEFINED()
}

napi_value Database::Get(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	UNWRAP_DB()
	ASSERT_DB_OPEN(env, database)

	std::string key;
	rocksdb_js::getString(env, argv[0], key);

	std::string value;
	rocksdb::Status status = database->db->Get(rocksdb::ReadOptions(), key, &value);

	if (!status.ok()) {
		::napi_throw_error(env, nullptr, status.ToString().c_str());
	}

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(env, value.c_str(), value.size(), &result))

	return result;
}

/**
 * Initializes the Database class.
 *
 * This function is called when the binding is loaded.
 */
void Database::Init(napi_env env, napi_value exports) {
	napi_property_descriptor properties[] = {
		{ "close", nullptr, Database::Close, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "get", nullptr, Database::Get, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "open", nullptr, Database::Open, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "opened", nullptr, nullptr, Database::IsOpen, nullptr, nullptr, napi_default, nullptr },
		{ "put", nullptr, Database::Put, nullptr, nullptr, nullptr, napi_default, nullptr }
	};

	napi_value cons;
	NAPI_STATUS_THROWS_VOID(::napi_define_class(
		env,
		"Database",             // className
		9,                      // length of class name
		[](napi_env env, napi_callback_info info) -> napi_value {
			NAPI_METHOD_ARGV(1)
			NAPI_GET_STRING(argv[0], path)

			// check if database already exists

			Database* database = new Database(path);

			NAPI_STATUS_THROWS(::napi_wrap(
				env,
				jsThis,
				reinterpret_cast<void*>(database),
				[](napi_env env, void* data, void* hint) {
					delete reinterpret_cast<Database*>(data);
				},
				nullptr,
				nullptr
			))

			return jsThis;
		},                      // constructor as lambda
		nullptr,                // constructor arg
		sizeof(properties) / sizeof(napi_property_descriptor), // number of properties
		properties,             // properties array
		&cons                   // [out] constructor
	))

	NAPI_STATUS_THROWS_VOID(::napi_create_reference(env, cons, 1, &constructor_ref))
	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, "Database", cons))
}

napi_value Database::IsOpen(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_DB()

	if (database == nullptr || database->db == nullptr) {
		napi_value result;
		NAPI_STATUS_THROWS(::napi_get_boolean(env, false, &result))
		return result;
	}

	napi_value result;
	NAPI_STATUS_THROWS(::napi_get_boolean(env, database->db != nullptr, &result))
	return result;
}

/**
 * Opens the RocksDB database. This must be called before any data methods are called.
 */
napi_value Database::Open(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	UNWRAP_DB()

	if (database->db != nullptr) {
		// already open
		NAPI_RETURN_UNDEFINED()
	}

	const napi_value options = argv[0];

	int parallelism = std::max<int>(1, std::thread::hardware_concurrency() / 2);
	rocksdb_js::getProperty(env, options, "parallelism", parallelism);

	std::string name;
	rocksdb_js::getProperty(env, options, "name", name);
	// TODO: if `name` is not set, then use default column family,
	// otherwise create a new column family

	rocksdb::Options dbOptions;
	dbOptions.comparator = rocksdb::BytewiseComparator();
	dbOptions.create_if_missing = true;
	dbOptions.create_missing_column_families = true;
	dbOptions.enable_blob_files = true;
	dbOptions.enable_blob_garbage_collection = true;
	dbOptions.min_blob_size = 1024;
	dbOptions.persist_user_defined_timestamps = true;

	rocksdb::TransactionDBOptions txndbOptions;

	rocksdb::Status status = rocksdb::TransactionDB::Open(dbOptions, txndbOptions, database->path, &database->db);
	if (!status.ok()) {
		::napi_throw_error(env, nullptr, status.ToString().c_str());
	}

	if (!name.empty()) {
		rocksdb::ColumnFamilyHandle* cfHandle;
		status = database->db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), name, &cfHandle);
		if (!status.ok()) {
			::napi_throw_error(env, nullptr, status.ToString().c_str());
		}
	}

	NAPI_RETURN_UNDEFINED()
}

napi_value Database::Put(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	NAPI_GET_STRING(argv[0], key)
	NAPI_GET_STRING(argv[1], value)
	UNWRAP_DB()

	rocksdb::Status status = database->db->Put(rocksdb::WriteOptions(), key, value);

	if (!status.ok()) {
		::napi_throw_error(env, nullptr, status.ToString().c_str());
	}

	NAPI_RETURN_UNDEFINED()
}

} // namespace rocksdb_js
