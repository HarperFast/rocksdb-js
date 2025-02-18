#include "binding.h"
#include "rocksdb/db.h"
#include "database.h"
#include <napi-macros.h>
#include <unordered_map>

NODE_API_MODULE(NODE_GYP_MODULE_NAME, Init)

std:unordered_map<str::string, rocksdb::TransactionDB> openDBs;
NAPI_METHOD(db_open) {
	rocksdb::TransactionDB* db = nullptr;
	rocksdb::TransactionDBOptions txndb_options;
	std::string path; // TODO: Get path from arguments
	// TODO:
	//  if open_dbs has the path, set the db pointer to the existing opened database
	// else open the database
	{
		const auto status = rocksdb::TransactionDB::Open(dbOptions, txndb_options, database->location, &db);
		// TODO: set the db into the openDBs map so other threads can access it
	}
	napi_value result;
	napi_create_double(env, (double) (size_t) db, &result); // return the address, so JS can reference it
}
NAPI_METHOD(get_from_cache) {
	NAPI_ARGV(4);
	int64_t handle;
	napi_get_value_int64(env, args[0], &handle);
	ColumnFamilyHandle* column_family = (ColumnFamilyHandle*) handle;
	int key_length;
	napi_get_value_int64(env, args[1], &key_length);
	napi_get_value_int64(env, args[2], &handle);
	Transaction* transaction = (Transaction*) handle;
	rocksdb::ReadOptions read_options;
	read_options.read_tier = rocksdb::kBlockCacheTier; // only read from the block cache tier, otherwise fail
	// TODO: slice the key from the global key buffer using the key_length
	rocksdb::Slice key((void*) keyBuffer, key_length);
	std::string val;
	rocksdb::Status status = transaction->Get(read_options, column_family, key, &val);
	napi_value return_status;
	if (!status.ok()) {
		if (status.IsNotFound()) {
			napi_create_int32(env, -1, &return_status);
			return return_status;
		}
		if (status.IsIncomplete()) {
			// TODO: indicate that we couldn't get the value from the cache, so an async retrieval is needed
			napi_create_int32(env, -2, &return_status);
			return return_status;
		}
		ROCKS_STATUS_THROWS_NAPI(status);
	}
	napi_create_int32(env, val.length, &return_status);
	return return_status;
}
NAPI_METHOD(get_async) {
	// TODO: Start an async worker to get the value from the database (without any cache restriction)
}
NAPI_METHOD(put) {
	NAPI_ARGV(5);
	int64_t handle;
	napi_get_value_int64(env, args[0], &handle);
	ColumnFamilyHandle* column_family = (ColumnFamilyHandle*) handle;
	int key_length;
	napi_get_value_int64(env, args[1], &key_length);
	napi_get_value_int64(env, args[2], &handle);
	Transaction* transaction = (Transaction*) handle;
	napi_get_value_int64(env, args[3], &handle);
	int value_length;
	napi_get_value_int64(env, args[4], &value_length);
	rocksdb::Slice key((void*) keyBuffer, key_length);
	rocksdb::Slice val((void*) handle, value_length);
	rocksdb::WriteOptions writeOptions;
	writeOptions.disableWAL = true;
	writeOptions.sync = false;
	rocksdb::Status status = transaction->Put(write_options, column_family, key, &val);
	// TODO: handle status
}
NAPI_METHOD(start_txn) {
	int64_t handle;
	napi_get_value_int64(env, args[0], &handle);
	rocksdb::TransactionDB* db = (rocksdb::TransactionDB*) handle;
	rocksdb::TransactionOptions txn_options;
	rocksdb::Transaction* txn = db->BeginTransaction(writeOptions, txn_options);
	napi_value result;
	napi_create_double(env, (double) (size_t) txn, &result); // return the address, so JS can reference it
	return result;
}
NAPI_INIT() {
	NAPI_EXPORT_FUNCTION(db_open);
	NAPI_EXPORT_FUNCTION(get_from_cache);
	NAPI_EXPORT_FUNCTION(get_async);
	NAPI_EXPORT_FUNCTION(put);
	NAPI_EXPORT_FUNCTION(start_txn);
}
// TODO: Remove the node-addon-module code
using namespace Napi;

// Replace NAPI_MODULE_INIT() with NODE_API_MODULE
static Napi::Object Init(Napi::Env env, Napi::Object exports) {
	// version
	exports.Set("version", String::New(env, rocksdb::GetRocksVersionAsString()));

	// database
	rocksdb_js::Database::Init(env, exports);

	return exports;
}

