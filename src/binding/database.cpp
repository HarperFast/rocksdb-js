#include "database.h"

namespace rocksdb_js {

Database::Database(const Napi::CallbackInfo& info) :
	Napi::ObjectWrap<Database>(info),
	db(nullptr)
{
	//
}

Napi::Object Database::Init(Napi::Env env, Napi::Object exports) {
	Napi::Function func = DefineClass(env, "Database", {
		InstanceMethod<&Database::get>("get"),
		InstanceMethod<&Database::open>("open"),
		InstanceMethod<&Database::put>("put")
	});

	Napi::FunctionReference* constructor = new Napi::FunctionReference();
	*constructor = Napi::Persistent(func);
	exports.Set("Database", func);
	env.SetInstanceData<Napi::FunctionReference>(constructor);
	return exports;
}

Database::~Database() {
	if (this->db) {
		delete this->db;
		this->db = nullptr;
	}
}

Napi::Value Database::get(const Napi::CallbackInfo& info) {
	Napi::Env env = info.Env();
	//
	
	return env.Undefined();	
}

Napi::Value Database::open(const Napi::CallbackInfo& info) {
	Napi::Env env = info.Env();
	rocksdb::Options options;
	rocksdb::Status status = rocksdb::DB::Open(rocksdb::Options(), "test", &this->db);
	
	return env.Undefined();
}

Napi::Value Database::put(const Napi::CallbackInfo& info) {
	Napi::Env env = info.Env();
	//
	
	return env.Undefined();
}

} // namespace rocksdb_js
