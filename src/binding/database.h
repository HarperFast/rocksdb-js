#ifndef __DATABASE_H__
#define __DATABASE_H__

#include "rocksdb/db.h"
#include <napi.h>

namespace rocksdb_js {

class Database : public Napi::ObjectWrap<Database> {
public:
	static Napi::Object Init(Napi::Env env, Napi::Object exports);

	Database(const Napi::CallbackInfo& info);
	~Database();

	Napi::Value get(const Napi::CallbackInfo& info);
	Napi::Value open(const Napi::CallbackInfo& info);
	Napi::Value put(const Napi::CallbackInfo& info);

private:
	rocksdb::DB* db;
};

} // namespace rocksdb_js

#endif