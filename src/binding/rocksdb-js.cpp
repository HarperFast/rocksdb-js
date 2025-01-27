#include "rocksdb-js.h"
#include "rocksdb/db.h"

using namespace Napi;
using namespace rocksdb;

NAPI_MODULE_INIT() {
	Value exp = Value::From(env, exports);

	exp.As<Object>().Set("version", String::New(env, GetRocksVersionAsString()));

	return exports;
}
