#ifndef __DB_WRAP_H__
#define __DB_WRAP_H__

#include <node_api.h>

namespace rocksdb_js::db_wrap {

void init(napi_env env, napi_value exports);

} // namespace rocksdb_js::db_wrap

#endif