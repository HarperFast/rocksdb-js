#ifndef __TXN_WRAP_H__
#define __TXN_WRAP_H__

#include <node_api.h>

namespace rocksdb_js::txn_wrap {

void init(napi_env env, napi_value exports);

} // namespace rocksdb_js::txn_wrap

#endif