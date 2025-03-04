#ifndef __DBI_WRAP_H__
#define __DBI_WRAP_H__

#include <node_api.h>

namespace rocksdb_js::dbi_wrap {

void init(napi_env env, napi_value exports);

} // namespace rocksdb_js::dbi_wrap

#endif