#ifndef __TRANSACTION_LOG_VALIDATION_NAPI_H__
#define __TRANSACTION_LOG_VALIDATION_NAPI_H__

#include <node_api.h>

namespace rocksdb_js {

/**
 * Registers the module-level `validateTransactionLog` function on the exports
 * object. It validates a transaction log store directory (framing, headers,
 * txn.state) on a worker thread and does not require an open database, so it
 * works on closed stores and backup snapshots alike.
 */
void initTransactionLogValidationExports(napi_env env, napi_value exports);

} // namespace rocksdb_js

#endif
