#ifndef __TRANSACTION_LOG_HANDLE_H__
#define __TRANSACTION_LOG_HANDLE_H__

#include <memory>
#include <string>
#include "db_handle.h"
#include "transaction_log_store.h"

namespace rocksdb_js {

struct TransactionLogHandle final : Closable {
    /**
     * The database handle.
     */
    std::weak_ptr<DBHandle> dbHandle;

    /**
     * The transaction log store.
     */
    std::weak_ptr<TransactionLogStore> store;

    /**
     * The name of the transaction log store.
     */
    std::string logName;

    /**
     * The transaction id.
     */
    uint32_t transactionId;

    TransactionLogHandle(const std::shared_ptr<DBHandle>& dbHandle, const std::string& logName);
    ~TransactionLogHandle();

    void addEntry(
        uint32_t transactionId,
        char* data,
        size_t size,
        napi_env env,
        napi_ref bufferRef
    );
    void close();
    void query();
};

} // namespace rocksdb_js

#endif
