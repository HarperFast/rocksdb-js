#ifndef __TRANSACTION_LOG_HANDLE_H__
#define __TRANSACTION_LOG_HANDLE_H__

#include <memory>
#include <string>
#include "db_handle.h"
#include "transaction_log_store.h"

namespace rocksdb_js {

struct TransactionLogHandle final : Closable {
    TransactionLogHandle(const std::shared_ptr<DBHandle>& dbHandle, const std::string& logName);
    ~TransactionLogHandle();

    std::weak_ptr<DBHandle> dbHandle;
    std::weak_ptr<TransactionLogStore> store;
    std::string logName;

    void addEntry(uint64_t timestamp, char* data, size_t size);
    void close();
    void query();
};

} // namespace rocksdb_js

#endif
