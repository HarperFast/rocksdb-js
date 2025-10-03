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

    std::shared_ptr<DBHandle> dbHandle;
    std::shared_ptr<TransactionLogStore> store;

    void addEntry(uint64_t timestamp, char* logEntry, size_t logEntryLength);

    void close();
};

} // namespace rocksdb_js

#endif
