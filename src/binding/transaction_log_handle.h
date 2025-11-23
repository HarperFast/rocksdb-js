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
		std::unique_ptr<char[]> data,
		uint32_t size
	);

	void addEntry(
		uint32_t transactionId,
		char* data,
		uint32_t size,
		napi_env env,
		napi_ref bufferRef
	);

	MemoryMap* getMemoryMap(uint32_t sequenceNumber);
	uint32_t getLogFileSize(uint32_t sequenceNumber);
	PositionHandle* getLastCommittedPosition();
	void close();
	void query();

	std::map<uint32_t, std::unique_ptr<TransactionLogFile>>* getSequenceFiles();

private:
	/**
	 * Helper struct to hold resolved transaction/store context.
	 */
	struct AddEntryContext {
		std::shared_ptr<DBHandle> dbHandle;
		std::shared_ptr<TransactionHandle> txnHandle;
		std::shared_ptr<TransactionLogStore> store;
	};

	/**
	 * Helper method to resolve and validate transaction/store context.
	 * Shared by both addEntry overloads.
	 */
	AddEntryContext resolveAddEntryContext(uint32_t transactionId);
};

} // namespace rocksdb_js

#endif
