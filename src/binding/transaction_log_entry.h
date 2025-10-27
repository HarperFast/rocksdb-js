#ifndef __TRANSACTION_LOG_ENTRY_H__
#define __TRANSACTION_LOG_ENTRY_H__

#include <memory>
#include <node_api.h>
#include "transaction_log_store.h"

namespace rocksdb_js {

struct TransactionLogStore;

/**
 * A log entry that is pending to be written to the transaction log on commit.
 */
struct TransactionLogEntry final {
	/**
	 * The transaction log store.
	 */
	std::shared_ptr<TransactionLogStore> store;

	/**
	 * The timestamp of the log entry.
	 */
	uint64_t timestamp;

	/**
	 * The data of the log entry.
	 */
	char* data;

	/**
	 * The size of the log entry.
	 */
	size_t size;

	/**
	 * The node environment.
	 */
	napi_env env;

	/**
	 * A reference to the buffer of the log entry.
	 */
	napi_ref bufferRef;

	/**
	 * Creates a new transaction log entry.
	 */
	TransactionLogEntry(
		std::shared_ptr<TransactionLogStore> store,
		uint64_t timestamp,
		char* data,
		size_t size,
		napi_env env,
		napi_ref bufferRef
	) :
		store(store),
		timestamp(timestamp),
		data(data),
		size(size),
		env(env),
		bufferRef(bufferRef)
	{}

	/**
	 * Destroys the transaction log entry.
	 */
	~TransactionLogEntry() {
		// delete the buffer reference to allow the buffer to be GC'd
		if (this->bufferRef != nullptr) {
			::napi_delete_reference(this->env, this->bufferRef);
			this->bufferRef = nullptr;
		}
	}
};

} // namespace rocksdb_js

#endif
