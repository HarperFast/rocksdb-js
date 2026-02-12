#ifndef __DB_HANDLE_H__
#define __DB_HANDLE_H__

#include <memory>
#include <vector>
#include <utility>
#include <string>
#include <node_api.h>
#include "rocksdb/db.h"
#include "db_options.h"
#include "transaction_log_store.h"
#include "util.h"

namespace rocksdb_js {

// forward declarations
struct DBDescriptor;
struct UserSharedBufferData;
struct UserSharedBufferFinalizeData;

/**
 * Handle for a RocksDB database and the selected column family. This handle is
 * returned by the Registry and is used by the `Database` class.
 *
 * This handle is for convenience since passing around a shared pointer is a
 * pain.
 */
struct DBHandle final : Closable, AsyncWorkHandle, public std::enable_shared_from_this<DBHandle> {
	/**
	 * The RocksDB database descriptor
	 */
	std::shared_ptr<DBDescriptor> descriptor;

	/**
	 * The RocksDB column family handle.
	 */
	std::shared_ptr<rocksdb::ColumnFamilyHandle> column;

	/**
	 * The path of the database.
	 */
	std::string path;

	/**
	 * Whether to disable WAL.
	 */
	bool disableWAL = false;

	/**
	 * The node environment.
	 */
	napi_env env;

	/**
	 * A reference to the main `rocksdb_js` exports object. This is needed to
	 * get the `TransactionLog` class.
	 */
	napi_ref exportsRef;

	/**
	 * The default transaction log store.
	 */
	std::weak_ptr<TransactionLogStore> defaultLog;

	/**
	 * A map of transaction log store names to `TransactionLog` JavaScript
	 * instances.
	 */
	std::unordered_map<std::string, napi_ref> logRefs;

	/**
	 * The shared default value buffer and its length.
	 */
	char* defaultValueBufferPtr = nullptr;
	size_t defaultValueBufferLength = 0;

	/**
	 * The shared default key buffer and its length.
	 */
	char* defaultKeyBufferPtr = nullptr;
	size_t defaultKeyBufferLength = 0;

	/**
	 * Map of user shared buffers by key.
	 */
	std::unordered_map<std::string, std::shared_ptr<UserSharedBufferData>> userSharedBuffers;

	/**
	 * Mutex to protect the user shared buffers map.
	 */
	std::mutex userSharedBuffersMutex;

	DBHandle(napi_env env, napi_ref exportsRef);
	~DBHandle();

	napi_ref addListener(napi_env env, std::string key, napi_value callback);
	rocksdb::Status clear();
	void close();
	napi_value get(
		napi_env env,
		rocksdb::Slice& key,
		napi_value resolve,
		napi_value reject,
		std::shared_ptr<DBHandle> dbHandleOverride = nullptr
	);

	/**
	 * Creates a new user shared buffer or returns an existing one.
	 *
	 * @param env The environment of the current callback.
	 * @param key The key of the user shared buffer.
	 * @param defaultBuffer The default buffer to use if the user shared buffer does
	 * not exist.
	 * @param callbackRef An optional callback reference to remove the listener when
	 * the user shared buffer is garbage collected.
	 */
	napi_value getUserSharedBuffer(
		napi_env env,
		std::string& key,
		napi_value defaultBuffer,
		napi_ref callbackRef = nullptr
	);

	void open(const std::string& path, const DBOptions& options);
	bool opened() const;
	void unrefLog(const std::string& name);
	napi_value useLog(napi_env env, napi_value jsDatabase, std::string& name);
};

/**
 * Contains the buffer and buffer size for a user shared buffer.
 */
struct UserSharedBufferData final {
	UserSharedBufferData(void* sourceData, size_t size) : size(size) {
		this->data = new char[size];
		::memcpy(this->data, sourceData, size);
	}

	~UserSharedBufferData() {
		delete[] this->data;
	}

	// delete copy constructor and copy assignment to prevent accidental copying
	UserSharedBufferData(const UserSharedBufferData&) = delete;
	UserSharedBufferData& operator=(const UserSharedBufferData&) = delete;

	char* data;
	size_t size;
};

/**
 * Finalize data for user shared buffer ArrayBuffers to clean up map entries
 * when the ArrayBuffer is garbage collected.
 */
struct UserSharedBufferFinalizeData final {
	UserSharedBufferFinalizeData(
		const std::string& k,
		std::weak_ptr<DBHandle> d,
		std::shared_ptr<UserSharedBufferData> data,
		napi_ref callbackRef = nullptr
	) : key(k), dbHandle(d), sharedData(data), callbackRef(callbackRef) {}

	std::string key;
	std::weak_ptr<DBHandle> dbHandle;
	std::shared_ptr<UserSharedBufferData> sharedData;
	napi_ref callbackRef;
};

} // namespace rocksdb_js

#endif
