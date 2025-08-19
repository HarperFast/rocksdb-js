#ifndef __DB_HANDLE_H__
#define __DB_HANDLE_H__

#include <memory>
#include <node_api.h>
#include "rocksdb/db.h"
#include "db_descriptor.h"
#include "util.h"

namespace rocksdb_js {

// forward declarations
struct DBDescriptor;
struct ListenerCallback;

/**
 * Handle for a RocksDB database and the selected column family. This handle is
 * returned by the Registry and is used by the `Database` class.
 *
 * This handle is for convenience since passing around a shared pointer is a
 * pain.
 */
struct DBHandle final : Closable, AsyncWorkHandle {
	DBHandle();
	DBHandle(std::shared_ptr<DBDescriptor> descriptor);
	~DBHandle();

	rocksdb::Status clear(uint32_t batchSize, uint64_t& deleted);
	void close();
	napi_value get(
		napi_env env,
		rocksdb::Slice& key,
		napi_value resolve,
		napi_value reject,
		std::shared_ptr<DBHandle> dbHandleOverride = nullptr
	);
	void open(const std::string& path, const DBOptions& options);
	bool opened() const;

	void addListener(napi_env env, std::string key, napi_value callback);
	napi_value removeListener(napi_env env, std::string key, napi_value callback);
	napi_value emit(napi_env env, std::string key, napi_value args);

	/**
	 * The RocksDB database descriptor
	 */
	std::shared_ptr<DBDescriptor> descriptor;

	/**
	 * The RocksDB column family handle.
	 */
	std::shared_ptr<rocksdb::ColumnFamilyHandle> column;

	/**
	 * Map of listeners by key.
	 */
	std::unordered_map<std::string, std::vector<ListenerCallback>> listeners;

	/**
	 * Mutex to protect the listeners map.
	 */
	std::mutex listenersMutex;
};

/**
 * A struct to hold the serialized arguments to emit to the listener callbacks.
 */
struct ListenerData final {
	std::string args;

	ListenerData(size_t size) : args(size, '\0') {}
};

/**
 * A wrapper for a listener callback that holds the threadsafe callback, env,
 * and callback reference. The callback reference is used to remove the listener
 * callback and the env is used for cleanup.
 */
struct ListenerCallback final {
    ListenerCallback(napi_env env, napi_threadsafe_function tsfn, napi_ref callbackRef)
        : env(env), threadsafeCallback(tsfn), callbackRef(callbackRef) {}

	// move constructor
	ListenerCallback(ListenerCallback&& other) noexcept
		: env(other.env), threadsafeCallback(other.threadsafeCallback), callbackRef(other.callbackRef) {
		other.env = nullptr;
		other.threadsafeCallback = nullptr;
		other.callbackRef = 0;
	}

	// move assignment operator
	ListenerCallback& operator=(ListenerCallback&& other) noexcept {
		if (this != &other) {
			// Clean up current resources first
			release();

			// transfer ownership
			env = other.env;
			threadsafeCallback = other.threadsafeCallback;
			callbackRef = other.callbackRef;

			// invalidate source
			other.env = nullptr;
			other.threadsafeCallback = nullptr;
			other.callbackRef = nullptr;
		}
		return *this;
	}

	// delete copy constructor and copy assignment to prevent accidental copying
	ListenerCallback(const ListenerCallback&) = delete;
	ListenerCallback& operator=(const ListenerCallback&) = delete;

	void release() {
		DEBUG_LOG("%p ListenerCallback::release 1\n", this)
		if (this->callbackRef && this->env) {
			::napi_delete_reference(this->env, this->callbackRef);
			this->callbackRef = nullptr;
		}
		DEBUG_LOG("%p ListenerCallback::release 2\n", this)
		if (this->threadsafeCallback) {
			DEBUG_LOG("%p ListenerCallback::release Releasing threadsafe callback %p\n", this, this->threadsafeCallback)
			napi_status status = ::napi_release_threadsafe_function(this->threadsafeCallback, napi_tsfn_release);
			if (status != napi_ok) {
				DEBUG_LOG("%p ListenerCallback::release failed to release threadsafe function (status=%d)\n", this, status);
			}
			this->threadsafeCallback = nullptr;
		}
		DEBUG_LOG("%p ListenerCallback::release 3\n", this)
	}

	~ListenerCallback() {
		DEBUG_LOG("%p ListenerCallback::~ListenerCallback\n", this)
		release();
	}

	napi_env env;
    napi_threadsafe_function threadsafeCallback;
	napi_ref callbackRef;
};

} // namespace rocksdb_js

#endif
