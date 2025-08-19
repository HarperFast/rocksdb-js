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
};

/**
 * A struct to hold the serialized arguments to emit to the listener callbacks.
 */
struct ListenerData final {
	std::string args;

	ListenerData(size_t size) : args(size, '\0') {}
	ListenerData(const ListenerData& other) : args(other.args) {}
};

/**
 * A wrapper for a listener callback that holds the threadsafe callback, env,
 * and callback reference. The callback reference is used to remove the listener
 * callback and the env is used for cleanup.
 */
struct ListenerCallback final {
	/**
	 * The environment of the current callback.
	 */
	napi_env env;

	/**
	 * The threadsafe function of the current callback. This is what is
	 * actually called when the event is emitted.
	 */
	napi_threadsafe_function threadsafeCallback;

	/**
	 * The callback reference of the current callback. This is used to remove
	 * the listener callback.
	 */
	napi_ref callbackRef;

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
			// clean up current resources first
			this->release();

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

	~ListenerCallback() {
		release();
	}

	void release() {
		if (this->callbackRef && this->env) {
			::napi_delete_reference(this->env, this->callbackRef);
			this->callbackRef = nullptr;
		}

		if (this->threadsafeCallback) {
			// ::napi_release_threadsafe_function(this->threadsafeCallback, napi_tsfn_release);
			this->threadsafeCallback = nullptr;
		}
	}
};

} // namespace rocksdb_js

#endif
