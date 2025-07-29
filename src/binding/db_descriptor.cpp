#include "db_descriptor.h"

namespace rocksdb_js {

/**
 * Creates a new database descriptor.
 */
DBDescriptor::DBDescriptor(
	std::string path,
	DBMode mode,
	std::shared_ptr<rocksdb::DB> db,
	std::unordered_map<std::string, std::shared_ptr<rocksdb::ColumnFamilyHandle>> columns
):
	path(path),
	mode(mode),
	db(db)
{
	for (auto& column : columns) {
		this->columns[column.first] = column.second;
	}
}

/**
 * Destroy the database descriptor and any resources associated to it
 * (transactions, iterators, etc).
 */
DBDescriptor::~DBDescriptor() {
	DEBUG_LOG("%p DBDescriptor::~DBDescriptor() %ld closables\n", this, this->closables.size())

	if (this->closables.size()) {
		while (!this->closables.empty()) {
			Closable* handle = *this->closables.begin();
			DEBUG_LOG("%p DBDescriptor::~DBDescriptor() closing closable %p\n", this, handle)
			this->closables.erase(handle);
			handle->close();
		}
	}

	// Clear everything else after all closables are done
	{
		std::lock_guard<std::mutex> lock(this->mutex);
		this->transactions.clear();
		this->columns.clear();
		this->db.reset();
	}
}

/**
 * Registers a database resource to be closed when the descriptor is closed.
 */
void DBDescriptor::attach(Closable* closable) {
	std::lock_guard<std::mutex> lock(this->mutex);
	this->closables.insert(closable);
}

/**
 * Unregisters a database resource from being closed when the descriptor is
 * closed.
 */
void DBDescriptor::detach(Closable* closable) {
	std::lock_guard<std::mutex> lock(this->mutex);
	this->closables.erase(closable);
}

/**
 * Enqueues a callback to be called when a lock is acquired.
 */
void DBDescriptor::lockEnqueueCallback(
	napi_env env,
	std::string key,
	napi_value callback,
	std::shared_ptr<DBHandle> owner,
	bool* isNewLock,
	bool skipEnqueueIfExists
) {
	std::lock_guard<std::mutex> lock(this->locksMutex);
	auto lockHandle = this->locks.find(key);

	if (lockHandle == this->locks.end()) {
		// no lock found
		auto newLockHandle = std::make_shared<LockHandle>(owner);
		this->locks.emplace(key, newLockHandle);
		lockHandle = this->locks.find(key);
		if (isNewLock != nullptr) {
			*isNewLock = true;
		}
		if (skipEnqueueIfExists) {
			DEBUG_LOG("%p DBDescriptor::lockEnqueueCallback() skipping enqueue because lock already exists\n", this)
			return;
		}
	}

	// lock found
	napi_valuetype type;
	NAPI_STATUS_THROWS_VOID(::napi_typeof(env, callback, &type))
	if (type == napi_function) {
		napi_value resource_name;
		NAPI_STATUS_THROWS_VOID(::napi_create_string_latin1(
			env,
			"rocksdb-js.lock",
			NAPI_AUTO_LENGTH,
			&resource_name
		))

		napi_threadsafe_function threadsafeCallback;
		NAPI_STATUS_THROWS_VOID(::napi_create_threadsafe_function(
			env,           // env
			callback,      // func
			nullptr,       // async_resource
			resource_name, // async_resource_name
			0,             // max_queue_size
			1,             // initial_thread_count
			nullptr,       // thread_finalize_data
			nullptr,       // thread_finalize_callback
			nullptr,       // context
			nullptr,       // call_js_cb
			&threadsafeCallback // [out] callback
		))

		DEBUG_LOG("%p DBDescriptor::lockEnqueueCallback() enqueuing onUnlocked callback\n", this)
		NAPI_STATUS_THROWS_VOID(::napi_unref_threadsafe_function(env, threadsafeCallback))
		lockHandle->second->callbacks.push(threadsafeCallback);
	}
}

bool DBDescriptor::lockExists(std::string key) {
	std::lock_guard<std::mutex> lock(this->locksMutex);
	auto lockHandle = this->locks.find(key);
	bool exists = lockHandle != this->locks.end();
	DEBUG_LOG("%p DBDescriptor::lockExists() %s lock for key %s\n", this, exists ? "found" : "not found", key.c_str())
	return exists;
}

bool DBDescriptor::lockRelease(std::string key) {
	std::queue<napi_threadsafe_function> callbacks;

	{
		std::lock_guard<std::mutex> lock(this->locksMutex);
		auto lockHandle = this->locks.find(key);

		if (lockHandle == this->locks.end()) {
			// no lock found
			DEBUG_LOG("%p DBDescriptor::lockRelease() no lock found\n", this)
			return false;
		}

		// lock found, remove it
		callbacks = std::move(lockHandle->second->callbacks);
		DEBUG_LOG("%p DBDescriptor::lockRelease() removing lock\n", this)
		this->locks.erase(key);
	}

	DEBUG_LOG("%p DBDescriptor::lockRelease() calling %zu unlock callbacks\n", this, callbacks.size())

	// call the callbacks in order, but stop if any callback fails
	while (!callbacks.empty()) {
		auto callback = callbacks.front();
		callbacks.pop();
		DEBUG_LOG("%p DBDescriptor::lockRelease() calling callback %p\n", this, callback)
		napi_status status = ::napi_call_threadsafe_function(callback, nullptr, napi_tsfn_blocking);
		if (status == napi_closing) {
			continue;
		}
		::napi_release_threadsafe_function(callback, napi_tsfn_release);
	}

	return true;
}

/**
 * Adds a transaction to the registry.
 */
void DBDescriptor::transactionAdd(std::shared_ptr<TransactionHandle> txnHandle) {
	uint32_t id = txnHandle->id;
	std::lock_guard<std::mutex> lock(this->mutex);
	this->transactions[id] = txnHandle;
	this->closables.insert(txnHandle.get());
}

/**
 * Retrieves a transaction from the registry.
 */
std::shared_ptr<TransactionHandle> DBDescriptor::transactionGet(uint32_t id) {
	std::lock_guard<std::mutex> lock(this->mutex);
	return this->transactions[id];
}

/**
 * Removes a transaction from the registry.
 */
void DBDescriptor::transactionRemove(uint32_t id) {
	std::lock_guard<std::mutex> lock(this->mutex);
	this->transactions.erase(id);
}

} // namespace rocksdb_js
