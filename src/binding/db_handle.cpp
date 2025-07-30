#include "db_handle.h"
#include "db_registry.h"

namespace rocksdb_js {

/**
 * Creates a new DBHandle.
 */
DBHandle::DBHandle()
	: descriptor(nullptr) {}

/**
 * Creates a new DBHandle from a DBDescriptor.
 */
DBHandle::DBHandle(std::shared_ptr<DBDescriptor> descriptor)
	: descriptor(descriptor) {}

/**
 * Close the DBHandle and destroy it.
 */
DBHandle::~DBHandle() {
	DEBUG_LOG("%p DBHandle::~DBHandle()\n", this)
	this->close();
}

/**
 * Closes the DBHandle.
 */
void DBHandle::close() {
	DEBUG_LOG("%p DBHandle::close() dbDescriptor=%p\n", this, this->descriptor.get())

	this->releaseLocks();

	if (this->column) {
		this->column.reset();
	}

	if (this->descriptor) {
		this->descriptor.reset();
	}

	// purge all weak references in the registry
	DBRegistry::Purge();
}

/**
 * Has the DBRegistry open a RocksDB database and then move it's handle properties
 * to this DBHandle.
 *
 * @param path - The filesystem path to the database.
 * @param options - The options for the database.
 */
void DBHandle::open(const std::string& path, const DBOptions& options) {
	auto handle = DBRegistry::OpenDB(path, options);
	this->column = std::move(handle->column);
	this->descriptor = std::move(handle->descriptor);
	DEBUG_LOG("%p DBHandle::open dbhandle %p is no longer needed\n", this, handle.get())
}

/**
 * Release all locks created by this handle.
 */
void DBHandle::releaseLocks() {
	if (!this->descriptor) {
		return;
	}

	std::set<napi_threadsafe_function> callbacks;

	{
		std::lock_guard<std::mutex> lock(this->descriptor->locksMutex);
		DEBUG_LOG("%p DBHandle::close() checking %d locks if they are owned by this handle\n", this, this->descriptor->locks.size())
		for (auto it = this->descriptor->locks.begin(); it != this->descriptor->locks.end();) {
			auto owner = it->second->owner.lock();
			if (!owner || owner.get() == this) {
				DEBUG_LOG("%p DBHandle::close() found lock %p with %d callbacks\n", this, it->second.get(), it->second->threadsafeCallbacks.size())
				// move all callbacks from the queue
				while (!it->second->threadsafeCallbacks.empty()) {
					callbacks.insert(it->second->threadsafeCallbacks.front());
					it->second->threadsafeCallbacks.pop();
				}
				it = this->descriptor->locks.erase(it);
			} else {
				++it;
			}
		}
	}

	DEBUG_LOG("%p DBHandle::close() calling %zu unlock callbacks\n", this, callbacks.size())

	// call the callbacks in order, but stop if any callback fails
	for (auto& callback : callbacks) {
		napi_status status = ::napi_call_threadsafe_function(callback, nullptr, napi_tsfn_blocking);
		if (status == napi_closing) {
			continue;
		}
		::napi_release_threadsafe_function(callback, napi_tsfn_release);
	}
}

/**
 * Checks if the referenced database is opened.
 */
bool DBHandle::opened() const {
	if (this->descriptor && this->descriptor->db) {
		return true;
	}
	return false;
}

} // namespace rocksdb_js
