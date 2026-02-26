#ifndef __DB_REGISTRY_H__
#define __DB_REGISTRY_H__

#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>
#include "db_descriptor.h"
#include "db_handle.h"
#include "transaction.h"

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>

// Windows-compatible mutex wrapper using CRITICAL_SECTION
// This avoids CRT compatibility issues between Node.js and native addon
class WinMutex {
private:
	CRITICAL_SECTION cs;
public:
	WinMutex() { InitializeCriticalSection(&cs); }
	~WinMutex() { DeleteCriticalSection(&cs); }
	void lock() { EnterCriticalSection(&cs); }
	void unlock() { LeaveCriticalSection(&cs); }
	bool try_lock() { return TryEnterCriticalSection(&cs) != 0; }

	WinMutex(const WinMutex&) = delete;
	WinMutex& operator=(const WinMutex&) = delete;
};

template<typename Mutex>
class lock_guard_generic {
private:
	Mutex& m;
public:
	explicit lock_guard_generic(Mutex& mutex) : m(mutex) { m.lock(); }
	~lock_guard_generic() { m.unlock(); }
	lock_guard_generic(const lock_guard_generic&) = delete;
	lock_guard_generic& operator=(const lock_guard_generic&) = delete;
};

template<typename Mutex>
class unique_lock_generic {
private:
	Mutex* m;
	bool owns;
public:
	explicit unique_lock_generic(Mutex& mutex) : m(&mutex), owns(true) { m->lock(); }
	~unique_lock_generic() { if (owns) m->unlock(); }
	void unlock() { if (owns) { m->unlock(); owns = false; } }
	unique_lock_generic(const unique_lock_generic&) = delete;
	unique_lock_generic& operator=(const unique_lock_generic&) = delete;
};

using RegistryMutex = WinMutex;
template<typename T> using registry_lock_guard = lock_guard_generic<T>;
template<typename T> using registry_unique_lock = unique_lock_generic<T>;
#else
using RegistryMutex = std::mutex;
template<typename T> using registry_lock_guard = std::lock_guard<T>;
template<typename T> using registry_unique_lock = std::unique_lock<T>;
#endif

namespace rocksdb_js {

/**
 * Entry in the database registry containing both the descriptor and a condition
 * variable for coordinating access to that specific path.
 */
struct DBRegistryEntry final {
	std::shared_ptr<DBDescriptor> descriptor;
	std::shared_ptr<std::condition_variable> condition;

	// Default constructor
	DBRegistryEntry() : condition(std::make_shared<std::condition_variable>()) {}

	DBRegistryEntry(std::shared_ptr<DBDescriptor> desc)
		: descriptor(std::move(desc)), condition(std::make_shared<std::condition_variable>()) {}
};


struct DBHandleParams final {
	std::shared_ptr<DBDescriptor> descriptor;
	std::shared_ptr<ColumnFamilyDescriptor> columnDescriptor;

	DBHandleParams(std::shared_ptr<DBDescriptor> descriptor, std::shared_ptr<ColumnFamilyDescriptor> columnDescriptor)
		: descriptor(std::move(descriptor)), columnDescriptor(std::move(columnDescriptor)) {}
};

/**
 * Tracks all RocksDB databases instances using a RocksDBDescriptor that
 * contains a weak reference to the database and column families.
 */
class DBRegistry final {
private:
	/**
	 * Private constructor.
	 */
	DBRegistry() : databasesMutex(std::make_unique<RegistryMutex>()) {}

	/**
	 * Map of database path to registry entry containing both the descriptor
	 * and condition variable for that path.
	 */
	std::unordered_map<std::string, DBRegistryEntry> databases;

	/**
	 * Mutex to protect the databases map.
	 * Using RegistryMutex (Windows CRITICAL_SECTION or std::mutex) to avoid CRT issues.
	 */
	std::unique_ptr<RegistryMutex> databasesMutex;

	/**
	 * Get the singleton instance of the registry.
	 * Uses Meyer's singleton pattern for guaranteed initialization.
	 */
	static DBRegistry& getInstance() {
		static DBRegistry instance;
		return instance;
	}

public:
	static void CloseDB(const std::shared_ptr<DBHandle> handle);
#ifdef DEBUG
	static void DebugLogDescriptorRefs();
#endif
	static void DestroyDB(const std::string& path);
	static void Init(napi_env env, napi_value exports);
	static std::unique_ptr<DBHandleParams> OpenDB(const std::string& path, const DBOptions& options);
	static void PurgeAll();
	static napi_value RegistryStatus(napi_env env, napi_callback_info info);
	static void Shutdown();
	static size_t Size();
};

} // namespace rocksdb_js

#endif
