#include "transaction_log.h"
#include "macros.h"
#include "util.h"
#include <stdlib.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

namespace rocksdb_js {

napi_value TransactionLog::Constructor(napi_env env, napi_callback_info info) {
	NAPI_CONSTRUCTOR("TransactionLog")
	return nullptr;
}

/**
 * Commits the transaction log.
 */
napi_value TransactionLog::Commit(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	// UNWRAP_TRANSACTION_LOG_HANDLE("Commit")
	return nullptr;
}

const int MAX_LOG_FILE_SIZE = 16777216;
/**
 * Gets the range of the transaction log.
 */
napi_value TransactionLog::getMemoryMapOfFile(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	// UNWRAP_TRANSACTION_LOG_HANDLE("getMemoryMapOfFile")
	NAPI_GET_STRING(argv[0], logNumber, "Log number required");
	char *filename = "id/seq.log"; // TODO: create the filename
	// TODO: We will need a registry/map of the opened memory maps, so we can use an existing map if it exists
	void *map = nullptr; // = get the map from registry by filename
	if (!map) {
		int fd = open(filename, O_RDONLY);
		if (fd == -1) {
			// error
		}
		map = mmap(NULL, MAX_LOG_FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
		// TODO: handle errors
		// insert into registry of memory-maps
	}
	napi_value result;
	napi_create_external_buffer(env, MAX_LOG_FILE_SIZE, map, [](napi_env env, void* data, void* hint) {
		// TODO: Decrement reference count of registry/map, possibly calling munmap(map, size) and close(fd) if it is done
	}, nullptr, &result);
	return result;
}


/**
 * Initializes the `NativeTransactionLog` JavaScript class.
 */
void TransactionLog::Init(napi_env env, napi_value exports) {
	napi_property_descriptor properties[] = {
		{ "commit", nullptr, Commit, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getMemoryMapOfFile", nullptr, getMemoryMapOfFile, nullptr, nullptr, nullptr, napi_default, nullptr },
	};

	auto className = "TransactionLog";
	constexpr size_t len = sizeof("TransactionLog") - 1;

	napi_ref exportsRef;
	NAPI_STATUS_THROWS_VOID(::napi_create_reference(env, exports, 1, &exportsRef))

	napi_value ctor;
	NAPI_STATUS_THROWS_VOID(::napi_define_class(
		env,
		className,         // className
		len,               // length of class name
		Constructor,       // constructor
		(void*)exportsRef, // constructor arg
		sizeof(properties) / sizeof(napi_property_descriptor), // number of properties
		properties,        // properties array
		&ctor              // [out] constructor
	))

	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, className, ctor))
}

} // namespace rocksdb_js
