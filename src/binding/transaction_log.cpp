#include "db_handle.h"
#include "db_descriptor.h"
#include "transaction_log.h"
#include "transaction_log_handle.h"
#include "macros.h"
#include "util.h"
#include <stdlib.h>
#include <fcntl.h>

#define UNWRAP_TRANSACTION_LOG_HANDLE(fnName) \
	std::shared_ptr<TransactionLogHandle>* txnLogHandle = nullptr; \
	NAPI_STATUS_THROWS(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&txnLogHandle))) \
	if (!txnLogHandle || !(*txnLogHandle)) { \
		::napi_throw_error(env, nullptr, fnName " failed: TransactionLog has already been closed"); \
		return nullptr; \
	}

#define GET_CTOR_STRING_ARG(from, to, errorMsg) \
	std::string to; \
	{ \
		napi_valuetype type; \
		NAPI_STATUS_THROWS(::napi_typeof(env, from, &type)); \
		if (type != napi_string) { \
			::napi_throw_error(env, nullptr, errorMsg); \
			return nullptr; \
		} \
		size_t length = 0; \
		NAPI_STATUS_THROWS(::napi_get_value_string_utf8(env, from, nullptr, 0, &length)); \
		to.resize(length, '\0'); \
		NAPI_STATUS_THROWS(::napi_get_value_string_utf8(env, from, to.data(), length + 1, &length)); \
	}

namespace rocksdb_js {

/**
 * Helper struct to hold parsed log entry data from NAPI arguments.
 */
struct ParsedLogEntryData {
	char* data;
	uint32_t size;
	uint32_t transactionId;
};

/**
 * Helper function to parse common log entry arguments.
 *
 * @returns true on success, false if error was thrown
 */
static bool parseLogEntryArgs(
	napi_env env,
	napi_value* argv,
	std::shared_ptr<TransactionLogHandle>& txnLogHandle,
	ParsedLogEntryData* parsed
) {
	bool isBuffer;
	NAPI_STATUS_THROWS_ERROR_RVAL(::napi_is_buffer(env, argv[0], &isBuffer), false, "Failed to check if log entry data is a Buffer");
	bool isArrayBuffer;
	NAPI_STATUS_THROWS_ERROR_RVAL(::napi_is_arraybuffer(env, argv[0], &isArrayBuffer), false, "Failed to check if log entry data is an ArrayBuffer");
	if (!isBuffer && !isArrayBuffer) {
		::napi_throw_type_error(env, nullptr, "Invalid log entry, expected a Buffer or ArrayBuffer");
		return false;
	}
	size_t size;
	NAPI_STATUS_THROWS_ERROR_RVAL(::napi_get_buffer_info(env, argv[0], reinterpret_cast<void**>(&parsed->data), &size), false,
		"Failed to get log entry data buffer info");
	if (parsed->data == nullptr) {
		::napi_throw_type_error(env, nullptr, "Invalid log entry data, expected a Buffer or Uint8Array");
		return false;
	}
	parsed->size = static_cast<uint32_t>(size);

	napi_valuetype type;
	NAPI_STATUS_THROWS_ERROR_RVAL(::napi_typeof(env, argv[1], &type), false, "Failed to get log entry transaction id type");
	parsed->transactionId = txnLogHandle->transactionId;
	if (type != napi_undefined) {
		if (type == napi_number) {
			int32_t signedTransactionId;
			NAPI_STATUS_THROWS_ERROR_RVAL(::napi_get_value_int32(env, argv[1], &signedTransactionId), false, "Failed to get log entry transaction id");
			if (signedTransactionId < 0) {
				::napi_throw_type_error(env, nullptr, "Invalid argument, transaction id must be a non-negative integer");
				return false;
			}
			parsed->transactionId = static_cast<uint32_t>(signedTransactionId);
		} else {
			::napi_throw_type_error(env, nullptr, "Invalid argument, transaction id must be a non-negative integer");
			return false;
		}
	} else if (parsed->transactionId == 0) {
		::napi_throw_type_error(env, nullptr, "Missing argument, transaction id is required");
		return false;
	}

	return true;
}

/**
 * Constructor for the `NativeTransactionLog` class.
 *
 * @param env - The NAPI environment.
 * @param info - The callback info.
 * @returns The new `NativeTransactionLog` object.
 *
 * @example
 * ```typescript
 * const db = RocksDatabase.open('/tmp/testdb');
 * db.open('/tmp/testdb');
 * const txnLog = new NativeTransactionLog(db, 'test');
 * ```
 */
napi_value TransactionLog::Constructor(napi_env env, napi_callback_info info) {
	NAPI_CONSTRUCTOR_ARGV_WITH_DATA("TransactionLog", 2)

	napi_ref exportsRef = reinterpret_cast<napi_ref>(data);
	NAPI_GET_DB_HANDLE(argv[0], exportsRef, dbHandle, "Invalid argument, expected Database instance")

	NAPI_GET_STRING(argv[1], name, "Transaction log store name is required")

	std::shared_ptr<TransactionLogHandle>* txnLogHandle = new std::shared_ptr<TransactionLogHandle>(
		std::make_shared<TransactionLogHandle>(*dbHandle, name)
	);

	DEBUG_LOG("TransactionLog::Constructor Creating NativeTransactionLog TransactionLogHandle=%p\n", txnLogHandle->get())

	NAPI_STATUS_THROWS(::napi_wrap(
		env,
		jsThis,
		reinterpret_cast<void*>(txnLogHandle),
		[](napi_env env, void* data, void* hint) {
			DEBUG_LOG("TransactionLog::Constructor NativeTransactionLog GC'd txnLogHandle=%p\n", data)
			auto* txnLogHandle = static_cast<std::shared_ptr<TransactionLogHandle>*>(data);
			if (txnLogHandle) {
				(*txnLogHandle)->close();
				txnLogHandle->reset();
				delete txnLogHandle;
			}
		},
		nullptr, // finalize_hint
		nullptr  // result
	));

	return jsThis;
}

/**
 * Adds an entry to the transaction log. The data is copied.
 *
 * @example
 * ```typescript
 * const log = db.useLog('foo');
 * log.addEntry(Buffer.from('hello'), txn.id);
 * log.addEntry(Buffer.from('world'), txn.id, true);
 * ```
 */
napi_value TransactionLog::AddEntry(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	UNWRAP_TRANSACTION_LOG_HANDLE("AddEntry")

	ParsedLogEntryData parsed;
	if (!parseLogEntryArgs(env, argv, *txnLogHandle, &parsed)) {
		return nullptr;
	}

	try {
		std::unique_ptr<char[]> copyData(new char[parsed.size]);
		::memcpy(copyData.get(), parsed.data, parsed.size);
		(*txnLogHandle)->addEntry(parsed.transactionId, std::move(copyData), parsed.size);
	} catch (const std::exception& e) {
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}

	NAPI_RETURN_UNDEFINED()
}

/**
 * Get the size of a sequenced log file.
 */
napi_value TransactionLog::GetLogFileSize(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	UNWRAP_TRANSACTION_LOG_HANDLE("GetLogFileSize")
	uint32_t sequenceNumber = 0;
	NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[0], &sequenceNumber));
	uint32_t fileSize = (*txnLogHandle)->getLogFileSize(sequenceNumber);
	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_uint32(env, fileSize, &result));
	return result;
}

/**
 * Return a buffer with the status of the sequenced log file.
 */
napi_value TransactionLog::GetLastCommittedPosition(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_TRANSACTION_LOG_HANDLE("GetLastCommittedPosition")
	PositionHandle* lastCommittedPosition = (*txnLogHandle)->getLastCommittedPosition();
	napi_value result;
	lastCommittedPosition->refCount++;
	NAPI_STATUS_THROWS(::napi_create_external_buffer(env, 8, lastCommittedPosition, [](napi_env env, void* data, void* hint) {
		PositionHandle* lastCommittedPosition = static_cast<PositionHandle*>(data);
		unsigned int refCount = lastCommittedPosition->refCount;
		DEBUG_LOG("TransactionLog::GetLastCommittedPosition cleanup refCount %u\n", refCount);
		if (--lastCommittedPosition->refCount == 0) {
			delete lastCommittedPosition;
		}
	}, nullptr, &result));
	return result;
}

/**
 * Gets the range of the transaction log.
 */
napi_value TransactionLog::GetMemoryMapOfFile(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	UNWRAP_TRANSACTION_LOG_HANDLE("GetMemoryMapOfFile")
	uint32_t sequenceNumber = 0;
	NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[0], &sequenceNumber));
	MemoryMap* memoryMap = (*txnLogHandle)->getMemoryMap(sequenceNumber);
	if (!memoryMap)
	{
		::napi_throw_error(env, nullptr, "Unable to create memory map for sequence number");
		return nullptr;
	}
	memoryMap->refCount++;
	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_external_buffer(env, memoryMap->fileSize, memoryMap->map, [](napi_env env, void* data, void* hint) {
		MemoryMap* memoryMap = static_cast<MemoryMap*>(hint);
		if (--memoryMap->refCount == 0) {
			// if there are no more references to the memory map, unmap it
			delete memoryMap;
		}
		int64_t memoryUsage;
		// re-adjust back
		napi_adjust_external_memory(env, memoryMap->fileSize, &memoryUsage);
	}, memoryMap, &result));
	int64_t memoryUsage = memoryMap->fileSize;
	// We need to adjust the tracked external memory after creating the external buffer.
	// More external memory "pressure" causes V8 to more aggressively garbage collect,
	// and with lots of external memory, this can be detrimental to performance.
	// And this is really should *not* be counted as external memory, because it is
	// a memory map of OS-owner memory, not process owned memory.
	// However, I am doubtful this is really implemented effectively in V8, these external
	// memory blocks do still seem to induce extra garbage collection. Still we call this,
	// because that's what we are supposed to do, and maybe eventually V8 will handle it
	// better.
	napi_adjust_external_memory(env, -memoryUsage, &memoryUsage);
	DEBUG_LOG("TransactionLog::GetMemoryMapOfFile fileSize=%u, external memory=%u\n", memoryMap->fileSize, memoryUsage);
	return result;
}


/**
 * Initializes the `NativeTransactionLog` JavaScript class.
 */
void TransactionLog::Init(napi_env env, napi_value exports) {
	napi_property_descriptor properties[] = {
		{ "addEntry", nullptr, AddEntry, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getLastCommittedPosition", nullptr, GetLastCommittedPosition, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getLogFileSize", nullptr, GetLogFileSize, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getMemoryMapOfFile", nullptr, GetMemoryMapOfFile, nullptr, nullptr, nullptr, napi_default, nullptr },
	};

	auto className = "TransactionLog";
	constexpr size_t len = sizeof("TransactionLog") - 1;

	napi_ref exportsRef;
	NAPI_STATUS_THROWS_VOID(::napi_create_reference(env, exports, 1, &exportsRef))

	napi_value ctor;
	NAPI_STATUS_THROWS_VOID(::napi_define_class(
		env,
		className,                   // className
		len,                         // length of class name
		TransactionLog::Constructor, // constructor
		(void*)exportsRef,           // constructor arg
		sizeof(properties) / sizeof(napi_property_descriptor), // number of properties
		properties,                  // properties array
		&ctor                        // [out] constructor
	))

	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, className, ctor))
}

} // namespace rocksdb_js
