#include "database/db_handle.h"
#include "database/db_descriptor.h"
#include "transaction_log/transaction_log.h"
#include "transaction_log_handle.h"
#include "napi/macros.h"
#include "core/platform.h"
#include "napi/helpers.h"
#include "napi/async.h"
#include <stdlib.h>
#include <fcntl.h>

#define UNWRAP_TRANSACTION_LOG_HANDLE(fnName) \
	std::shared_ptr<TransactionLogHandle>* txnLogHandle = nullptr; \
	do { \
		NAPI_STATUS_THROWS(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&txnLogHandle))); \
		if (!txnLogHandle || !(*txnLogHandle)) { \
			::napi_throw_error(env, nullptr, fnName " failed: TransactionLog has already been closed"); \
			return nullptr; \
		} \
	} while (0)

namespace rocksdb_js {

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
 *
 * // create a log that is NOT bound to a transaction; transaction id must be passed into
 * // `txnLog.addEntry()`
 * const log = db.useLog('test');
 * await db.transaction(async (txn) => {
 *   log.addEntry(Buffer.from('hello'), txn.id);
 * });
 *
 * // create a log that is bound to a transaction; transaction id is set automatically
 * await db.transaction(async (txn) => {
 *   const txnLog = txn.useLog('test');
 *   txnLog.addEntry(Buffer.from('hello'));
 * });
 * ```
 */
napi_value TransactionLog::Constructor(napi_env env, napi_callback_info info) {
	NAPI_CONSTRUCTOR_ARGV_WITH_DATA("TransactionLog", 3);

	napi_ref exportsRef = reinterpret_cast<napi_ref>(data);
	NAPI_GET_DB_HANDLE(argv[0], exportsRef, dbHandle, "Invalid argument, expected Database instance");

	NAPI_GET_STRING(argv[1], name, "Transaction log store name is required");

	// optional 3rd arg: transactionId (set when created via txn.useLog())
	uint32_t transactionId = 0;
	napi_valuetype thirdArgType;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[2], &thirdArgType));
	if (thirdArgType == napi_number) {
		NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[2], &transactionId));
	}

	std::shared_ptr<TransactionLogHandle>* txnLogHandle = new std::shared_ptr<TransactionLogHandle>(
		std::make_shared<TransactionLogHandle>(*dbHandle, name, (*dbHandle)->descriptor->readOnly)
	);
	(*txnLogHandle)->transactionId = transactionId;

	DEBUG_LOG("TransactionLog::Constructor Creating NativeTransactionLog TransactionLogHandle=%p\n", txnLogHandle->get());

	NAPI_STATUS_THROWS(::napi_wrap(
		env,
		jsThis,
		reinterpret_cast<void*>(txnLogHandle),
		[](napi_env env, void* data, void* hint) {
			DEBUG_LOG("TransactionLog::Constructor NativeTransactionLog GC'd txnLogHandle=%p\n", data);
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
	NAPI_METHOD_ARGV(2);
	UNWRAP_TRANSACTION_LOG_HANDLE("AddEntry");
	THROW_IF_READONLY(*txnLogHandle, "");

	bool isBuffer;
	bool isArrayBuffer;
	NAPI_STATUS_THROWS_ERROR(::napi_is_buffer(env, argv[0], &isBuffer), "Failed to check if log entry data is a Buffer");
	NAPI_STATUS_THROWS_ERROR(::napi_is_arraybuffer(env, argv[0], &isArrayBuffer), "Failed to check if log entry data is an ArrayBuffer");
	if (!isBuffer && !isArrayBuffer) {
		::napi_throw_type_error(env, nullptr, "Invalid log entry, expected a Buffer or ArrayBuffer");
		return nullptr;
	}

	char* data = nullptr;
	size_t size = 0;
	NAPI_STATUS_THROWS_ERROR(::napi_get_buffer_info(env, argv[0], reinterpret_cast<void**>(&data), &size),
		"Failed to get log entry data buffer info");
	if (data == nullptr) {
		::napi_throw_type_error(env, nullptr, "Invalid log entry data, expected a Buffer or ArrayBuffer");
		return nullptr;
	}

	uint32_t transactionId = (*txnLogHandle)->transactionId;
	napi_valuetype type;
	NAPI_STATUS_THROWS_ERROR(::napi_typeof(env, argv[1], &type), "Failed to get log entry transaction id type");
	if (type != napi_undefined) {
		if (type == napi_number) {
			int32_t signedTransactionId;
			NAPI_STATUS_THROWS_ERROR(::napi_get_value_int32(env, argv[1], &signedTransactionId), "Failed to get log entry transaction id");
			if (signedTransactionId < 0) {
				::napi_throw_type_error(env, nullptr, "Invalid argument, transaction id must be a non-negative integer");
				return nullptr;
			}
			transactionId = static_cast<uint32_t>(signedTransactionId);
		} else {
			::napi_throw_type_error(env, nullptr, "Invalid argument, transaction id must be a non-negative integer");
			return nullptr;
		}
	} else if (transactionId == 0) {
		::napi_throw_type_error(env, nullptr, "Missing argument, transaction id is required");
		return nullptr;
	}

	try {
		(*txnLogHandle)->addEntry(transactionId, data, static_cast<uint32_t>(size));
	} catch (const std::exception& e) {
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}

	NAPI_RETURN_UNDEFINED();
}

/**
 * Get the size of a sequenced log file.
 */
napi_value TransactionLog::GetLogFileSize(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);
	UNWRAP_TRANSACTION_LOG_HANDLE("GetLogFileSize");
	uint32_t sequenceNumber = 0;
	napi_valuetype type;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[0], &type));
	if (type == napi_number) {
		NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[0], &sequenceNumber));
		if (sequenceNumber == 0) {
			::napi_throw_type_error(env, nullptr, "Expected sequence number to be a positive integer greater than 0");
		}
	} else if (type != napi_undefined) {
		::napi_throw_type_error(env, nullptr, "Expected sequence number to be a number");
	} // if type == napi_undefined, leave sequenceNumber as 0, get all log files

	uint64_t fileSize = (*txnLogHandle)->getLogFileSize(sequenceNumber);
	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_double(env, (double) fileSize, &result));
	return result;
}

struct PositionHandle {
	std::shared_ptr<LogPosition> position;
};

/**
 * Return a buffer with the status of the sequenced log file.
 */
napi_value TransactionLog::GetLastCommittedPosition(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_TRANSACTION_LOG_HANDLE("GetLastCommittedPosition");
	auto lastCommittedPosition = (*txnLogHandle)->getLastCommittedPosition().lock();

	if (!lastCommittedPosition) {
		NAPI_RETURN_UNDEFINED();
	}

	napi_value result;
	PositionHandle* positionHandle = new PositionHandle{ lastCommittedPosition };
	NAPI_STATUS_THROWS(::napi_create_external_buffer(
		env,
		LOG_POSITION_SIZE, // length
		(void*)positionHandle->position.get(), // data
		[](napi_env env, void* data, void* hint) {
			PositionHandle* positionHandle = static_cast<PositionHandle*>(hint);
			delete positionHandle;
		},
		positionHandle, // finalize_hint
		&result // [out] result
	));
	return result;
}

/**
 * Gets the range of the transaction log.
 */
napi_value TransactionLog::GetMemoryMapOfFile(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);
	UNWRAP_TRANSACTION_LOG_HANDLE("GetMemoryMapOfFile");
	uint32_t sequenceNumber = 0;
	NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[0], &sequenceNumber));

	std::shared_ptr<MemoryMap> memoryMap = (*txnLogHandle)->getMemoryMap(sequenceNumber);
	if (!memoryMap) {
		// if memory map is not found (if given a sequence number to a file that doesn't exist), return undefined
		NAPI_RETURN_UNDEFINED();
	}

	// Transfer ownership to the external buffer: this heap shared_ptr is held
	// until finalize. For a frozen log the log file keeps only a weak handle, so
	// this (plus any still-live buffer for the same file) is the sole owner — the
	// mapping is unmapped when JS GCs the buffer.
	auto* memoryMapHandle = new std::shared_ptr<MemoryMap>(memoryMap);

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_external_buffer(
		env,  // env
		memoryMap->fileSize, // length
		memoryMap->map, // data
		[](napi_env env, void* data, void* hint) { // finalize_cb
			DEBUG_LOG("TransactionLog::GetMemoryMapOfFile External buffer GC'd memoryMapHandle=%p\n", hint);
			auto* memoryMap = static_cast<std::shared_ptr<MemoryMap>*>(hint);
			delete memoryMap;
		},
		memoryMapHandle, // finalize_hint
		&result // [out] result
	));

	// at this point, the transaction log file and the external buffer have a ref to the memory map

	return result;
}

/**
 * Find the position in the transaction logs with a transaction equal to or greater than the provided timestamp.
 */
napi_value TransactionLog::FindPosition(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);
	UNWRAP_TRANSACTION_LOG_HANDLE("FindPosition");
	double timestamp = 0;
	NAPI_STATUS_THROWS(::napi_get_value_double(env, argv[0], &timestamp));
	LogPosition position = (*txnLogHandle)->findPosition(timestamp);
	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_double(env, position.fullPosition, &result));
	return result;
}

/**
 * Get the last flushed position from the txn.state file.
 */
napi_value TransactionLog::GetLastFlushed(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_TRANSACTION_LOG_HANDLE("GetLastFlushed");
	LogPosition position = (*txnLogHandle)->getLastFlushed();
	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_double(env, position.fullPosition, &result));
	return result;
}

/**
 * Get the name of the transaction log store for this log.
 */
napi_value TransactionLog::GetName(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_TRANSACTION_LOG_HANDLE("GetName");
	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(env, (*txnLogHandle)->logName.c_str(), (*txnLogHandle)->logName.size(), &result));
	return result;
}

/**
 * Get the path to the transaction log store for this log.
 */
napi_value TransactionLog::GetPath(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_TRANSACTION_LOG_HANDLE("GetPath");
	auto store = (*txnLogHandle)->store.lock();
	if (store) {
		napi_value result;
		NAPI_STATUS_THROWS(::napi_create_string_utf8(env, store->path.string().c_str(), store->path.string().size(), &result));
		return result;
	}
	NAPI_RETURN_UNDEFINED();
}

// Sets a numeric property on `obj`. All transaction log stat values are exposed
// as JS numbers (doubles), which is what JS uses internally anyway and avoids
// any 2^53-boundary surprises with large byte counters.
#define SET_STAT(obj, key, value) \
	do { \
		napi_value _statValue; \
		NAPI_STATUS_THROWS(::napi_create_double(env, static_cast<double>(value), &_statValue)); \
		NAPI_STATUS_THROWS(::napi_set_named_property(env, obj, key, _statValue)); \
	} while (0)

// Builds a `{ sequence, offset }` object for a log position.
static napi_value buildPositionObject(napi_env env, const LogPosition& position) {
	napi_value obj;
	NAPI_STATUS_THROWS(::napi_create_object(env, &obj));
	SET_STAT(obj, "sequence", position.logSequenceNumber);
	SET_STAT(obj, "offset", position.positionInLogFile);
	return obj;
}

/**
 * Returns a detailed statistics snapshot for this transaction log store,
 * including memory (memory-map + overlay) usage, file/transaction gauges,
 * purge/retention gauges, and lifetime counters.
 */
napi_value TransactionLog::GetStats(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_TRANSACTION_LOG_HANDLE("GetStats");

	TransactionLogStoreStats s;
	if (!(*txnLogHandle)->collectStats(s)) {
		// database closed / store unresolvable
		NAPI_RETURN_UNDEFINED();
	}

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_object(env, &result));

	napi_value name;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(env, s.name.c_str(), s.name.size(), &name));
	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "name", name));
	napi_value path;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(env, s.path.c_str(), s.path.size(), &path));
	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "path", path));

	SET_STAT(result, "fileCount", s.fileCount);
	SET_STAT(result, "currentSequenceNumber", s.currentSequenceNumber);
	SET_STAT(result, "oldestSequenceNumber", s.oldestSequenceNumber);
	SET_STAT(result, "totalSizeBytes", s.totalSizeBytes);
	SET_STAT(result, "currentFileSize", s.currentFileSize);
	SET_STAT(result, "pendingTransactions", s.pendingTransactions);
	SET_STAT(result, "uncommittedTransactions", s.uncommittedTransactions);
	SET_STAT(result, "replayGapBytes", s.replayGapBytes);

	// memory
	napi_value memory;
	NAPI_STATUS_THROWS(::napi_create_object(env, &memory));
	SET_STAT(memory, "mappedBytes", s.mappedBytes);
	SET_STAT(memory, "overlayBytes", s.overlayBytes);
	SET_STAT(memory, "activeMaps", s.activeMaps);
	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "memory", memory));

	// positions
	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "nextLogPosition", buildPositionObject(env, s.nextLogPosition)));
	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "lastFlushedPosition", buildPositionObject(env, s.lastFlushedPosition)));
	if (s.hasLastCommittedPosition) {
		NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "lastCommittedPosition", buildPositionObject(env, s.lastCommittedPosition)));
	} else {
		napi_value nullValue;
		NAPI_STATUS_THROWS(::napi_get_null(env, &nullValue));
		NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "lastCommittedPosition", nullValue));
	}

	// purge / retention gauges
	napi_value purge;
	NAPI_STATUS_THROWS(::napi_create_object(env, &purge));
	SET_STAT(purge, "oldestFileAgeMs", s.oldestFileAgeMs);
	SET_STAT(purge, "purgeableFiles", s.purgeableFiles);
	SET_STAT(purge, "retainedUnflushedFiles", s.retainedUnflushedFiles);
	SET_STAT(purge, "lastPurgeMs", s.lastPurgeMs);
	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "purge", purge));

	// lifetime totals
	napi_value totals;
	NAPI_STATUS_THROWS(::napi_create_object(env, &totals));
	SET_STAT(totals, "transactionsWritten", s.transactionsWritten);
	SET_STAT(totals, "entriesWritten", s.entriesWritten);
	SET_STAT(totals, "bytesWritten", s.bytesWritten);
	SET_STAT(totals, "rotations", s.rotations);
	SET_STAT(totals, "filesPurged", s.filesPurged);
	SET_STAT(totals, "bytesPurged", s.bytesPurged);
	SET_STAT(totals, "purgeRuns", s.purgeRuns);
	SET_STAT(totals, "databaseFlushes", s.databaseFlushes);
	SET_STAT(totals, "writeFailures", s.writeFailures);
	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "totals", totals));

	// config
	napi_value config;
	NAPI_STATUS_THROWS(::napi_create_object(env, &config));
	SET_STAT(config, "maxFileSize", s.maxFileSize);
	SET_STAT(config, "retentionMs", s.retentionMs);
	SET_STAT(config, "maxAgeThreshold", s.maxAgeThreshold);
	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "config", config));

	return result;
}

/**
 * Initializes the `NativeTransactionLog` JavaScript class.
 */
void TransactionLog::Init(napi_env env, napi_value exports) {
	napi_property_descriptor properties[] = {
		{ "addEntry", nullptr, AddEntry, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getLogFileSize", nullptr, GetLogFileSize, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "path", nullptr, nullptr, GetPath, nullptr, nullptr, napi_default, nullptr },
		{ "name", nullptr, nullptr, GetName, nullptr, nullptr, napi_default, nullptr },
		{ "getStats", nullptr, GetStats, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "_findPosition", nullptr, FindPosition, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "_getLastCommittedPosition", nullptr, GetLastCommittedPosition, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "_getMemoryMapOfFile", nullptr, GetMemoryMapOfFile, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "_getLastFlushed", nullptr, GetLastFlushed, nullptr, nullptr, nullptr, napi_default, nullptr }
	};

	auto className = "TransactionLog";
	constexpr size_t len = sizeof("TransactionLog") - 1;

	napi_ref exportsRef;
	NAPI_STATUS_THROWS_VOID(::napi_create_reference(env, exports, 1, &exportsRef));

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
	));

	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, className, ctor));
}

} // namespace rocksdb_js
