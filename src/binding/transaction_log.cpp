#include "db_handle.h"
#include "db_descriptor.h"
#include "transaction_log.h"
#include "transaction_log_handle.h"
#include "macros.h"
#include "util.h"

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
 * Constructor for the `NativeTransactionLog` class.
 *
 * @param env - The NAPI environment.
 * @param info - The callback info.
 * @returns The new `NativeTransactionLog` object.
 *
 * @example
 * ```ts
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
 * Adds an entry to the transaction log.
 *
 * @example
 * ```ts
 * const log = db.useLog('foo');
 * log.addEntry(Buffer.from('hello'), txn.id);
 * log.addEntry(Buffer.from('world'), txn.id, true);
 * ```
 */
napi_value TransactionLog::AddEntry(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	UNWRAP_TRANSACTION_LOG_HANDLE("AddEntry")

	// log entry
	bool isBuffer;
	NAPI_STATUS_THROWS(::napi_is_buffer(env, argv[0], &isBuffer));
	bool isArrayBuffer;
	NAPI_STATUS_THROWS(::napi_is_arraybuffer(env, argv[0], &isArrayBuffer));
	if (!isBuffer && !isArrayBuffer) {
		::napi_throw_error(env, nullptr, "Invalid log entry, expected a Buffer or Uint8Array");
		return nullptr;
	}

	char* data = nullptr;
	size_t size = 0;
	NAPI_STATUS_THROWS(::napi_get_buffer_info(env, argv[0], reinterpret_cast<void**>(&data), &size));
	if (data == nullptr) {
		::napi_throw_error(env, nullptr, "Invalid log data, expected a Buffer or Uint8Array");
		return nullptr;
	}

	// transaction (only required if TransactionHandle isn't bound to a transaction)
	napi_valuetype type;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[1], &type));
	uint32_t transactionId = (*txnLogHandle)->transactionId;
	if (type != napi_undefined) {
		if (type == napi_number) {
			NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[1], &transactionId));
		} else {
			transactionId = 0;
		}
	}
	if (transactionId == 0) {
		::napi_throw_error(env, nullptr, "Invalid argument, expected a transaction id");
		return nullptr;
	}

	napi_ref bufferRef = nullptr;

	try {
		// create a reference to pin the buffer in memory (prevents GC)
		NAPI_STATUS_THROWS(::napi_create_reference(env, argv[0], 1, &bufferRef));
		(*txnLogHandle)->addEntry(transactionId, data, size, env, bufferRef);
	} catch (const std::exception& e) {
		// if addEntry fails, clean up the buffer reference
		if (bufferRef != nullptr) {
			::napi_delete_reference(env, bufferRef);
		}
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}

	NAPI_RETURN_UNDEFINED()
}

napi_value TransactionLog::AddEntryCopy(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	UNWRAP_TRANSACTION_LOG_HANDLE("AddEntryCopy")

	// log entry
	bool isBuffer;
	NAPI_STATUS_THROWS(::napi_is_buffer(env, argv[0], &isBuffer));
	bool isArrayBuffer;
	NAPI_STATUS_THROWS(::napi_is_arraybuffer(env, argv[0], &isArrayBuffer));
	if (!isBuffer && !isArrayBuffer) {
		::napi_throw_error(env, nullptr, "Invalid log entry, expected a Buffer or Uint8Array");
		return nullptr;
	}

	char* data = nullptr;
	size_t size = 0;
	NAPI_STATUS_THROWS(::napi_get_buffer_info(env, argv[0], reinterpret_cast<void**>(&data), &size));
	if (data == nullptr) {
		::napi_throw_error(env, nullptr, "Invalid log data, expected a Buffer or Uint8Array");
		return nullptr;
	}

	// transaction (only required if TransactionHandle isn't bound to a transaction)
	napi_valuetype type;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[1], &type));
	uint32_t transactionId = (*txnLogHandle)->transactionId;
	if (type != napi_undefined) {
		if (type == napi_number) {
			NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[1], &transactionId));
		} else {
			transactionId = 0;
		}
	}
	if (transactionId == 0) {
		::napi_throw_error(env, nullptr, "Invalid argument, expected a transaction id");
		return nullptr;
	}

	try {
		std::unique_ptr<char[]> copyData(new char[size]);
		::memcpy(copyData.get(), data, size);
		(*txnLogHandle)->addEntry(transactionId, std::move(copyData), size);
	} catch (const std::exception& e) {
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}

	NAPI_RETURN_UNDEFINED()
}

/**
 * Gets the range of the transaction log.
 */
napi_value TransactionLog::Query(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_TRANSACTION_LOG_HANDLE("Query")

	// TODO:
	// - create a buffer for query() to populate
	// - bind the buffer to napi_create_external_buffer()

	try {
		(*txnLogHandle)->query();
	} catch (const std::exception& e) {
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}

	NAPI_RETURN_UNDEFINED()
}


/**
 * Initializes the `NativeTransactionLog` JavaScript class.
 */
void TransactionLog::Init(napi_env env, napi_value exports) {
	napi_property_descriptor properties[] = {
		{ "addEntry", nullptr, AddEntry, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "addEntryCopy", nullptr, AddEntryCopy, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "query", nullptr, Query, nullptr, nullptr, nullptr, napi_default, nullptr },
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
