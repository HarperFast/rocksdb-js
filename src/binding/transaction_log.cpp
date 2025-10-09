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
	NAPI_GET_DB_HANDLE(args[0], exportsRef, dbHandle, "Invalid argument, expected Database instance")

	NAPI_GET_STRING(args[1], name, "Transaction log store name is required")

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
 */
napi_value TransactionLog::AddEntry(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(3)
	UNWRAP_TRANSACTION_LOG_HANDLE("AddEntry")

	// timestamp
	napi_valuetype type;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[0], &type));
	if (type != napi_number) {
		::napi_throw_error(env, nullptr, "Invalid timestamp, expected a number");
		return nullptr;
	}
	uint64_t timestamp;
    NAPI_STATUS_THROWS(rocksdb_js::getValue(env, argv[0], timestamp));

	// log entry
	bool isBuffer;
	NAPI_STATUS_THROWS(::napi_is_buffer(env, argv[1], &isBuffer));
	bool isArrayBuffer;
	NAPI_STATUS_THROWS(::napi_is_arraybuffer(env, argv[1], &isArrayBuffer));
	if (!isBuffer && !isArrayBuffer) {
		::napi_throw_error(env, nullptr, "Invalid log entry, expected a Buffer or Uint8Array");
		return nullptr;
	}
	char* logEntry = nullptr;
	size_t logEntryLength = 0;
	NAPI_STATUS_THROWS(::napi_get_buffer_info(env, argv[1], reinterpret_cast<void**>(&logEntry), &logEntryLength));
	if (logEntry == nullptr) {
		::napi_throw_error(env, nullptr, "Invalid log entry, expected a Buffer or Uint8Array");
		return nullptr;
	}

	// options
	napi_valuetype optionsType;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[2], &optionsType));
	if (optionsType != napi_undefined && optionsType != napi_null) {
		if (optionsType != napi_object) {
			::napi_throw_error(env, nullptr, "Invalid options, expected an object");
			return nullptr;
		}

		// TODO: handle `options.transaction`
	}

	try {
		(*txnLogHandle)->addEntry(timestamp, logEntry, logEntryLength);
	} catch (const std::exception& e) {
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}

	NAPI_RETURN_UNDEFINED()
}

/**
 * Commits the transaction log.
 */
napi_value TransactionLog::Commit(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_TRANSACTION_LOG_HANDLE("Commit")

	// Now you can use (*txnLogHandle) to access the TransactionLogHandle
	// TODO: Implement the actual commit functionality

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
		{ "commit", nullptr, Commit, nullptr, nullptr, nullptr, napi_default, nullptr },
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
