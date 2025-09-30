#include "transaction_log.h"
#include "transaction_log_handle.h"
#include "macros.h"
#include "util.h"

namespace rocksdb_js {

napi_value TransactionLog::Constructor(napi_env env, napi_callback_info info) {
	NAPI_CONSTRUCTOR_ARGV("TransactionLog", 1)

	std::string name;
	size_t nameLength = 0;
	NAPI_STATUS_THROWS(::napi_get_value_string_utf8(env, args[0], name.data(), name.size(), &nameLength))

	auto* txnLogHandle = new std::shared_ptr<TransactionLogHandle>(std::make_shared<TransactionLogHandle>(name));

	DEBUG_LOG("TransactionLog::Constructor Creating NativeTransactionLog TransactionLogHandle=%p\n", txnLogHandle->get())

	NAPI_STATUS_THROWS(::napi_wrap(
		env,
		jsThis,
		reinterpret_cast<void*>(txnLogHandle),
		[](napi_env env, void* data, void* hint) {
			DEBUG_LOG("TransactionLog::Constructor NativeTransactionLog GC'd txnLogHandle=%p\n", data)
			auto* txnLogHandle = static_cast<std::shared_ptr<TransactionLogHandle>*>(data);
			if (txnLogHandle) {
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
napi_value TransactionLog::Add(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	// UNWRAP_TRANSACTION_LOG_HANDLE("Add")
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

/**
 * Gets the range of the transaction log.
 */
napi_value TransactionLog::Query(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	// UNWRAP_TRANSACTION_LOG_HANDLE("Query")
	return nullptr;
}


/**
 * Initializes the `NativeTransactionLog` JavaScript class.
 */
void TransactionLog::Init(napi_env env, napi_value exports) {
	napi_property_descriptor properties[] = {
		{ "add", nullptr, Add, nullptr, nullptr, nullptr, napi_default, nullptr },
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
