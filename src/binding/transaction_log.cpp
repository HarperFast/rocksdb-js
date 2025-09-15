#include "transaction_log.h"
#include "macros.h"
#include "util.h"

namespace rocksdb_js {

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
napi_value TransactionLog::GetRange(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	// UNWRAP_TRANSACTION_LOG_HANDLE("GetRange")
	return nullptr;
}


/**
 * Initializes the `NativeTransactionLog` JavaScript class.
 */
void TransactionLog::Init(napi_env env, napi_value exports) {
	napi_property_descriptor properties[] = {
		{ "commit", nullptr, Commit, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getRange", nullptr, GetRange, nullptr, nullptr, nullptr, napi_default, nullptr },
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
