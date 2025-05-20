#include "database.h"
#include "db_handle.h"
#include "macros.h"
#include "transaction.h"
#include "transaction_handle.h"
#include "util.h"
#include <sstream>

#define UNWRAP_TRANSACTION_HANDLE(fnName) \
	DBTxnHandle* dbTxnHandle = nullptr; \
	NAPI_STATUS_THROWS(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&dbTxnHandle))) \
	if (dbTxnHandle == nullptr || !dbTxnHandle->txnHandle) { \
		::napi_throw_error(env, nullptr, fnName " failed: Transaction has already been closed"); \
		return nullptr; \
	} \
	std::shared_ptr<TransactionHandle> txnHandle = dbTxnHandle->txnHandle;

namespace rocksdb_js {

/**
 * Initialize the constructor reference for the `NativeTransaction` class. We
 * need to do this because the constructor is static and we need to access it
 * in the static methods.
 */
napi_ref Transaction::constructor = nullptr;

void dumpNapiValue(napi_env env, const char* name, napi_value value) {
	napi_valuetype type;
	NAPI_STATUS_THROWS_VOID(::napi_typeof(env, value, &type))
	
	fprintf(stderr, "%s: (type %d) ", name, type);
	switch (type) {
		case napi_undefined: fprintf(stderr, "undefined"); break;
		case napi_null: fprintf(stderr, "null"); break;
		case napi_boolean: {
			bool result;
			NAPI_STATUS_THROWS_VOID(::napi_get_value_bool(env, value, &result))
			fprintf(stderr, "boolean: %s", result ? "true" : "false");
			break;
		}
		case napi_number: {
			double result;
			NAPI_STATUS_THROWS_VOID(::napi_get_value_double(env, value, &result))
			fprintf(stderr, "number: %f", result);
			break;
		}
		case napi_string: {
			char buffer[1024];
			size_t result;
			NAPI_STATUS_THROWS_VOID(::napi_get_value_string_utf8(env, value, buffer, sizeof(buffer), &result))
			fprintf(stderr, "string: %s", buffer);
			break;
		}
		case napi_symbol: fprintf(stderr, "symbol"); break;
		case napi_object: {
			fprintf(stderr, "object");
			// Get property names
			napi_value properties;
			NAPI_STATUS_THROWS_VOID(::napi_get_property_names(env, value, &properties))
			
			uint32_t length;
			NAPI_STATUS_THROWS_VOID(::napi_get_array_length(env, properties, &length))
			
			fprintf(stderr, " with %u properties:\n", length);
			
			// Iterate through properties
			for (uint32_t i = 0; i < length; i++) {
				napi_value propertyName;
				NAPI_STATUS_THROWS_VOID(::napi_get_element(env, properties, i, &propertyName))
				
				char nameBuffer[1024];
				size_t nameLength;
				NAPI_STATUS_THROWS_VOID(::napi_get_value_string_utf8(env, propertyName, nameBuffer, sizeof(nameBuffer), &nameLength))
				
				napi_value propertyValue;
				NAPI_STATUS_THROWS_VOID(::napi_get_property(env, value, propertyName, &propertyValue))
				
				fprintf(stderr, "  - %s: ", nameBuffer);
				dumpNapiValue(env, name, propertyValue); // Recursively dump property values
				fprintf(stderr, "\n");
			}
			return; // Return early since we've already printed the object details
		}
		case napi_function: fprintf(stderr, "function"); break;
		case napi_external: fprintf(stderr, "external"); break;
		case napi_bigint: fprintf(stderr, "bigint"); break;
		default: fprintf(stderr, "unknown");
	}
	fprintf(stderr, ")\n");
}

/**
 * A simple wrapper around the DBHandle and TransactionHandle to pass into the
 * Transaction JS constructor so it can be cleaned up when the Transaction JS
 * object is garbage collected.
 */
struct DBTxnHandle final {
	DBTxnHandle(std::shared_ptr<DBHandle> dbHandle)
		: dbDescriptor(dbHandle->descriptor)
	{
		this->txnHandle = std::make_shared<TransactionHandle>(dbHandle);
		this->dbDescriptor->transactionAdd(this->txnHandle);
	}

	~DBTxnHandle() {
		this->close();
	}

	void close() {
		if (this->txnHandle) {
			this->dbDescriptor->transactionRemove(this->txnHandle);
			this->txnHandle->close();
			this->txnHandle.reset();
		}
	}

	std::shared_ptr<DBDescriptor> dbDescriptor;
	std::shared_ptr<TransactionHandle> txnHandle;
};

/**
 * Creates a new `NativeTransaction` object.
 *
 * @param env - The NAPI environment.
 * @param info - The callback info.
 * @returns The new `NativeTransaction` object.
 */
napi_value Transaction::Constructor(napi_env env, napi_callback_info info) {
	NAPI_CONSTRUCTOR_ARGV("Transaction", 1)

	// 6 object
	// 7 function
	napi_valuetype argsZeroType;
	NAPI_STATUS_THROWS(::napi_typeof(env, args[0], &argsZeroType))
	fprintf(stderr, "Transaction::Constructor args[0] type=%d\n", argsZeroType);
	if (argsZeroType == 6) {
		fprintf(stderr, "Transaction::Constructor args[0] is an object!\n");
		// display the object properties
	} else if (argsZeroType == 7) {
		fprintf(stderr, "Transaction::Constructor args[0] is a function!\n");
	}

	napi_value databaseCtor;
	NAPI_STATUS_THROWS(::napi_get_reference_value(env, Database::constructor, &databaseCtor))

	bool isDatabase = false;
	NAPI_STATUS_THROWS(::napi_instanceof(env, args[0], databaseCtor, &isDatabase))

	napi_value prototype;
	NAPI_STATUS_THROWS(::napi_get_prototype(env, args[0], &prototype))

	napi_value constructorName;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(env, "constructor", NAPI_AUTO_LENGTH, &constructorName))

	napi_value protoConstructor;
	NAPI_STATUS_THROWS(::napi_get_property(env, prototype, constructorName, &protoConstructor))

	dumpNapiValue(env, "args[0]", args[0]);
	dumpNapiValue(env, "args[0].prototype", prototype);
	dumpNapiValue(env, "args[0].prototype.constructor", protoConstructor);
	dumpNapiValue(env, "Database.constructor", databaseCtor);

	napi_valuetype protoConstructorType;
	NAPI_STATUS_THROWS(::napi_typeof(env, protoConstructor, &protoConstructorType))
	fprintf(stderr, "Transaction::Constructor protoConstructorType=%d\n", protoConstructorType);
	if (protoConstructorType == 6) {
		fprintf(stderr, "Transaction::Constructor protoConstructor is an object!\n");
		// display the object properties
	} else if (protoConstructorType == 7) {
		fprintf(stderr, "Transaction::Constructor protoConstructor is a function!\n");
	}

	napi_value protoConstructorName;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(env, "name", NAPI_AUTO_LENGTH, &protoConstructorName))

	napi_value protoConstructorNameValue;
	NAPI_STATUS_THROWS(::napi_get_property(env, protoConstructor, protoConstructorName, &protoConstructorNameValue))
	char buffer[1024];
	size_t bufferSize = sizeof(buffer);
	NAPI_STATUS_THROWS(::napi_get_value_string_utf8(env, protoConstructorNameValue, buffer, bufferSize, nullptr))
	fprintf(stderr, "Transaction::Constructor protoConstructorNameValue=%s\n", buffer);

	napi_value properties;
	NAPI_STATUS_THROWS(::napi_get_property_names(env, prototype, &properties))
	uint32_t propertyCount;
	NAPI_STATUS_THROWS(::napi_get_array_length(env, properties, &propertyCount))
	fprintf(stderr, "Transaction::Constructor PropertyCount=%zu\n", propertyCount);
	for (size_t i = 0; i < propertyCount; i++) {
		napi_value propertyName;
		NAPI_STATUS_THROWS(::napi_get_element(env, properties, i, &propertyName))
		fprintf(stderr, "Transaction::Constructor PropertyName=%s\n", propertyName);
	}

	bool isEqual;
	NAPI_STATUS_THROWS(::napi_strict_equals(env, protoConstructor, databaseCtor, &isEqual))
	fprintf(stderr, "Transaction::Constructor isEqual=%s\n", isEqual ? "true" : "false");

	DBTxnHandle* dbTxnHandle = nullptr;

	if (isDatabase) {
		DEBUG_LOG("Transaction::Constructor Received Database instance\n")
		std::shared_ptr<DBHandle>* dbHandle = nullptr;
		NAPI_STATUS_THROWS(::napi_unwrap(env, args[0], reinterpret_cast<void**>(&dbHandle)))
		DEBUG_LOG("Transaction::Constructor DBHandle=%p\n", (*dbHandle).get())
		if (dbHandle == nullptr || !(*dbHandle)->opened()) {
			::napi_throw_error(env, nullptr, "Database not open");
			return nullptr;
		}
		dbTxnHandle = new DBTxnHandle(*dbHandle);
	} else {
		bool isTransaction = false;
		napi_value transactionCtor;
		NAPI_STATUS_THROWS(::napi_get_reference_value(env, Transaction::constructor, &transactionCtor))
		NAPI_STATUS_THROWS(::napi_instanceof(env, args[0], transactionCtor, &isTransaction))

		if (isTransaction) {
			DEBUG_LOG("Transaction::Constructor Received Transaction instance\n")
			NAPI_STATUS_THROWS(::napi_unwrap(env, args[0], reinterpret_cast<void**>(&dbTxnHandle)))
			DEBUG_LOG("Transaction::Constructor DBTxnHandle=%p\n", dbTxnHandle)
		} else {
			napi_valuetype type;
			NAPI_STATUS_THROWS(::napi_typeof(env, args[0], &type))
			std::string errorMsg = "Invalid context, expected Database or Transaction instance, got type " + std::to_string(type);
			::napi_throw_error(env, nullptr, errorMsg.c_str());
			return nullptr;
		}
	}

	try {
		NAPI_STATUS_THROWS(::napi_wrap(
			env,
			jsThis,
			reinterpret_cast<void*>(dbTxnHandle),
			[](napi_env env, void* data, void* hint) {
				DBTxnHandle* dbTxnHandle = reinterpret_cast<DBTxnHandle*>(data);
				dbTxnHandle->close();
				delete dbTxnHandle;
			},
			nullptr, // finalize_hint
			nullptr  // result
		));

		return jsThis;
	} catch (const std::exception& e) {
		delete dbTxnHandle;
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}
}

/**
 * Aborts the transaction.
 */
napi_value Transaction::Abort(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_TRANSACTION_HANDLE("Abort")

	ROCKSDB_STATUS_THROWS_ERROR_LIKE(txnHandle->txn->Rollback(), "Transaction rollback failed")
	dbTxnHandle->close();

	NAPI_RETURN_UNDEFINED()
}

/**
 * State for the `Commit` async work.
 */
struct TransactionCommitState final {
	TransactionCommitState(napi_env env, std::shared_ptr<TransactionHandle> txnHandle)
		: asyncWork(nullptr), resolveRef(nullptr), rejectRef(nullptr), txnHandle(txnHandle) {}

	napi_async_work asyncWork;
	napi_ref resolveRef;
	napi_ref rejectRef;
	std::shared_ptr<TransactionHandle> txnHandle;
	rocksdb::Status status;
};

/**
 * Commits the transaction.
 */
napi_value Transaction::Commit(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	napi_value resolve = argv[0];
	napi_value reject = argv[1];
	UNWRAP_TRANSACTION_HANDLE("Commit")

	napi_value name;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(
		env,
		"transaction.commit",
		NAPI_AUTO_LENGTH,
		&name
	))

	TransactionCommitState* state = new TransactionCommitState(env, txnHandle);
	NAPI_STATUS_THROWS(::napi_create_reference(env, resolve, 1, &state->resolveRef))
	NAPI_STATUS_THROWS(::napi_create_reference(env, reject, 1, &state->rejectRef))

	NAPI_STATUS_THROWS(::napi_create_async_work(
		env,       // node_env
		nullptr,   // async_resource
		name,      // async_resource_name
		[](napi_env env, void* data) { // execute
			TransactionCommitState* state = reinterpret_cast<TransactionCommitState*>(data);
			state->status = state->txnHandle->txn->Commit();
			if (state->status.ok()) {
				state->txnHandle->close();
			}
		},
		[](napi_env env, napi_status status, void* data) { // complete
			TransactionCommitState* state = reinterpret_cast<TransactionCommitState*>(data);

			napi_value global;
			NAPI_STATUS_THROWS_VOID(::napi_get_global(env, &global))

			if (state->status.ok()) {
				napi_value resolve;
				NAPI_STATUS_THROWS_VOID(::napi_get_reference_value(env, state->resolveRef, &resolve))
				NAPI_STATUS_THROWS_VOID(::napi_call_function(env, global, resolve, 0, nullptr, nullptr))
			} else {
				napi_value reject;
				napi_value error;
				NAPI_STATUS_THROWS_VOID(::napi_get_reference_value(env, state->rejectRef, &reject))
				ROCKSDB_CREATE_ERROR_LIKE_VOID(error, state->status, "Transaction commit failed")
				NAPI_STATUS_THROWS_VOID(::napi_call_function(env, global, reject, 1, &error, nullptr))
			}

			NAPI_STATUS_THROWS_VOID(::napi_delete_reference(env, state->resolveRef))
			NAPI_STATUS_THROWS_VOID(::napi_delete_reference(env, state->rejectRef))

			delete state;
		},
		state,     // data
		&state->asyncWork // -> result
	));

	NAPI_STATUS_THROWS(::napi_queue_async_work(env, state->asyncWork))

	NAPI_RETURN_UNDEFINED()
}

/**
 * Commits the transaction synchronously.
 */
napi_value Transaction::CommitSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_TRANSACTION_HANDLE("CommitSync")

	rocksdb::Status status = txnHandle->txn->Commit();
	if (status.ok()) {
		dbTxnHandle->close();
	} else {
		napi_value error;
		ROCKSDB_CREATE_ERROR_LIKE_VOID(error, status, "Transaction commit failed")
		NAPI_STATUS_THROWS(::napi_throw(env, error))
	}

	NAPI_RETURN_UNDEFINED()
}

/**
 * Retrieves a value for the given key.
 */
napi_value Transaction::Get(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(3)
	NAPI_GET_BUFFER(argv[0], key, "Key is required")
	napi_value resolve = argv[1];
	napi_value reject = argv[2];
	UNWRAP_TRANSACTION_HANDLE("Get")

	rocksdb::Slice keySlice(key + keyStart, keyEnd - keyStart);
	return txnHandle->get(env, keySlice, resolve, reject);
}

/**
 * Retrieves a value for the given key.
 */
napi_value Transaction::GetSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	NAPI_GET_BUFFER(argv[0], key, "Key is required")
	UNWRAP_TRANSACTION_HANDLE("GetSync")

	rocksdb::Slice keySlice(key + keyStart, keyEnd - keyStart);
	std::string value;
	rocksdb::Status status = txnHandle->getSync(keySlice, value);

	if (status.IsNotFound()) {
		NAPI_RETURN_UNDEFINED()
	}

	if (!status.ok()) {
		::napi_throw_error(env, nullptr, status.ToString().c_str());
		return nullptr;
	}

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_buffer_copy(
		env,
		value.size(),
		value.data(),
		nullptr,
		&result
	))

	return result;
}

/**
 * Retrieves the ID of the transaction.
 */
napi_value Transaction::Id(napi_env env, napi_callback_info info) {
	NAPI_METHOD()
	UNWRAP_TRANSACTION_HANDLE("Id")

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_uint32(
		env,
		txnHandle->id,
		&result
	))
	return result;
}

/**
 * Puts a value for the given key.
 */
napi_value Transaction::PutSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(2)
	NAPI_GET_BUFFER(argv[0], key, "Key is required")
	NAPI_GET_BUFFER(argv[1], value, nullptr)
	UNWRAP_TRANSACTION_HANDLE("Put")

	rocksdb::Slice keySlice(key + keyStart, keyEnd - keyStart);
	rocksdb::Slice valueSlice(value + valueStart, valueEnd - valueStart);

	ROCKSDB_STATUS_THROWS_ERROR_LIKE(txnHandle->putSync(keySlice, valueSlice), "Transaction put failed")

	NAPI_RETURN_UNDEFINED()
}

/**
 * Removes a value for the given key.
 */
napi_value Transaction::RemoveSync(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1)
	NAPI_GET_BUFFER(argv[0], key, "Key is required")
	UNWRAP_TRANSACTION_HANDLE("Remove")

	rocksdb::Slice keySlice(key + keyStart, keyEnd - keyStart);

	ROCKSDB_STATUS_THROWS_ERROR_LIKE(txnHandle->removeSync(keySlice), "Transaction remove failed")

	NAPI_RETURN_UNDEFINED()
}

/**
 * Initializes the `NativeTransaction` JavaScript class.
 */
void Transaction::Init(napi_env env, napi_value exports) {
	napi_property_descriptor properties[] = {
		{ "abort", nullptr, Abort, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "commit", nullptr, Commit, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "commitSync", nullptr, CommitSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "get", nullptr, Get, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "getSync", nullptr, GetSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "id", nullptr, nullptr, Id, nullptr, nullptr, napi_default, nullptr },
		// merge?
		{ "putSync", nullptr, PutSync, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "removeSync", nullptr, RemoveSync, nullptr, nullptr, nullptr, napi_default, nullptr }
	};

	auto className = "Transaction";
	constexpr size_t len = sizeof("Transaction") - 1;

	napi_value ctor;
	NAPI_STATUS_THROWS_VOID(::napi_define_class(
		env,
		className,    // className
		len,          // length of class name
		Constructor,  // constructor
		nullptr,      // constructor arg
		sizeof(properties) / sizeof(napi_property_descriptor), // number of properties
		properties,   // properties array
		&ctor         // [out] constructor
	))

	NAPI_STATUS_THROWS_VOID(::napi_create_reference(env, ctor, 1, &constructor))

	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, className, ctor))
}

} // namespace rocksdb_js

