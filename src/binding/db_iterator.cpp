#include "db_handle.h"
#include "db_descriptor.h"
#include "db_iterator.h"
#include "db_iterator_handle.h"
#include "macros.h"
#include "transaction.h"
#include "util.h"

namespace rocksdb_js {

DBIteratorOptions::DBIteratorOptions() :
	startKeyStr(nullptr),
	startKeyStart(0),
	startKeyEnd(0),

	endKeyStr(nullptr),
	endKeyStart(0),
	endKeyEnd(0),

	inclusiveEnd(false),
	exclusiveStart(false),
	reverse(false),
	values(true)
{}

napi_status DBIteratorOptions::initFromNapiObject(napi_env env, napi_value options) {
	NAPI_STATUS_THROWS_RVAL(rocksdb_js::getProperty(env, options, "exclusiveStart", this->exclusiveStart), napi_invalid_arg);
	NAPI_STATUS_THROWS_RVAL(rocksdb_js::getProperty(env, options, "inclusiveEnd", this->inclusiveEnd), napi_invalid_arg);

	this->readOptions.adaptive_readahead = true;
	NAPI_STATUS_THROWS_RVAL(rocksdb_js::getProperty(env, options, "adaptiveReadahead", this->readOptions.adaptive_readahead), napi_invalid_arg);

	this->readOptions.async_io = true;
	NAPI_STATUS_THROWS_RVAL(rocksdb_js::getProperty(env, options, "asyncIO", this->readOptions.async_io), napi_invalid_arg);

	this->readOptions.auto_readahead_size = true;
	NAPI_STATUS_THROWS_RVAL(rocksdb_js::getProperty(env, options, "autoReadaheadSize", this->readOptions.auto_readahead_size), napi_invalid_arg);

	this->readOptions.background_purge_on_iterator_cleanup = true;
	NAPI_STATUS_THROWS_RVAL(rocksdb_js::getProperty(env, options, "backgroundPurgeOnIteratorCleanup", this->readOptions.background_purge_on_iterator_cleanup), napi_invalid_arg);

	this->readOptions.fill_cache = false;
	NAPI_STATUS_THROWS_RVAL(rocksdb_js::getProperty(env, options, "fillCache", this->readOptions.fill_cache), napi_invalid_arg);

	this->readOptions.readahead_size = 0;
	NAPI_STATUS_THROWS_RVAL(rocksdb_js::getProperty(env, options, "readaheadSize", this->readOptions.readahead_size), napi_invalid_arg);

	this->readOptions.tailing = false;
	NAPI_STATUS_THROWS_RVAL(rocksdb_js::getProperty(env, options, "tailing", this->readOptions.tailing), napi_invalid_arg);

	NAPI_ASSERT_OBJECT_OR_UNDEFINED(options, "Invalid options");
	NAPI_STATUS_THROWS_RVAL(rocksdb_js::getKeyFromProperty(env, options, "start", "Invalid start key", this->startKeyStr, this->startKeyStart, this->startKeyEnd), napi_invalid_arg);
	NAPI_STATUS_THROWS_RVAL(rocksdb_js::getKeyFromProperty(env, options, "end", "Invalid end key", this->endKeyStr, this->endKeyStart, this->endKeyEnd), napi_invalid_arg);

	return napi_ok;
}

/**
 * Creates a new `NativeIterator` object.
 *
 * @param env - The NAPI environment.
 * @param info - The callback info.
 * @returns The new `NativeIterator` object.
 */
napi_value DBIterator::Constructor(napi_env env, napi_callback_info info) {
	NAPI_CONSTRUCTOR_ARGV_WITH_DATA("Iterator", 2);

	napi_ref exportsRef = reinterpret_cast<napi_ref>(data);
	napi_value exports;
	NAPI_STATUS_THROWS(::napi_get_reference_value(env, exportsRef, &exports));

	napi_value databaseCtor;
	NAPI_STATUS_THROWS(::napi_get_named_property(env, exports, "Database", &databaseCtor));

	bool isDatabase = false;
	NAPI_STATUS_THROWS(::napi_instanceof(env, argv[0], databaseCtor, &isDatabase));

	napi_value options = argv[1];
	DBIteratorOptions itOptions;
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "reverse", itOptions.reverse));
	NAPI_STATUS_THROWS(rocksdb_js::getProperty(env, options, "values", itOptions.values));
	if (itOptions.initFromNapiObject(env, options) != napi_ok) {
		return nullptr;
	}

	std::shared_ptr<DBIteratorHandle>* itHandle = nullptr;

	if (isDatabase) {
		std::shared_ptr<DBHandle>* dbHandle = nullptr;
		NAPI_STATUS_THROWS(::napi_unwrap(env, argv[0], reinterpret_cast<void**>(&dbHandle)));
		DEBUG_LOG("DBIterator::Constructor Initializing transaction handle with Database instance (dbHandle=%p)\n", (*dbHandle).get());
		if (dbHandle == nullptr || !(*dbHandle)->opened()) {
			::napi_throw_error(env, nullptr, "Database not open");
			return nullptr;
		}
		itHandle = new std::shared_ptr<DBIteratorHandle>(std::make_shared<DBIteratorHandle>(*dbHandle, itOptions));
	} else {
		DEBUG_LOG("DBIterator::Constructor Using existing transaction handle\n");
		napi_value transactionCtor;
		NAPI_STATUS_THROWS(::napi_get_named_property(env, exports, "Transaction", &transactionCtor));

		bool isTransaction = false;
		NAPI_STATUS_THROWS(::napi_instanceof(env, argv[0], transactionCtor, &isTransaction));

		if (isTransaction) {
			DEBUG_LOG("DBIterator::Constructor Received Transaction instance\n");
			std::shared_ptr<TransactionHandle>* txnHandle = nullptr;
			NAPI_STATUS_THROWS(::napi_unwrap(env, argv[0], reinterpret_cast<void**>(&txnHandle)));
			itHandle = new std::shared_ptr<DBIteratorHandle>(std::make_shared<DBIteratorHandle>((*txnHandle).get(), itOptions));
			DEBUG_LOG("DBIterator::Constructor txnHandle=%p descriptor=%p\n", (*txnHandle).get(), (*itHandle)->dbHandle->descriptor.get());
		} else {
			napi_valuetype type;
			NAPI_STATUS_THROWS(::napi_typeof(env, argv[0], &type));
			std::string errorMsg = "Invalid context, expected Database or Transaction instance, got type " + std::to_string(type);
			::napi_throw_error(env, nullptr, errorMsg.c_str());
			return nullptr;
		}
	}

	// attach this iterator to the descriptor so it gets cleaned up when the descriptor is closed
	if ((*itHandle)->dbHandle && (*itHandle)->dbHandle->descriptor) {
		(*itHandle)->dbHandle->descriptor->attach(*itHandle);
	}

	DEBUG_LOG("DBIterator::Constructor itHandle=%p\n", itHandle);

	try {
		NAPI_STATUS_THROWS(::napi_wrap(
			env,
			jsThis,
			reinterpret_cast<void*>(itHandle),
			[](napi_env env, void* data, void* hint) {
				DEBUG_LOG("DBIterator::Constructor NativeIterator GC'd itHandle=%p\n", data);
				auto* itHandle = static_cast<std::shared_ptr<DBIteratorHandle>*>(data);
				if (*itHandle) {
					if ((*itHandle)->dbHandle && (*itHandle)->dbHandle->descriptor) {
						(*itHandle)->dbHandle->descriptor->detach(*itHandle);
					}
					(*itHandle)->close();
				}
				delete itHandle;
			},
			nullptr, // finalize_hint
			nullptr  // result
		));

		return jsThis;
	} catch (const std::exception& e) {
		delete itHandle;
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}
}

#define UNWRAP_ITERATOR_HANDLE(fnName) \
	std::shared_ptr<DBIteratorHandle>* itHandle = nullptr; \
	do { \
		NAPI_STATUS_THROWS(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&itHandle))); \
		if (!itHandle || (*itHandle)->iterator == nullptr) { \
			::napi_throw_error(env, nullptr, fnName " failed: Iterator not initialized"); \
			return nullptr; \
		} \
	} while (0)

/**
 * Advances the iterator to the next key/value pair.
 *
 * @param env - The NAPI environment.
 * @param info - The callback info.
 * @returns The next key/value pair.
 */
napi_value DBIterator::Next(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_ITERATOR_HANDLE("Next");

	napi_value result;
	napi_value resultDone;
	napi_value resultValue;
	NAPI_STATUS_THROWS(::napi_create_object(env, &result));

	if ((*itHandle)->iterator->Valid()) {
		rocksdb::Slice keySlice = (*itHandle)->iterator->key();

		// Check if this is the last item and should be skipped (reverse + exclusiveStart + key equals startKey)
		if ((*itHandle)->reverse && (*itHandle)->exclusiveStart &&
		    (*itHandle)->startKey.size() > 0 && keySlice.compare((*itHandle)->startKey) == 0) {
			// Peek ahead to check if this is the last item
			(*itHandle)->iterator->Prev();
			if (!(*itHandle)->iterator->Valid()) {
				// This is the last item and it equals startKey, skip it
				NAPI_STATUS_THROWS(::napi_get_boolean(env, true, &resultDone));
				NAPI_STATUS_THROWS(::napi_get_undefined(env, &resultValue));
			} else {
				// Not the last item, restore position and continue normally
				(*itHandle)->iterator->Next();

				// Re-read key and value after restoring position
				rocksdb::Slice restoredKeySlice = (*itHandle)->iterator->key();

				NAPI_STATUS_THROWS(::napi_get_boolean(env, false, &resultDone));

				napi_value key;
				napi_value value;

				NAPI_STATUS_THROWS(::napi_create_buffer_copy(
					env,
					restoredKeySlice.size(),
					restoredKeySlice.data(),
					nullptr,
					&key
				));

				NAPI_STATUS_THROWS(::napi_create_object(env, &resultValue));
				NAPI_STATUS_THROWS(::napi_set_named_property(env, resultValue, "key", key));

				if ((*itHandle)->values) {
					rocksdb::Slice valueSlice = (*itHandle)->iterator->value();
					// TODO: use a shared buffer
					NAPI_STATUS_THROWS(::napi_create_buffer_copy(
						env,
						valueSlice.size(),
						valueSlice.data(),
						nullptr,
						&value
					));

					NAPI_STATUS_THROWS(::napi_set_named_property(env, resultValue, "value", value));
				}

				(*itHandle)->iterator->Prev();
			}
		} else {
			NAPI_STATUS_THROWS(::napi_get_boolean(env, false, &resultDone));

			napi_value key;
			napi_value value;

			NAPI_STATUS_THROWS(::napi_create_buffer_copy(
				env,
				keySlice.size(),
				keySlice.data(),
				nullptr,
				&key
			));

			NAPI_STATUS_THROWS(::napi_create_object(env, &resultValue));
			NAPI_STATUS_THROWS(::napi_set_named_property(env, resultValue, "key", key));

			if ((*itHandle)->values) {
				rocksdb::Slice valueSlice = (*itHandle)->iterator->value();
				// TODO: use a shared buffer
				NAPI_STATUS_THROWS(::napi_create_buffer_copy(
					env,
					valueSlice.size(),
					valueSlice.data(),
					nullptr,
					&value
				));

				NAPI_STATUS_THROWS(::napi_set_named_property(env, resultValue, "value", value));
			}

			if ((*itHandle)->reverse) {
				(*itHandle)->iterator->Prev();
			} else {
				(*itHandle)->iterator->Next();
			}
		}
	} else {
		if (!(*itHandle)->iterator->status().ok()) {
			DEBUG_LOG("%p DBIterator::Next iterator not valid/ok: %s\n", itHandle, (*itHandle)->iterator->status().ToString().c_str());
		} else {
			DEBUG_LOG("%p DBIterator::Next iterator no keys found in range\n", (*itHandle).get());
		}

		NAPI_STATUS_THROWS(::napi_get_boolean(env, true, &resultDone));
		NAPI_STATUS_THROWS(::napi_get_undefined(env, &resultValue));
	}

	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "done", resultDone));
	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "value", resultValue));

	return result;
}

/**
 * Called when an control block returns before the iterator is done.
 *
 * @param env - The NAPI environment.
 * @param info - The callback info.
 * @returns An iterator done result.
 */
napi_value DBIterator::Return(napi_env env, napi_callback_info info) {
	NAPI_METHOD_ARGV(1);
	UNWRAP_ITERATOR_HANDLE("Return");

	DEBUG_LOG("%p DBIterator::Return Closing iterator handle\n", (*itHandle).get());

	(*itHandle)->close();

	napi_value result;
	napi_value done;
	napi_value value;
	NAPI_STATUS_THROWS(::napi_create_object(env, &result));
	NAPI_STATUS_THROWS(::napi_get_boolean(env, true, &done));

	napi_valuetype type;
	NAPI_STATUS_THROWS(::napi_typeof(env, argv[0], &type));
	if (type == napi_undefined) {
		NAPI_STATUS_THROWS(::napi_get_undefined(env, &value));
	} else {
		value = argv[0];
	}
	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "done", done));
	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "value", value));

	return result;
}

/**
 * Called when an control block throws an error before the iterator is done.
 *
 * @param env - The NAPI environment.
 * @param info - The callback info.
 * @returns An iterator done result.
 */
napi_value DBIterator::Throw(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_ITERATOR_HANDLE("Throw");

	DEBUG_LOG("%p DBIterator::Throw Closing iterator handle\n", (*itHandle).get());

	// Note: There shouldn't be any need to abort the transaction here since the error
	// will bubble up to the transaction callback handler and abort the transaction.

	(*itHandle)->close();

	napi_value result;
	napi_value done;
	napi_value value;
	NAPI_STATUS_THROWS(::napi_create_object(env, &result));
	NAPI_STATUS_THROWS(::napi_get_boolean(env, true, &done));
	NAPI_STATUS_THROWS(::napi_get_undefined(env, &value));
	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "done", done));
	NAPI_STATUS_THROWS(::napi_set_named_property(env, result, "value", value));
	return result;
}

/**
 * Initializes the `NativeIterator` JavaScript class.
 */
void DBIterator::Init(napi_env env, napi_value exports) {
	napi_property_descriptor properties[] = {
		{ "next", nullptr, Next, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "return", nullptr, Return, nullptr, nullptr, nullptr, napi_default, nullptr },
		{ "throw", nullptr, Throw, nullptr, nullptr, nullptr, napi_default, nullptr },
	};

	auto className = "Iterator";
	constexpr size_t len = sizeof("Iterator") - 1;

	DEBUG_LOG("DBIterator::Init exports=%p\n", exports);

	napi_ref exportsRef;
	NAPI_STATUS_THROWS_VOID(::napi_create_reference(env, exports, 1, &exportsRef));

	napi_value ctor;
	NAPI_STATUS_THROWS_VOID(::napi_define_class(
		env,
		className,               // className
		len,                     // length of class name
		DBIterator::Constructor, // constructor
		(void*)exportsRef,       // constructor arg
		sizeof(properties) / sizeof(napi_property_descriptor), // number of properties
		properties,              // properties array
		&ctor                    // [out] constructor
	));

	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, className, ctor));
}

}
