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
	values(true),
	needsStableValueBuffer(false)
{}

void DBIteratorOptions::setReadOptionDefaults() {
	this->readOptions.adaptive_readahead = true;
	this->readOptions.async_io = true;
	this->readOptions.auto_readahead_size = true;
	this->readOptions.background_purge_on_iterator_cleanup = true;
	this->readOptions.fill_cache = false;
	this->readOptions.readahead_size = 0;
	this->readOptions.tailing = false;
}

napi_status DBIteratorOptions::initReadOptionsFromObject(napi_env env, napi_value options) {
	NAPI_STATUS_RETURN(rocksdb_js::getProperty(env, options, "adaptiveReadahead", this->readOptions.adaptive_readahead));
	NAPI_STATUS_RETURN(rocksdb_js::getProperty(env, options, "asyncIO", this->readOptions.async_io));
	NAPI_STATUS_RETURN(rocksdb_js::getProperty(env, options, "autoReadaheadSize", this->readOptions.auto_readahead_size));
	NAPI_STATUS_RETURN(rocksdb_js::getProperty(env, options, "backgroundPurgeOnIteratorCleanup", this->readOptions.background_purge_on_iterator_cleanup));
	NAPI_STATUS_RETURN(rocksdb_js::getProperty(env, options, "fillCache", this->readOptions.fill_cache));
	NAPI_STATUS_RETURN(rocksdb_js::getProperty(env, options, "readaheadSize", this->readOptions.readahead_size));
	NAPI_STATUS_RETURN(rocksdb_js::getProperty(env, options, "tailing", this->readOptions.tailing));
	return napi_ok;
}

napi_status DBIteratorOptions::initFromNapiObject(napi_env env, napi_value options) {
	NAPI_STATUS_THROWS_RVAL(rocksdb_js::getProperty(env, options, "exclusiveStart", this->exclusiveStart), napi_invalid_arg);
	NAPI_STATUS_THROWS_RVAL(rocksdb_js::getProperty(env, options, "inclusiveEnd", this->inclusiveEnd), napi_invalid_arg);

	this->setReadOptionDefaults();

	NAPI_ASSERT_OBJECT_OR_UNDEFINED(options, "Invalid options");
	NAPI_STATUS_THROWS_RVAL(this->initReadOptionsFromObject(env, options), napi_invalid_arg);
	NAPI_STATUS_THROWS_RVAL(rocksdb_js::getKeyFromProperty(env, options, "start", "Invalid start key", this->startKeyStr, this->startKeyStart, this->startKeyEnd), napi_invalid_arg);
	NAPI_STATUS_THROWS_RVAL(rocksdb_js::getKeyFromProperty(env, options, "end", "Invalid end key", this->endKeyStr, this->endKeyStart, this->endKeyEnd), napi_invalid_arg);

	return napi_ok;
}

/**
 * Creates a new `NativeIterator` object.
 *
 * Fast path arguments (avoids per-iterator NAPI property lookups):
 *   argv[0] - the `Database` or `Transaction` instance
 *   argv[1] - flags bitmask (see `ITERATOR_*_FLAG`)
 *   argv[2] - startKeyEnd (uint32) - end position of start key in the shared
 *             default key buffer; 0 means no start key
 *   argv[3] - endKeyStart (uint32) - start position of end key in the shared
 *             default key buffer
 *   argv[4] - endKeyEnd (uint32) - end position of end key; if equal to
 *             endKeyStart there is no end key
 *   argv[5] - optional advanced ReadOptions object (rare path)
 */
napi_value DBIterator::Constructor(napi_env env, napi_callback_info info) {
	NAPI_CONSTRUCTOR_ARGV_WITH_DATA("Iterator", 6);

	napi_ref exportsRef = reinterpret_cast<napi_ref>(data);
	napi_value exports;
	NAPI_STATUS_THROWS(::napi_get_reference_value(env, exportsRef, &exports));

	napi_value databaseCtor;
	NAPI_STATUS_THROWS(::napi_get_named_property(env, exports, "Database", &databaseCtor));

	bool isDatabase = false;
	NAPI_STATUS_THROWS(::napi_instanceof(env, argv[0], databaseCtor, &isDatabase));

	uint32_t flags = 0;
	NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[1], &flags));

	uint32_t startKeyEnd = 0;
	NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[2], &startKeyEnd));

	uint32_t endKeyStart = 0;
	NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[3], &endKeyStart));

	uint32_t endKeyEnd = 0;
	NAPI_STATUS_THROWS(::napi_get_value_uint32(env, argv[4], &endKeyEnd));

	DBIteratorOptions itOptions;
	itOptions.reverse                = (flags & ITERATOR_REVERSE_FLAG) != 0;
	itOptions.inclusiveEnd           = (flags & ITERATOR_INCLUSIVE_END_FLAG) != 0;
	itOptions.exclusiveStart         = (flags & ITERATOR_EXCLUSIVE_START_FLAG) != 0;
	itOptions.values                 = (flags & ITERATOR_INCLUDE_VALUES_FLAG) != 0;
	itOptions.needsStableValueBuffer = (flags & ITERATOR_NEEDS_STABLE_VALUE_BUFFER_FLAG) != 0;

	itOptions.setReadOptionDefaults();

	// Optional advanced ReadOptions (rare path)
	napi_valuetype optionsType = napi_undefined;
	if (argc > 5) {
		NAPI_STATUS_THROWS(::napi_typeof(env, argv[5], &optionsType));
		if (optionsType == napi_object) {
			NAPI_STATUS_THROWS(itOptions.initReadOptionsFromObject(env, argv[5]));
		}
	}

	std::shared_ptr<DBIteratorHandle>* itHandle = nullptr;
	std::shared_ptr<DBHandle>* dbHandle = nullptr;
	TransactionHandle* txnHandlePtr = nullptr;

	if (isDatabase) {
		NAPI_STATUS_THROWS(::napi_unwrap(env, argv[0], reinterpret_cast<void**>(&dbHandle)));
		DEBUG_LOG("DBIterator::Constructor Initializing iterator handle with Database instance (dbHandle=%p)\n", (*dbHandle).get());
		if (dbHandle == nullptr || !(*dbHandle)->opened()) {
			::napi_throw_error(env, nullptr, "Database not open");
			return nullptr;
		}
	} else {
		DEBUG_LOG("DBIterator::Constructor Using existing transaction handle\n");
		napi_value transactionCtor;
		NAPI_STATUS_THROWS(::napi_get_named_property(env, exports, "Transaction", &transactionCtor));

		bool isTransaction = false;
		NAPI_STATUS_THROWS(::napi_instanceof(env, argv[0], transactionCtor, &isTransaction));

		if (!isTransaction) {
			napi_valuetype type;
			NAPI_STATUS_THROWS(::napi_typeof(env, argv[0], &type));
			std::string errorMsg = "Invalid context, expected Database or Transaction instance, got type " + std::to_string(type);
			::napi_throw_error(env, nullptr, errorMsg.c_str());
			return nullptr;
		}

		std::shared_ptr<TransactionHandle>* txnHandle = nullptr;
		NAPI_STATUS_THROWS(::napi_unwrap(env, argv[0], reinterpret_cast<void**>(&txnHandle)));
		txnHandlePtr = (*txnHandle).get();
		dbHandle = &txnHandlePtr->dbHandle;
	}

	// Resolve start/end key pointers from the shared default key buffer
	char* keyBufferPtr = (*dbHandle)->defaultKeyBufferPtr;
	if (startKeyEnd > 0) {
		if (keyBufferPtr == nullptr) {
			::napi_throw_error(env, nullptr, "Default key buffer is not set");
			return nullptr;
		}
		itOptions.startKeyStr = keyBufferPtr;
		itOptions.startKeyStart = 0;
		itOptions.startKeyEnd = startKeyEnd;
	}
	if (endKeyEnd > endKeyStart) {
		if (keyBufferPtr == nullptr) {
			::napi_throw_error(env, nullptr, "Default key buffer is not set");
			return nullptr;
		}
		itOptions.endKeyStr = keyBufferPtr;
		itOptions.endKeyStart = endKeyStart;
		itOptions.endKeyEnd = endKeyEnd;
	}

	try {
		if (txnHandlePtr) {
			itHandle = new std::shared_ptr<DBIteratorHandle>(std::make_shared<DBIteratorHandle>(txnHandlePtr, itOptions));
		} else {
			itHandle = new std::shared_ptr<DBIteratorHandle>(std::make_shared<DBIteratorHandle>(*dbHandle, itOptions));
		}
	} catch (const std::exception& e) {
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
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
 * Builds a slow-path object `{ key: Buffer, value?: Buffer }` for the rare case
 * where the shared key/value buffers cannot be used (oversized data or stable
 * buffer required by the decoder).
 */
static napi_value buildSlowResult(
	napi_env env,
	rocksdb::Slice& keySlice,
	bool includeValue,
	rocksdb::Slice& valueSlice
) {
	napi_value resultObj;
	NAPI_STATUS_THROWS(::napi_create_object(env, &resultObj));

	napi_value keyVal;
	NAPI_STATUS_THROWS(::napi_create_buffer_copy(env, keySlice.size(), keySlice.data(), nullptr, &keyVal));
	NAPI_STATUS_THROWS(::napi_set_named_property(env, resultObj, "key", keyVal));

	if (includeValue) {
		napi_value valueVal;
		NAPI_STATUS_THROWS(::napi_create_buffer_copy(env, valueSlice.size(), valueSlice.data(), nullptr, &valueVal));
		NAPI_STATUS_THROWS(::napi_set_named_property(env, resultObj, "value", valueVal));
	}

	return resultObj;
}

/**
 * Advances the iterator to the next key/value pair.
 *
 * Returns one of:
 *   `ITERATOR_RESULT_DONE` (uint32 0) - iterator exhausted
 *   `ITERATOR_RESULT_FAST` (uint32 1) - key copied into the shared default key
 *     buffer; value (when included) copied into the shared default value
 *     buffer. Lengths are written to `iteratorStatePtr` (a `Uint32Array(2)`):
 *       state[0] = key length
 *       state[1] = value length (only meaningful when values are included)
 *   `{ key: Buffer, value?: Buffer }` - slow fallback path used when the data
 *     does not fit in the shared buffers, or when the decoder needs a stable
 *     value buffer.
 */
napi_value DBIterator::Next(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_ITERATOR_HANDLE("Next");

	auto& it = *itHandle;
	napi_value result;

	if (!it->iterator->Valid()) {
		if (!it->iterator->status().ok()) {
			DEBUG_LOG("%p DBIterator::Next iterator not valid/ok: %s\n", itHandle, it->iterator->status().ToString().c_str());
		} else {
			DEBUG_LOG("%p DBIterator::Next iterator no keys found in range\n", it.get());
		}
		NAPI_STATUS_THROWS(::napi_create_uint32(env, ITERATOR_RESULT_DONE, &result));
		return result;
	}

	rocksdb::Slice keySlice = it->iterator->key();

	// Edge case: reverse + exclusiveStart and we just landed on the start key
	// (which is the lower bound for reverse iteration). We need to peek ahead
	// to determine whether this is the last item.
	if (it->reverse && it->exclusiveStart &&
	    it->startKey.size() > 0 && keySlice.compare(it->startKey) == 0) {
		it->iterator->Prev();
		if (!it->iterator->Valid()) {
			NAPI_STATUS_THROWS(::napi_create_uint32(env, ITERATOR_RESULT_DONE, &result));
			return result;
		}
		// not the last item; restore position and continue normally below
		it->iterator->Next();
		keySlice = it->iterator->key();
	}

	// Try the fast path: copy key (and optionally value) into the shared
	// default buffers and write lengths to the iterator state buffer.
	auto& dbHandle = it->dbHandle;
	char* keyBuffer = dbHandle->defaultKeyBufferPtr;
	size_t keyBufferLength = dbHandle->defaultKeyBufferLength;
	char* valueBuffer = dbHandle->defaultValueBufferPtr;
	size_t valueBufferLength = dbHandle->defaultValueBufferLength;
	uint32_t* state = reinterpret_cast<uint32_t*>(dbHandle->iteratorStatePtr);

	bool keyFitsInShared = (keyBuffer != nullptr) && (state != nullptr) && (keySlice.size() <= keyBufferLength);
	rocksdb::Slice valueSlice;
	bool valueFitsInShared = true;

	if (it->values) {
		valueSlice = it->iterator->value();
		valueFitsInShared = !it->needsStableValueBuffer
			&& valueBuffer != nullptr
			&& valueSlice.size() <= valueBufferLength;
	}

	if (keyFitsInShared && valueFitsInShared) {
		// Fast path
		::memcpy(keyBuffer, keySlice.data(), keySlice.size());
		state[0] = static_cast<uint32_t>(keySlice.size());
		if (it->values) {
			::memcpy(valueBuffer, valueSlice.data(), valueSlice.size());
			state[1] = static_cast<uint32_t>(valueSlice.size());
		}
		if (it->reverse) {
			it->iterator->Prev();
		} else {
			it->iterator->Next();
		}
		NAPI_STATUS_THROWS(::napi_create_uint32(env, ITERATOR_RESULT_FAST, &result));
		return result;
	}

	// Slow path: at least one of key or value can't go in the shared buffer.
	napi_value slowResult = buildSlowResult(env, keySlice, it->values, valueSlice);
	if (it->reverse) {
		it->iterator->Prev();
	} else {
		it->iterator->Next();
	}
	return slowResult;
}

/**
 * Called when the consuming code returns before the iterator is done. The JS
 * wrapper constructs the `{ done: true, value }` result, so this method just
 * closes the iterator and returns undefined.
 */
napi_value DBIterator::Return(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_ITERATOR_HANDLE("Return");

	DEBUG_LOG("%p DBIterator::Return Closing iterator handle\n", (*itHandle).get());
	(*itHandle)->close();

	NAPI_RETURN_UNDEFINED();
}

/**
 * Called when the consuming code throws before the iterator is done. The JS
 * wrapper rethrows after this returns.
 */
napi_value DBIterator::Throw(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_ITERATOR_HANDLE("Throw");

	DEBUG_LOG("%p DBIterator::Throw Closing iterator handle\n", (*itHandle).get());
	(*itHandle)->close();

	NAPI_RETURN_UNDEFINED();
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
