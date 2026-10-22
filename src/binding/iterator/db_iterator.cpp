#include "database/db_handle.h"
#include "database/db_descriptor.h"
#include "iterator/db_iterator.h"
#include "iterator/db_iterator_handle.h"
#include "napi/macros.h"
#include "transaction/transaction.h"
#include "core/platform.h"
#include "core/test_seam.h"
#include "napi/helpers.h"
#include "napi/async.h"
#include <chrono>
#include <thread>

namespace rocksdb_js {

namespace {

/**
 * The native data behind a `NativeIterator` JS object. A pooled handle leaves
 * it when the JS iterator finishes or returns, so a retained JS iterator can
 * never drive a handle the pool has since lent to another range.
 */
struct IteratorWrap final {
	std::shared_ptr<DBIteratorHandle> handle;
	// Set when a pooled handle left on exhaustion: next() keeps reporting done
	// until the transaction closes its iterators, when a closed iterator would
	// start throwing.
	std::weak_ptr<TransactionHandle> exhaustedIn;
	uint64_t exhaustedEpoch = 0;
};

/**
 * Ends a pooled handle's range and gives it back to its transaction's pool, or
 * closes it when it cannot be pooled. Takes the handle out of `wrap`.
 */
void releasePooled(IteratorWrap* wrap, std::unique_lock<std::mutex>& iteratorLock, bool pool) {
	std::shared_ptr<DBIteratorHandle> handle = std::move(wrap->handle);
	std::shared_ptr<TransactionHandle> txnHandle = handle->txnHandle;
	bool released = false;
	bool checkOrphan = false;
	if (pool && txnHandle && handle->iterator) {
		handle->endRange();
		released = txnHandle->releaseIterator(handle.get(), checkOrphan);
	}
	iteratorLock.unlock();
	if (!released) {
		handle->close();
		handle->detachFromDescriptor();
	}
	if (checkOrphan) {
		txnHandle->closeOrphanIfUnused();
	}
}

}

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
 *   argv[0] - the `Database` or `Transaction` instance; the JS layer
 *             communicates which via `ITERATOR_CONTEXT_IS_TRANSACTION_FLAG`
 *             so the native constructor doesn't need an instanceof check
 *   argv[1] - flags bitmask (see `ITERATOR_*_FLAG`)
 *   argv[2] - startKeyEnd (uint32) - end position of start key in the shared
 *             default key buffer; 0 means no start key
 *   argv[3] - endKeyStart (uint32) - start position of end key in the shared
 *             default key buffer
 *   argv[4] - endKeyEnd (uint32) - end position of end key; if equal to
 *             endKeyStart there is no end key
 *   argv[5] - optional advanced ReadOptions object (rare path)
 *   argv[6] - optional transaction ID for a Database context
 */
napi_value DBIterator::Constructor(napi_env env, napi_callback_info info) {
	NAPI_CONSTRUCTOR_ARGV("Iterator", 7);

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
	bool hasAdvancedOptions = false;
	if (argc > 5) {
		napi_valuetype optionsType = napi_undefined;
		NAPI_STATUS_THROWS(::napi_typeof(env, argv[5], &optionsType));
		if (optionsType == napi_object) {
			hasAdvancedOptions = true;
			NAPI_STATUS_THROWS(itOptions.initReadOptionsFromObject(env, argv[5]));
		}
	}

	// Trust the JS layer's IS_TRANSACTION flag instead of running
	// napi_instanceof against the Database / Transaction constructors. JS
	// always knows the concrete class of `_context` (set in the DBI base
	// class constructor), so this is a safe optimization that avoids
	// napi_get_reference_value + napi_get_named_property + napi_instanceof
	// per iterator construction.
	const bool isTransaction = (flags & ITERATOR_CONTEXT_IS_TRANSACTION_FLAG) != 0;
	const bool hasTransactionId = (flags & ITERATOR_HAS_TRANSACTION_ID_FLAG) != 0;

	IteratorWrap* wrap = nullptr;
	std::shared_ptr<DBHandle>* dbHandle = nullptr;
	std::shared_ptr<TransactionHandle> txnHandle;

	if (isTransaction) {
		std::shared_ptr<TransactionHandle>* wrappedTxnHandle = nullptr;
		NAPI_STATUS_THROWS(::napi_unwrap(env, argv[0], reinterpret_cast<void**>(&wrappedTxnHandle)));
		if (wrappedTxnHandle == nullptr || *wrappedTxnHandle == nullptr) {
			::napi_throw_error(env, nullptr, "Invalid transaction handle");
			return nullptr;
		}
		txnHandle = *wrappedTxnHandle;
		if (!txnHandle->dbHandle) {
			::napi_throw_error(env, nullptr, "Transaction is not in pending state");
			return nullptr;
		}
		if (!txnHandle->dbHandle->opened()) {
			::napi_throw_error(env, nullptr, "Database not open");
			return nullptr;
		}
		dbHandle = &txnHandle->dbHandle;
		DEBUG_LOG("DBIterator::Constructor txnHandle=%p dbHandle=%p\n", txnHandle.get(), dbHandle->get());
	} else {
		NAPI_STATUS_THROWS(::napi_unwrap(env, argv[0], reinterpret_cast<void**>(&dbHandle)));
		if (dbHandle == nullptr || !(*dbHandle)->opened()) {
			::napi_throw_error(env, nullptr, "Database not open");
			return nullptr;
		}
		DEBUG_LOG("DBIterator::Constructor Initializing iterator handle with Database instance (dbHandle=%p)\n", (*dbHandle).get());

		if (hasTransactionId) {
			if (argc <= 6) {
				::napi_throw_type_error(env, nullptr, "Invalid transaction");
				return nullptr;
			}
			uint64_t txnId = 0;
			if (!rocksdb_js::readTransactionId(env, argv[6], txnId)) {
				return nullptr;
			}
			txnHandle = (*dbHandle)->descriptor->transactionGet(txnId);
			if (!txnHandle) {
				std::string errorMsg = "Iterator failed: Transaction not found (txnId: " + std::to_string(txnId) + ")";
				::napi_throw_error(env, nullptr, errorMsg.c_str());
				return nullptr;
			}
		}
	}
	const int setupDelayMs = iteratorSetupDelayMsFlag().load(std::memory_order_relaxed);
	if (setupDelayMs > 0) {
		std::this_thread::sleep_for(std::chrono::milliseconds(setupDelayMs));
	}
	auto descriptor = (*dbHandle)->descriptor;
	if (!descriptor) {
		::napi_throw_error(env, nullptr, "Database not open");
		return nullptr;
	}
	OperationGuard operationGuard(descriptor);
	if (descriptor->isClosing()) {
		::napi_throw_error(env, nullptr, "Database is closing");
		return nullptr;
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

	bool attach = true;
	try {
		std::shared_ptr<DBIteratorHandle> iteratorHandle;
		if (txnHandle) {
			std::shared_ptr<DBHandle> dbHandleOverride = isTransaction ? nullptr : *dbHandle;
			if (!hasAdvancedOptions && iteratorPoolEnabled()) {
				bool created = false;
				iteratorHandle = txnHandle->acquireIterator(itOptions, std::move(dbHandleOverride), created);
				// A reused pooled handle is still attached from when the pool created it.
				attach = !iteratorHandle->pooled || created;
			} else {
				iteratorHandle = txnHandle->createIterator(itOptions, std::move(dbHandleOverride));
			}
		} else {
			iteratorHandle = std::make_shared<DBIteratorHandle>(*dbHandle, itOptions);
		}
		wrap = new IteratorWrap{ std::move(iteratorHandle) };
	} catch (const std::exception& e) {
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}

	// attach this iterator to the descriptor so it gets cleaned up when the descriptor is closed
	if (attach && wrap->handle->dbHandle && wrap->handle->dbHandle->descriptor) {
		wrap->handle->dbHandle->descriptor->attach(wrap->handle);
		wrap->handle->attached.store(true);
	}

	DEBUG_LOG("DBIterator::Constructor wrap=%p\n", wrap);

	try {
		NAPI_STATUS_THROWS(::napi_wrap(
			env,
			jsThis,
			reinterpret_cast<void*>(wrap),
			[](napi_env env, void* data, void* hint) {
				DEBUG_LOG("DBIterator::Constructor NativeIterator GC'd wrap=%p\n", data);
				auto* wrap = static_cast<IteratorWrap*>(data);
				if (wrap->handle) {
					// Close before detaching: while attached, a concurrent
					// destroy()/shutdown() sweep sees this iterator in
					// `closables` and calls its close() too, serialized
					// against this one by `iteratorMutex` (closeIfOpen()
					// is idempotent). Detaching first would let that sweep
					// skip this iterator and reset the DB out from under a
					// still-running native `Reset()` here.
					wrap->handle->close();
					wrap->handle->detachFromDescriptor();
				}
				delete wrap;
			},
			nullptr, // finalize_hint
			nullptr  // result
		));

		return jsThis;
	} catch (const std::exception& e) {
		if (wrap->handle) {
			wrap->handle->close();
			wrap->handle->detachFromDescriptor();
		}
		delete wrap;
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}
}

#define UNWRAP_ITERATOR_WRAP() \
	IteratorWrap* wrap = nullptr; \
	NAPI_STATUS_THROWS(::napi_unwrap(env, jsThis, reinterpret_cast<void**>(&wrap))); \
	if (!wrap) { \
		::napi_throw_error(env, nullptr, "Iterator not initialized"); \
		return nullptr; \
	}

/**
 * Closes the iterator, or gives a pooled one back to its transaction's pool.
 * A later `next()` throws "not initialized" either way.
 */
static void closeOrRelease(IteratorWrap* wrap) {
	wrap->exhaustedIn.reset();
	if (!wrap->handle) {
		return;
	}
	if (!wrap->handle->pooled) {
		wrap->handle->close();
		return;
	}
	std::unique_lock<std::mutex> iteratorLock(wrap->handle->iteratorMutex);
	const bool healthy = wrap->handle->iterator && wrap->handle->iterator->status().ok();
	releasePooled(wrap, iteratorLock, healthy);
}

static napi_value nextImpl(napi_env env, IteratorWrap* wrap);

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
	UNWRAP_ITERATOR_WRAP();
	try {
		return nextImpl(env, wrap);
	} catch (const std::exception& e) {
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}
}

static napi_value nextImpl(napi_env env, IteratorWrap* wrap) {
	if (!wrap->handle) {
		auto txnHandle = wrap->exhaustedIn.lock();
		if (txnHandle && !txnHandle->closed.load()
			&& txnHandle->iteratorEpoch.load(std::memory_order_relaxed) == wrap->exhaustedEpoch
		) {
			napi_value result;
			NAPI_STATUS_THROWS(::napi_create_uint32(env, ITERATOR_RESULT_DONE, &result));
			return result;
		}
		::napi_throw_error(env, nullptr, "Next failed: Iterator not initialized");
		return nullptr;
	}
	auto& it = wrap->handle;
	std::unique_lock<std::mutex> iteratorLock(it->iteratorMutex);
	// Test-only: widen the window where a foreign finishClose()'s closables
	// sweep is blocked on iteratorMutex behind this call, so a fixture can
	// reliably position a forced close mid-Next() rather than only ever
	// between calls.
	const int nextDelayMs = iteratorNextDelayMsFlag().load(std::memory_order_relaxed);
	if (nextDelayMs > 0) {
		std::this_thread::sleep_for(std::chrono::milliseconds(nextDelayMs));
	}
	if (!it->iterator) {
		::napi_throw_error(env, nullptr, "Next failed: Iterator not initialized");
		return nullptr;
	}
	napi_value result;

	if (!it->valid()) {
		// An iterator stops being valid for one of two reasons, and only `status()`
		// tells them apart: it ran out of range, or it failed (a block checksum
		// mismatch, an I/O error). A failure has to reach JavaScript as an error, the
		// way `get()` reports the same corruption; returning DONE would make a
		// damaged file read as the end of the data.
		if (it->iterator && !it->iterator->status().ok()) {
			DEBUG_LOG("%p DBIterator::Next iterator failed: %s\n", it.get(), it->iterator->status().ToString().c_str());
			ROCKSDB_STATUS_THROWS_ERROR_LIKE(it->iterator->status(), "Iterator failed");
			return nullptr;
		}
		DEBUG_LOG("%p DBIterator::Next iterator no keys found in range\n", it.get());
		if (it->pooled) {
			std::shared_ptr<TransactionHandle> txnHandle = it->txnHandle;
			if (txnHandle) {
				wrap->exhaustedIn = txnHandle;
				wrap->exhaustedEpoch = txnHandle->iteratorEpoch.load(std::memory_order_relaxed);
			}
			releasePooled(wrap, iteratorLock, true);
		}
		NAPI_STATUS_THROWS(::napi_create_uint32(env, ITERATOR_RESULT_DONE, &result));
		return result;
	}

	rocksdb::Slice keySlice = it->iterator->key();

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
		it->advance();
		NAPI_STATUS_THROWS(::napi_create_uint32(env, ITERATOR_RESULT_FAST, &result));
		return result;
	}

	// Slow path: at least one of key or value can't go in the shared buffer.
	napi_value slowResult = buildSlowResult(env, keySlice, it->values, valueSlice);
	it->advance();
	return slowResult;
}

/**
 * Called when the consuming code returns before the iterator is done. The JS
 * wrapper constructs the `{ done: true, value }` result, so this method just
 * closes the iterator and returns undefined.
 */
napi_value DBIterator::Return(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_ITERATOR_WRAP();
	try {
		closeOrRelease(wrap);
	} catch (const std::exception& e) {
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}
	NAPI_RETURN_UNDEFINED();
}

/**
 * Called when the consuming code throws before the iterator is done. The JS
 * wrapper rethrows after this returns.
 */
napi_value DBIterator::Throw(napi_env env, napi_callback_info info) {
	NAPI_METHOD();
	UNWRAP_ITERATOR_WRAP();
	try {
		closeOrRelease(wrap);
	} catch (const std::exception& e) {
		::napi_throw_error(env, nullptr, e.what());
		return nullptr;
	}
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

	// No constructor data is needed: the JS layer passes
	// ITERATOR_CONTEXT_IS_TRANSACTION_FLAG so the constructor never has to
	// reach back into `exports` for the Database / Transaction class refs.
	napi_value ctor;
	NAPI_STATUS_THROWS_VOID(::napi_define_class(
		env,
		className,               // className
		len,                     // length of class name
		DBIterator::Constructor, // constructor
		nullptr,                 // constructor data (unused)
		sizeof(properties) / sizeof(napi_property_descriptor), // number of properties
		properties,              // properties array
		&ctor                    // [out] constructor
	));

	NAPI_STATUS_THROWS_VOID(::napi_set_named_property(env, exports, className, ctor));
}

}
