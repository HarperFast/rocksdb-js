#ifndef __DB_ITERATOR_H__
#define __DB_ITERATOR_H__

#include <memory>
#include <node_api.h>
#include "db_handle.h"
#include "rocksdb/iterator.h"

namespace rocksdb_js {

// Iterator flags (bitmask) passed from JS to the iterator constructor
#define ITERATOR_REVERSE_FLAG                    0x01
#define ITERATOR_INCLUSIVE_END_FLAG              0x02
#define ITERATOR_EXCLUSIVE_START_FLAG            0x04
#define ITERATOR_INCLUDE_VALUES_FLAG             0x08
#define ITERATOR_NEEDS_STABLE_VALUE_BUFFER_FLAG  0x10
// Set by JS when `context` (argv[0]) is a NativeTransaction instance.
// When unset, `context` is treated as a NativeDatabase instance. The native
// constructor relies on this flag to skip the expensive
// napi_get_named_property + napi_instanceof type checks.
#define ITERATOR_CONTEXT_IS_TRANSACTION_FLAG     0x20

// Iterator Next() return signals
#define ITERATOR_RESULT_DONE 0
#define ITERATOR_RESULT_FAST 1

// forward declare DBHandle because of circular dependency
struct DBHandle;

struct DBIteratorOptions final {
	DBIteratorOptions();
	napi_status initFromNapiObject(napi_env env, napi_value options);

	/**
	 * Sets default read options for iterators (matches what
	 * `initFromNapiObject` would set when no overrides are provided).
	 */
	void setReadOptionDefaults();

	/**
	 * Reads only the advanced RocksDB ReadOptions properties (e.g.
	 * `adaptiveReadahead`, `asyncIO`, etc.) from a JS options object. Boolean
	 * options for the iterator itself (`reverse`, `values`, `exclusiveStart`,
	 * `inclusiveEnd`) are passed via flags and not via this function.
	 */
	napi_status initReadOptionsFromObject(napi_env env, napi_value options);

	rocksdb::ReadOptions readOptions;

	const char* startKeyStr;
	uint32_t startKeyStart;
	uint32_t startKeyEnd;

	const char* endKeyStr;
	uint32_t endKeyStart;
	uint32_t endKeyEnd;

	bool inclusiveEnd;
	bool exclusiveStart;
	bool reverse;
	bool values;
	bool needsStableValueBuffer;
};

/**
 * The `NativeIterator` JavaScript class implementation.
 */
struct DBIterator final {
	static napi_value Constructor(napi_env env, napi_callback_info info);
	static napi_value Next(napi_env env, napi_callback_info info);
	static napi_value Return(napi_env env, napi_callback_info info);
	static napi_value Throw(napi_env env, napi_callback_info info);

	static void Init(napi_env env, napi_value exports);
};

} // namespace rocksdb_js

#endif
