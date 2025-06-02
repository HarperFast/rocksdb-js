#ifndef __DB_ITERATOR_H__
#define __DB_ITERATOR_H__

#include <memory>
#include <node_api.h>
#include "db_handle.h"
#include "rocksdb/iterator.h"

namespace rocksdb_js {

// forward declare DBHandle because of circular dependency
struct DBHandle;

struct DBIteratorOptions final {
	DBIteratorOptions();
	napi_status initFromNapiObject(napi_env env, napi_value options);

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
