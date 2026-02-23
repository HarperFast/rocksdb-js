#include "rocksdb_stats.h"
#include "macros.h"
#include "util.h"

namespace rocksdb_js {

napi_value getHistogramNames(napi_env env) {
	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_array_with_length(env, rocksdb::HistogramsNameMap.size(), &result));

	uint32_t i = 0;
	for (const auto& p : rocksdb::HistogramsNameMap) {
		napi_value value;
		NAPI_STATUS_THROWS(::napi_create_string_utf8(env, p.second.c_str(), p.second.size(), &value));
		NAPI_STATUS_THROWS(::napi_set_element(env, result, i, value));
		i++;
	}

	return result;
}

napi_value getTickerNames(napi_env env) {
	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_array_with_length(env, rocksdb::TickersNameMap.size(), &result));

	int i = 0;
	for (const auto& p : rocksdb::TickersNameMap) {
		napi_value value;
		NAPI_STATUS_THROWS(::napi_create_string_utf8(env, p.second.c_str(), p.second.size(), &value));
		NAPI_STATUS_THROWS(::napi_set_element(env, result, i, value));
		i++;
	}

	return result;
}

} // namespace rocksdb_js
