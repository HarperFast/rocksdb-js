#include "stats/rocksdb_stats.h"
#include "napi/macros.h"
#include "core/platform.h"
#include "napi/helpers.h"
#include "napi/async.h"
#include "transaction_log/transaction_log_store.h"

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
	// count the summarized txnlog.* tickers (logCount + the X-macro entries) so
	// the array is sized to include both the RocksDB and transaction log names.
	size_t txnlogCount = 1; // TRANSACTION_LOG_SUMMARY_LOG_COUNT_KEY
#define X(key, field) txnlogCount++;
	TRANSACTION_LOG_SUMMARY_STATS(X)
#undef X

	napi_value result;
	NAPI_STATUS_THROWS(::napi_create_array_with_length(env, rocksdb::TickersNameMap.size() + txnlogCount, &result));

	uint32_t i = 0;
	for (const auto& p : rocksdb::TickersNameMap) {
		napi_value value;
		NAPI_STATUS_THROWS(::napi_create_string_utf8(env, p.second.c_str(), p.second.size(), &value));
		NAPI_STATUS_THROWS(::napi_set_element(env, result, i, value));
		i++;
	}

	// append the transaction log summary tickers (also returned by db.getStats()
	// and resolvable via db.getStat()).
	napi_value txnlogKey;
	NAPI_STATUS_THROWS(::napi_create_string_utf8(env, TRANSACTION_LOG_SUMMARY_LOG_COUNT_KEY, NAPI_AUTO_LENGTH, &txnlogKey));
	NAPI_STATUS_THROWS(::napi_set_element(env, result, i++, txnlogKey));
#define X(key, field) \
	{ \
		napi_value _key; \
		NAPI_STATUS_THROWS(::napi_create_string_utf8(env, key, NAPI_AUTO_LENGTH, &_key)); \
		NAPI_STATUS_THROWS(::napi_set_element(env, result, i++, _key)); \
	}
	TRANSACTION_LOG_SUMMARY_STATS(X)
#undef X

	return result;
}

} // namespace rocksdb_js
