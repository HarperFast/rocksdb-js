#ifndef __ROCKSDB_STATS_H__
#define __ROCKSDB_STATS_H__

#include "rocksdb/statistics.h"
#include "node_api.h"

namespace rocksdb_js {

/**
 * Gets all histogram names.
 *
 * @example
 * ```typescript
 * import { stats } from '@harperfast/rocksdb-js';
 * console.log(stats.histograms);
 * ```
 */
napi_value getHistogramNames(napi_env env);

/**
 * Gets all ticker names.
 *
 * @example
 * ```typescript
 * import { stats } from '@harperfast/rocksdb-js';
 * console.log(stats.tickers);
 * ```
 */
napi_value getTickerNames(napi_env env);

} // namespace rocksdb_js

#endif // __ROCKSDB_STATS_H__
