#ifndef __CORE_BACKGROUND_ERROR_H__
#define __CORE_BACKGROUND_ERROR_H__

#include <string>
#include <string_view>
#include "rocksdb/listener.h"
#include "rocksdb/status.h"

namespace rocksdb_js {

/**
 * Helpers for describing a RocksDB *background* error to JavaScript. When a
 * write fails at the filesystem level (e.g. a full disk or exhausted quota),
 * RocksDB records a background error and — for a hard-or-worse severity — stops
 * accepting writes until recovery (its own auto-recovery for retryable errors,
 * or an explicit `DB::Resume()`). This binding surfaces that by emitting an
 * `'error'` event on the owning database with a JS `Error` describing it
 * (HarperFast/rocksdb-js#730); nothing is persisted, so there is no shared
 * state to synchronize.
 *
 * These are Node-free (no `node_api.h`) so they can be exercised by GoogleTest.
 */

/**
 * Human-readable name for a `rocksdb::Status::Severity` value: "none", "soft",
 * "hard", "fatal", "unrecoverable", or "unknown".
 */
const char* backgroundErrorSeverityName(int severity);

/**
 * Human-readable name for a `rocksdb::BackgroundErrorReason` value (e.g.
 * "flush", "compaction", "memtable"). Returns "unknown" for -1 or any value
 * this binding does not recognize.
 */
const char* backgroundErrorReasonName(int reason);

/**
 * Whether a severity has disabled writes (`>= kHardError`). A soft error
 * (`kSoftError` == 1) is auto-recoverable and does NOT stop writes. (Distinct
 * from a database opened in read-only mode — this is RocksDB halting writes in
 * response to a background failure.)
 */
bool backgroundErrorDisablesWrites(int severity);

/**
 * Serializes a background error's fields into the JSON object string stored on
 * the `DBDescriptor` and later reconstructed into a `BackgroundError` instance
 * on the JS thread (see `napi/background_error.h`). Storing a plain string keeps
 * the descriptor free of any N-API/`napi_env` state, so `OnBackgroundError` can
 * write it from a RocksDB background thread without thread/env hazards.
 *
 * Shape: `{"type":"background","message":...,"severity":N,"severityName":...,
 * "writesDisabled":bool[,"reason":N,"reasonName":...]}`. `reason`/`reasonName`
 * are omitted when `reason < 0`. The `type` discriminator lets `getLastError()`
 * decide which error class to build. Node-free (GoogleTest-covered).
 */
std::string backgroundErrorToJson(
	std::string_view message,
	int severity,
	std::string_view severityName,
	bool writesDisabled,
	int reason,
	std::string_view reasonName
);

} // namespace rocksdb_js

#endif
