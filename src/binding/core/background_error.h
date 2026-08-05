#ifndef __CORE_BACKGROUND_ERROR_H__
#define __CORE_BACKGROUND_ERROR_H__

#include <string>
#include "rocksdb/listener.h"
#include "rocksdb/status.h"

namespace rocksdb_js {

/**
 * A snapshot of a RocksDB *background* error. When a write fails at the
 * filesystem level (e.g. a full disk or exhausted quota), RocksDB latches a
 * background error and refuses all further writes until it is cleared — either
 * by its own auto-recovery (retryable errors only) or by an explicit
 * `DB::Resume()`. This mirrors that latched state so a consumer can observe it
 * and attempt in-process recovery instead of restarting the process
 * (HarperFast/rocksdb-js#730).
 *
 * `reason` is a `rocksdb::BackgroundErrorReason` cast to int, or -1 when the
 * error was recorded outside a reason-bearing callback (e.g. a failed resume).
 */
struct BackgroundErrorInfo {
	bool latched = false;
	std::string message;
	int severity = 0; // rocksdb::Status::Severity (0 = kNoError)
	int reason = -1;
};

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

} // namespace rocksdb_js

#endif
