#ifndef __CORE_BACKGROUND_ERROR_H__
#define __CORE_BACKGROUND_ERROR_H__

#include <cstdint>
#include <mutex>
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

/**
 * Whether a severity represents the hard read-only latch (`>= kHardError`).
 * A soft error (`kSoftError` == 1) is auto-recoverable and does NOT stop
 * writes, so a mirror can hold a soft error that is not read-only.
 */
bool backgroundErrorIsReadOnly(int severity);

/**
 * Thread-safe mirror of RocksDB's latched background error. Every operation is
 * atomic under an internal mutex, and a monotonic generation lets a recovery
 * clear only the error it actually observed — never a newer one latched in
 * between (the reconciliation races RocksDB reports across background threads;
 * see HarperFast/rocksdb-js#730). This lives in a Node-free translation unit so
 * the interleavings can be exercised deterministically by GoogleTest.
 */
class BackgroundErrorMirror {
public:
	/**
	 * Records a latched error (from `OnBackgroundError` or a failed resume).
	 * `reason` is a `rocksdb::BackgroundErrorReason` cast to int, or -1 when
	 * there is no associated reason. Formats the message outside the lock.
	 */
	void latch(int reason, const rocksdb::Status& status);

	/**
	 * Clears the mirror only if its generation still equals `expectedGeneration`
	 * — i.e. no newer error latched since the caller observed it. Returns whether
	 * it cleared. Used by `resume()` so a recovery cannot erase a different error
	 * that latched during `DB::Resume()`.
	 */
	bool clearIfUnchanged(uint64_t expectedGeneration);

	/**
	 * Reconciles an `OnErrorRecoveryEnd` in a single atomic step.
	 * `recoveredMessage`/`recoveredSeverity` describe `old_bg_error` (the error
	 * RocksDB attempted to recover); `recovered` is whether `new_bg_error` is OK.
	 *
	 *  - recovered: clears the mirror ONLY if the currently-latched error IS the
	 *    recovered one (message match), so a newer error latched on another
	 *    thread between recovery start and this callback is preserved.
	 *  - failed: RocksDB keeps `old_bg_error` as the read-only latch, so seed it
	 *    ONLY when nothing is currently latched — never clobber a newer error.
	 */
	void reconcileRecoveryEnd(const std::string& recoveredMessage, int recoveredSeverity, bool recovered);

	/**
	 * Copies the current mirror into `out` and returns whether an error is
	 * latched. When `generation` is non-null, also returns the current
	 * generation for a later `clearIfUnchanged`.
	 */
	bool get(BackgroundErrorInfo& out, uint64_t* generation = nullptr) const;

private:
	mutable std::mutex mutex_;
	BackgroundErrorInfo info_;
	// Bumped on every latch/clear transition (see `clearIfUnchanged`).
	uint64_t generation_ = 0;
};

} // namespace rocksdb_js

#endif
