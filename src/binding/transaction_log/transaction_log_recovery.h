#ifndef __TRANSACTION_LOG_RECOVERY_H__
#define __TRANSACTION_LOG_RECOVERY_H__

#include <cstdint>

namespace rocksdb_js {

struct TransactionLogFile;

/**
 * The outcome of scanning a v1 transaction log file's framing during open-time
 * crash recovery.
 */
struct RecoveryScan final {
	enum class Kind {
		/** Framing is intact end-to-end; nothing to do. */
		Clean,
		/**
		 * A torn/partial entry sits at the tail with no valid framing after it
		 * (e.g. an O_APPEND short write interrupted by a crash). The file should
		 * be truncated to `validEnd` to drop the partial bytes.
		 */
		TruncateTail,
		/**
		 * A framing break sits mid-file with valid entries still following it
		 * (legacy corruption predating the writer fix). Truncating would discard
		 * committed/replicated entries, so the file is left intact and the break
		 * is surfaced instead.
		 */
		MidFileCorruption,
	};

	Kind kind;
	/**
	 * For `TruncateTail`: the offset to truncate to (end of the last valid
	 * entry). For `MidFileCorruption`: the offset of the first broken frame.
	 * For `Clean`: the validated end of the entries.
	 */
	uint32_t validEnd;
	/**
	 * Offset just past the last entry carrying `TRANSACTION_LOG_ENTRY_LAST_FLAG`
	 * — i.e. the end of the last COMPLETE transaction in this file — or 0 if the
	 * file contains no complete transaction. Always <= `validEnd`.
	 *
	 * A batch's entries are written together but only its final entry carries the
	 * flag, so a crash mid-batch can leave whole, well-framed entries that are
	 * only a *prefix* of a transaction. `validEnd` accepts that prefix (the frames
	 * are intact); this is the stricter bound for anything that must not observe a
	 * partial transaction, notably the committed-read watermark and the open-time
	 * discard of an interrupted batch.
	 */
	uint32_t lastCompleteTransactionEnd;
	/** Number of entries between `lastCompleteTransactionEnd` and the end of the entries. */
	uint32_t unclosedTailEntries;
	/**
	 * True when that trailing run exists and every entry in it carries the same
	 * timestamp. Together with a prior flagged boundary, this is the safety gate
	 * for discarding one interrupted batch. Distinct timestamps prove the file is
	 * not safe to repair; equal timestamps alone do not, because callers may
	 * assign repeated timestamps to separate transactions.
	 */
	bool unclosedTailIsOneTransaction;
	/**
	 * Largest entry timestamp among the frames the walk accepted (those before
	 * `validEnd`) that is finite and no more than MAX_CLOCK_FLOOR_SKEW_MS ahead
	 * of the wall clock, or 0 when there are none. Batch keys are not monotonic
	 * in log order, so this is a maximum over the walk, not the last entry's
	 * key. Feeds the open-time clock floor (TransactionLogStore::load); the
	 * bound keeps one corrupt far-future key from masking the real maximum.
	 */
	double maxTimestamp;
	/**
	 * Largest finite entry timestamp the walk saw above that bound, or 0 —
	 * reported so the store can warn about a far-future key it will not seed
	 * from.
	 */
	double maxImplausibleTimestamp;
};

/**
 * Reads `n` bytes at `offset` into `dest`. Return true only when all `n` bytes
 * were copied. False is I/O failure (short/interrupted/errored), never a framing
 * classification. The scanner does not request a range past `fileSize`.
 */
using TransactionLogReadFn = bool (*)(void* context, uint32_t offset, void* dest, uint32_t n);

/**
 * Walks v1 framing through a fallible random-access source and classifies its
 * integrity. The file header is assumed already validated by the caller; the
 * scan begins at the first entry. The only bound on an entry's length is
 * `fileSize` — a single entry can legitimately exceed the rotation threshold,
 * so that threshold must not be used as a cap (doing so would misclassify a
 * large committed entry as broken).
 *
 * Sequential headers share a 64 KiB read window. A large payload skip reads
 * exactly one 13-byte header so the payload is not pulled in. A failed `read`
 * throws DBException — it is not reported as TruncateTail or MidFileCorruption.
 *
 * @param fileSize Number of bytes in the log image (append-owned extent).
 * @param read     Positional reader; see TransactionLogReadFn.
 * @param context  Passed through to `read`.
 */
RecoveryScan scanTransactionLogForRecovery(
	uint32_t fileSize, TransactionLogReadFn read, void* context);

/**
 * In-memory adapter over scanTransactionLogForRecovery(fileSize, read, context).
 * Used by validation and native tests that already hold a buffer.
 */
RecoveryScan scanTransactionLogForRecovery(const char* data, uint32_t fileSize);

/**
 * File adapter: takes fileMutex, then scans via positional reads on `file`.
 * Callers that already hold fileMutex must use TransactionLogFile::scanRecoveryLocked()
 * instead (the mutex is not recursive). Throws DBException on I/O failure.
 */
RecoveryScan scanTransactionLogForRecovery(TransactionLogFile& file);

/**
 * Counts the well-formed v1 entry frames in an in-memory transaction log image.
 * Pure (no I/O) so it can be unit-tested standalone. The file header is assumed
 * already validated by the caller; counting begins at the first entry and stops
 * at the first zero-timestamp marker, EOF, or a broken/torn frame — yielding the
 * same entry count parseTransactionLog() reports for a clean file.
 *
 * @param data     Pointer to the full file image.
 * @param fileSize Number of bytes in `data`.
 */
uint32_t countTransactionLogEntries(const char* data, uint32_t fileSize);

} // namespace rocksdb_js

#endif
