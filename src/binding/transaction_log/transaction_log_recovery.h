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
	 * file contains no complete transaction. <= `validEnd` for `Clean` and
	 * `TruncateTail`.
	 *
	 * A batch's entries are written together but only its final entry carries the
	 * flag, so a crash mid-batch can leave whole, well-framed entries that are
	 * only a *prefix* of a transaction. `validEnd` accepts that prefix (the frames
	 * are intact); this is the stricter bound for anything that must not observe a
	 * partial transaction, notably the committed-read watermark and the open-time
	 * discard of an interrupted batch.
	 *
	 * For `MidFileCorruption` the scan resumes where framing does and keeps
	 * advancing this past every break, so it can exceed `validEnd`: the entries
	 * after a break stay reachable through the committed-read watermark, and readers
	 * report the break itself as a CorruptFrameError with the resync offset. A
	 * flagged entry after a break closes whatever group precedes it, including a
	 * group the break tore; that group is reported through the error, not excluded.
	 */
	uint32_t lastCompleteTransactionEnd;
	/**
	 * Number of entries between `lastCompleteTransactionEnd` (or the last framing
	 * break, whichever is later) and the end of the entries. Unused for
	 * `MidFileCorruption`.
	 */
	uint32_t unclosedTailEntries;
	/**
	 * True when that trailing run exists and every entry in it carries the same
	 * timestamp. Together with a prior flagged boundary, this is the safety gate
	 * for discarding one interrupted batch. Distinct timestamps prove the file is
	 * not safe to repair; equal timestamps alone do not, because callers may
	 * assign repeated timestamps to separate transactions.
	 */
	bool unclosedTailIsOneTransaction;
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
 * Finds where valid framing resumes at or after `from`, using the same rule as
 * the recovery scan: the first offset that starts a run of well-formed frames
 * which is either RESYNC_MIN_FRAMES long or lands exactly on the written extent
 * (`fileSize`, or the start of a pre-extended file's zero padding). Returns
 * 0 when nothing resumes (0 is never a valid entry offset). Throws DBException on
 * a failed read.
 */
uint32_t findFramingResumeOffset(
	uint32_t fileSize, TransactionLogReadFn read, void* context, uint32_t from);

/** In-memory adapter over findFramingResumeOffset(fileSize, read, context, from). */
uint32_t findFramingResumeOffset(const char* data, uint32_t fileSize, uint32_t from);

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
