#ifndef __TRANSACTION_LOG_RECOVERY_H__
#define __TRANSACTION_LOG_RECOVERY_H__

#include <cstdint>

namespace rocksdb_js {

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
};

/**
 * Walks the v1 framing of an in-memory transaction log image and classifies its
 * integrity. Pure (no I/O) so it can be unit-tested standalone. The file header
 * is assumed already validated by the caller; the scan begins at the first
 * entry. The only bound on an entry's length is `fileSize` — a single entry can
 * legitimately exceed the rotation threshold, so that threshold must not be used
 * as a cap (doing so would misclassify a large committed entry as broken).
 *
 * @param data     Pointer to the full file image.
 * @param fileSize Number of bytes in `data`.
 */
RecoveryScan scanTransactionLogForRecovery(const char* data, uint32_t fileSize);

/**
 * Counts the well-formed v1 entry frames in an in-memory transaction log image.
 * Pure (no I/O) so it can be unit-tested standalone, and shares the framing walk
 * with scanTransactionLogForRecovery(). The file header is assumed already
 * validated by the caller; counting begins at the first entry and stops at the
 * first zero-timestamp marker, EOF, or a broken/torn frame — yielding the same
 * entry count parseTransactionLog() reports for a clean file.
 *
 * @param data     Pointer to the full file image.
 * @param fileSize Number of bytes in `data`.
 */
uint32_t countTransactionLogEntries(const char* data, uint32_t fileSize);

/**
 * Returns the offset just past the last entry carrying
 * `TRANSACTION_LOG_ENTRY_LAST_FLAG` in an in-memory transaction log image, or 0
 * if it contains no complete transaction. Pure (no I/O) so it can be unit-tested
 * standalone, and shares the framing walk with scanTransactionLogForRecovery().
 * Used for log files that did not go through open-time recovery (which already
 * reports the same value in `RecoveryScan::lastCompleteTransactionEnd`).
 *
 * @param data     Pointer to the full file image.
 * @param fileSize Number of bytes in `data`.
 */
uint32_t findLastCompleteTransactionEnd(const char* data, uint32_t fileSize);

} // namespace rocksdb_js

#endif
