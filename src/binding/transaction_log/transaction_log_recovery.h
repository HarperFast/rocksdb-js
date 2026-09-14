#ifndef __TRANSACTION_LOG_RECOVERY_H__
#define __TRANSACTION_LOG_RECOVERY_H__

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <optional>

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
		// A caller-supplied budget ended the walk. It must never reach recovery's
		// truncation path because the unread bytes can contain complete entries.
		Incomplete,
	};

	Kind kind;
	/**
	 * For `TruncateTail`: the offset to truncate to (end of the last valid
	 * entry). For `MidFileCorruption`: the offset of the first broken frame.
	 * For `Clean`: the validated end of the entries. For `Incomplete`: the end
	 * of the frames read before the deadline.
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
	double maxTimestamp;
	double maxImplausibleTimestamp;
	/** The extent this classification covers, i.e. the bytes the walk was given. */
	uint32_t extent;
	/**
	 * Bytes the walk asked its reader for. Not the extent: a walk that stops at a
	 * break or a deadline reads only a prefix, and a budget failure reported in
	 * extents would overstate the scan's cost and undersize the next budget. It
	 * can exceed the extent, because overlapping header windows are re-read — that
	 * is work done, which is what a budget is spent on.
	 */
	uint64_t bytesRead;
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
 * `requirePaddedTail` switches the terminal classifications from recovery's
 * repair question to the clock floor's uniqueness question — see
 * scanTransactionLogForFloor().
 *
 * @param fileSize Number of bytes in the log image (append-owned extent).
 * @param read     Positional reader; see TransactionLogReadFn.
 * @param context  Passed through to `read`.
 */
RecoveryScan scanTransactionLogForRecovery(
	uint32_t fileSize,
	TransactionLogReadFn read,
	void* context,
	double plausibleBound = std::numeric_limits<double>::infinity(),
	std::optional<std::chrono::steady_clock::time_point> deadline = std::nullopt,
	bool requirePaddedTail = false);

/**
 * In-memory adapter over scanTransactionLogForRecovery(fileSize, read, context).
 * Used by validation and native tests that already hold a buffer.
 */
RecoveryScan scanTransactionLogForRecovery(
	const char* data,
	uint32_t fileSize,
	double plausibleBound = std::numeric_limits<double>::infinity(),
	std::optional<std::chrono::steady_clock::time_point> deadline = std::nullopt,
	bool requirePaddedTail = false);

/**
 * Scans a segment for the monotonic clock floor seed, which needs a stronger
 * statement than recovery does: recovery asks "where may I safely truncate",
 * this asks "did I observe every durable key in this segment". So the walk may
 * stop only on a proof that no complete frame can remain — the framed end
 * reaches the extent, the whole remaining suffix is zero, or the suffix is
 * shorter than an entry header. Anything else is reported as
 * `MidFileCorruption` at the stopping offset, which the caller refuses on.
 *
 * The extent is derived from the private read-only stream this opens, not from
 * a separate stat of `path`: a stat followed by an open is two different
 * objects, and a segment replaced or grown in between would let a short bound
 * report a clean end while omitting a higher-keyed suffix. It also never uses
 * or mutates a shared TransactionLogFile's handle, mapping, index or open
 * state (a live writer's `size` is append-owned; see invariant 5).
 *
 * @param retiredAppendBoundary The segment's authoritative logical extent when
 *   it has been retired, else 0. Bytes past a retired boundary are orphaned and
 *   can never be appended to again, so they hold nothing durable. A boundary
 *   above the handle's own extent is corruption and throws.
 */
RecoveryScan scanTransactionLogForFloor(
	const std::filesystem::path& path,
	uint32_t retiredAppendBoundary,
	double plausibleBound,
	std::optional<std::chrono::steady_clock::time_point> deadline = std::nullopt);

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
