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

} // namespace rocksdb_js

#endif
