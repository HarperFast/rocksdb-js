#ifndef __TRANSACTION_LOG_VALIDATION_H__
#define __TRANSACTION_LOG_VALIDATION_H__

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace rocksdb_js {

/**
 * The outcome of validating a single v1 transaction log file image.
 *
 * `errors` are conditions that make the file invalid: a bad/missing header, an
 * unsupported version, or a mid-file framing break (committed entries follow a
 * broken frame). `warnings` are conditions that do not invalidate the file: a
 * torn tail (recoverable — open-time recovery truncates it without losing
 * committed entries) and per-entry anomalies (suspicious timestamps, undefined
 * flag bits). With `strict`, a torn tail is reported as an error instead —
 * appropriate for backup snapshots, which are copied on committed entry
 * boundaries and must therefore be framing-clean end to end.
 */
struct TransactionLogFileValidation final {
	/** True when `errors` is empty. */
	bool valid = true;
	/** Number of well-formed entry frames. */
	uint32_t entryCount = 0;
	/** End offset of the validly framed data (header + well-formed entries). */
	uint32_t validBytes = 0;
	std::vector<std::string> errors;
	std::vector<std::string> warnings;
};

/**
 * Validates an in-memory v1 transaction log file image: header (token,
 * version, timestamp sanity), framing integrity (classified with the same scan
 * used by open-time crash recovery), and per-entry anomalies. Pure (no I/O) so
 * it can be unit-tested standalone.
 *
 * @param data     Pointer to the full file image.
 * @param fileSize Number of bytes in `data`.
 * @param strict   Report a torn tail as an error instead of a warning.
 */
TransactionLogFileValidation validateTransactionLogImage(
	const char* data,
	uint32_t fileSize,
	bool strict
);

/**
 * The validation result for one log file within a store directory.
 */
struct TransactionLogStoreFileValidation final {
	/** File name relative to the store directory, e.g. "3.txnlog". */
	std::string file;
	/** Sequence number parsed from the file name. */
	uint32_t sequenceNumber = 0;
	/** On-disk file size in bytes. */
	uint64_t size = 0;
	TransactionLogFileValidation result;
};

/**
 * The outcome of validating a transaction log store directory.
 *
 * Store-level `errors`/`warnings` cover directory-shape problems (unreadable
 * files, malformed file names, sequence gaps, a corrupt `txn.state`); per-file
 * results carry each log file's own validation. `valid` is true only when
 * there are no store-level errors and every file is valid.
 */
struct TransactionLogStoreValidation final {
	std::string path;
	bool valid = true;
	std::vector<TransactionLogStoreFileValidation> files;
	std::vector<std::string> errors;
	std::vector<std::string> warnings;
};

/**
 * Validates a transaction log store directory: every `<sequence>.txnlog` file
 * is read and validated with `validateTransactionLogImage()`, file-name and
 * sequence continuity are checked, and the `txn.state` side file (when
 * present) is checked for shape and a plausible flushed position.
 *
 * Intended for offline stores (a closed database, or a backup's transaction
 * log snapshot). Running it against a store that is being actively appended
 * to can spuriously report a torn tail for the current file — the tail of an
 * in-flight append is indistinguishable from a crash artifact.
 *
 * Throws `DBException` if `path` does not exist or is not a directory (a bad
 * argument, as opposed to an invalid store).
 *
 * @param path   The store directory, e.g. `<dbDir>/transaction_logs/<name>`.
 * @param strict Report recoverable torn tails as errors (backup snapshots).
 */
TransactionLogStoreValidation validateTransactionLogStore(
	const std::filesystem::path& path,
	bool strict
);

} // namespace rocksdb_js

#endif
