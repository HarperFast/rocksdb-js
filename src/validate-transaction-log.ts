import { nativeValidateTransactionLog } from './load-binding.js';

/**
 * The validation result for a single transaction log file within a store, as
 * returned by {@link validateTransactionLogStore}.
 */
export interface TransactionLogFileValidation {
	/** File name relative to the store directory, e.g. `"3.txnlog"`. */
	file: string;
	/** Sequence number parsed from the file name. */
	sequence: number;
	/** On-disk file size in bytes. */
	size: number;
	/** Number of well-formed entry frames. */
	entries: number;
	/** End offset of the validly framed data (header + well-formed entries). */
	validBytes: number;
	/** `true` when `errors` is empty. */
	valid: boolean;
	/**
	 * Conditions that make the file invalid: a bad/missing header, an
	 * unsupported version, or a mid-file framing break. With `strict`, a torn
	 * tail is also reported here.
	 */
	errors: string[];
	/**
	 * Conditions that do not invalidate the file: a torn tail (recoverable —
	 * open-time recovery truncates it without losing committed entries) and
	 * per-entry anomalies such as suspicious timestamps or undefined flag bits.
	 */
	warnings: string[];
}

/**
 * The result of validating a transaction log store directory, as returned by
 * {@link validateTransactionLogStore}.
 */
export interface TransactionLogStoreValidation {
	/** The validated store directory. */
	path: string;
	/**
	 * `true` when there are no store-level errors and every log file is valid.
	 */
	valid: boolean;
	/** Per-file validation results, ordered by sequence number. */
	files: TransactionLogFileValidation[];
	/**
	 * Store-level errors: malformed file names, an unreadable file, or a
	 * corrupt `txn.state`.
	 */
	errors: string[];
	/**
	 * Store-level warnings: sequence gaps, unexpected directory entries, or an
	 * implausible `txn.state` flushed position.
	 */
	warnings: string[];
}

/**
 * Options for {@link validateTransactionLogStore}.
 */
export interface ValidateTransactionLogStoreOptions {
	/**
	 * Report conditions that indicate an incomplete snapshot — a torn tail, a
	 * sequence gap, or a `txn.state` flushed position beyond the newest log
	 * file or beyond its file's actual size — as errors instead of warnings. A
	 * torn tail on a live store is a normal crash artifact that open-time
	 * recovery truncates, but a backup snapshot captures every surviving file
	 * on committed entry boundaries and must be clean end to end —
	 * `backups.verify()` validates snapshots with `strict: true`. Defaults to
	 * `false`.
	 */
	strict?: boolean;
}

/**
 * Validates a transaction log store directory: every `<sequence>.txnlog` file
 * is checked for a valid header (token, version) and intact v1 entry framing
 * (using the same scan as open-time crash recovery), file-name and sequence
 * continuity are checked, and the `txn.state` side file (when present) is
 * checked for shape and a plausible flushed position. The result reports
 * per-file entry counts, errors, and warnings; it does not throw when the
 * store is invalid — check `result.valid`.
 *
 * Runs entirely in native code on a worker thread and does not require (or
 * open) a database, so it works on the store of a closed database and on the
 * per-backup snapshots under `<backupDir>/transaction_logs/<backupId>/` alike.
 *
 * Validating a store that is being actively appended to can spuriously report
 * a torn tail for the current log file — the tail of an in-flight append is
 * indistinguishable from a crash artifact.
 *
 * Rejects if `path` does not exist or is not a directory (a bad argument, as
 * opposed to an invalid store).
 *
 * @param path The store directory, e.g. `<dbDir>/transaction_logs/<name>`.
 * @param options Validation options.
 *
 * @example
 * ```typescript
 * import { validateTransactionLogStore } from '@harperfast/rocksdb-js';
 *
 * const result = await validateTransactionLogStore('/path/to/db/transaction_logs/mylog');
 * if (!result.valid) {
 *   for (const file of result.files) {
 *     console.error(file.file, file.errors);
 *   }
 * }
 * ```
 */
export function validateTransactionLogStore(
	path: string,
	options?: ValidateTransactionLogStoreOptions
): Promise<TransactionLogStoreValidation> {
	return new Promise((resolve, reject) =>
		nativeValidateTransactionLog(resolve, reject, path, options?.strict ?? false)
	);
}
