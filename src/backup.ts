import {
	nativeBackupDelete,
	nativeBackupList,
	fileLockRelease,
	tryFileLock,
	nativeBackupPurge,
	nativeBackupRestore,
	nativeBackupVerify,
} from './load-binding.js';
import { access, cp, mkdir, readdir, rm } from 'node:fs/promises';
import { join, resolve as resolvePath } from 'node:path';

/** Subdirectory (under a backup directory) holding per-backup transaction log snapshots. */
const TRANSACTION_LOGS_DIRNAME = 'transaction_logs';

/** Non-blocking existence check (`fs.existsSync` would block the event loop). */
async function exists(path: string): Promise<boolean> {
	try {
		await access(path);
		return true;
	} catch {
		return false;
	}
}

/**
 * Runs `fn` while holding a native file lock for `backupDir`, releasing it when
 * `fn` settles. Used by the writable-engine operations `backups.delete` and
 * `backups.purge`; backup creation (`Store.backup`) takes the same lock on the
 * same file natively inside `Database::Backup` (see `runCreateBackup` in
 * `src/binding/database/backup.cpp`), where the backup directory is also
 * created. Throws immediately (without running `fn`) if another writer holds
 * the directory lock.
 *
 * Read-only operations (`list`, `verify`, and a restore's source read) use
 * `BackupEngineReadOnly` and are not locked: concurrent readers are safe.
 * Running a reader concurrently with a `delete`/`purge` on the same directory is
 * a caller-managed hazard, matching RocksDB's one-writable-engine-per-dir model.
 *
 * RocksDB only serializes work within a single `BackupEngine` instance and has
 * no lock on the backup directory itself. Two writable engines — in any
 * processes — creating/deleting/purging backups in the same directory
 * concurrently race on the per-backup staging directory and both fail,
 * potentially leaving the directory with no usable backup. An in-memory lock
 * cannot prevent this across processes, so the lock lives on disk: a kernel
 * advisory lock (`flock` on POSIX, `LockFileEx` on Windows) held on a
 * `.backup.lock` file at the directory root.
 *
 * The lock is taken entirely in native code (`tryFileLock`), which opens,
 * locks, and later closes the file's OS handle without ever exposing a
 * descriptor to JS — the addon statically links its own C runtime, so a
 * Node/libuv fd is not usable across that boundary. The kernel owning the lock
 * buys two properties a pidfile cannot:
 *
 * - No staleness heuristic. The lock is released when the holder's handle closes
 *   — normal release, crash, `kill -9`, container exit — so there is nothing to
 *   reclaim and no liveness check.
 * - True cross-context exclusion. The lock conflicts across processes, containers
 *   sharing a volume (same kernel), and `worker_threads`.
 *
 * The lock file is never deleted, only locked and unlocked; an unlocked, empty
 * `.backup.lock` at the directory root is the steady state.
 */
export async function withBackupDirLock<T>(backupDir: string, fn: () => Promise<T>): Promise<T> {
	const token = tryFileLock(join(backupDir, '.backup.lock'));
	if (token === 0) {
		throw new Error(`Backup directory is locked: ${backupDir}`);
	}
	try {
		return await fn();
	} finally {
		fileLockRelease(token);
	}
}

/**
 * Options for creating a backup via `db.backup()`.
 *
 * Backups are whole-database: every column family, the manifest, and (unless
 * `backupLogFiles` is disabled) the write-ahead log are captured. A backup is
 * not scoped to an individual `Store`.
 */
export interface BackupOptions {
	/**
	 * Include write-ahead log files in the backup. Defaults to `true`.
	 */
	backupLogFiles?: boolean;

	/**
	 * Flush the memtable before backing up. Defaults to `true` when the database
	 * was opened with `disableWAL` (otherwise unflushed data would be lost from
	 * the backup), and `false` otherwise.
	 */
	flushBeforeBackup?: boolean;

	/**
	 * Number of background threads used to copy files. Defaults to `1`.
	 */
	maxBackgroundOperations?: number;

	/**
	 * Arbitrary application metadata stored with the backup and returned by
	 * `backups.list()`.
	 */
	metadata?: string;

	/**
	 * Distinguish shared files by checksum to avoid collisions across databases.
	 * Defaults to `true`. Only relevant when `shareTableFiles` is enabled.
	 */
	shareFilesWithChecksum?: boolean;

	/**
	 * Share table/blob files between backups in the same directory to enable
	 * incremental backups. Defaults to `true`.
	 */
	shareTableFiles?: boolean;

	/**
	 * `fsync` backup files — including the transaction log snapshot when
	 * `transactionLogs` is enabled — for crash consistency. Defaults to `true`.
	 */
	sync?: boolean;

	/**
	 * Snapshot the transaction log store into
	 * `<backupDir>/transaction_logs/<backupId>/`. Defaults to `false`. This is an
	 * all-or-nothing snapshot per backup (not incremental); `backups.delete()` and
	 * `backups.purge()` remove the corresponding log snapshots, and
	 * `backups.restore()` restores them into the database directory.
	 *
	 * The snapshot is staged and atomically renamed into place after every file
	 * is copied (and fsynced, per `sync`), so a crash mid-backup can never leave
	 * a partial snapshot: a backup id either has its complete log snapshot or
	 * none at all.
	 *
	 * The log snapshot is captured just after the RocksDB engine snapshot, so it
	 * may include entries committed between the two — the restored logs can run
	 * slightly ahead of the restored key-value data, never behind it. That bias
	 * is safe for redo-style logs that are replayed against the restored data.
	 */
	transactionLogs?: boolean;
}

/**
 * The level of incremental restore to perform.
 *
 * - `purgeAllFiles` (default): purge the destination directory and restore all
 *   files from the backup. Destructive but always correct.
 * - `keepLatestDbSessionIdFiles`: efficiently restore a healthy database,
 *   reusing existing files that match the backup.
 * - `verifyChecksum`: reuse existing destination files whose checksums match the
 *   backup, replacing only mismatched/corrupt files.
 */
export type RestoreMode = 'purgeAllFiles' | 'keepLatestDbSessionIdFiles' | 'verifyChecksum';

/**
 * Options for restoring a backup via `backups.restore()`.
 */
export interface RestoreOptions {
	/**
	 * The backup id to restore. Defaults to the latest non-corrupt backup.
	 */
	backupId?: number;

	/**
	 * Directory to restore write-ahead log files into. Defaults to the database
	 * directory.
	 */
	walDir?: string;

	/**
	 * Keep existing log files in `walDir` rather than overwriting them. Defaults
	 * to `false`.
	 */
	keepLogFiles?: boolean;

	/**
	 * The restore strategy. Defaults to `purgeAllFiles`, which is destructive.
	 */
	mode?: RestoreMode;
}

/**
 * Information about a single backup, as returned by `backups.list()`.
 */
export interface BackupInfo {
	/** The backup id (monotonically increasing integer). */
	backupId: number;
	/** Creation time in seconds since the epoch. */
	timestamp: number;
	/** Total size in bytes of the backed-up file payloads. */
	size: number;
	/** Number of files in the backup (some may be shared with other backups). */
	numberFiles: number;
	/** Application metadata supplied when the backup was created. */
	appMetadata: string;
}

/**
 * Removes `<backupDir>/transaction_logs/<id>` subtrees whose backup id no longer
 * corresponds to a surviving backup. Called after a `purge` (which holds the
 * backup-dir lock); a no-op when no transaction logs were backed up. Self-healing:
 * it drops every log subtree without a live backup. Note the surviving set comes
 * from `nativeBackupList`, which omits corrupt backups — a still-present but
 * corrupt backup's logs may be pruned (acceptable: it cannot be restored anyway).
 * This also sweeps `.staging-*` leftovers from a crashed backup process (a
 * staging name never matches a live backup id); backup creation sweeps them
 * natively as well, under the backup-dir lock.
 */
async function pruneOrphanedTransactionLogs(backupDir: string): Promise<void> {
	const logsRoot = join(backupDir, TRANSACTION_LOGS_DIRNAME);
	let ids: string[];
	try {
		ids = await readdir(logsRoot);
	} catch (err) {
		if ((err as NodeJS.ErrnoException).code === 'ENOENT') {
			return; // no transaction logs were backed up
		}
		throw err;
	}

	const list = await new Promise<BackupInfo[]>((resolve, reject) =>
		nativeBackupList(resolve, reject, backupDir)
	);
	const surviving = new Set(list.map((info) => String(info.backupId)));

	await Promise.all(
		ids
			.filter((id) => !surviving.has(id))
			.map((id) => rm(join(logsRoot, id), { recursive: true, force: true }))
	);
}

/**
 * Restores the transaction log snapshot for the restored backup into
 * `<dbDir>/transaction_logs/`, wiping the destination first so that restoring an
 * older backup over a newer one cannot leave stale (newer) log files behind.
 *
 * Only acts when THIS backup actually captured logs: if the restored id has no
 * snapshot, the destination is left untouched. Wiping unconditionally would
 * destroy on-disk logs that a mixed-mode (`transactionLogs:false`) backup never
 * managed.
 *
 * Offline only: the files are picked up when the database is next opened. mtimes
 * are preserved because the store derives file age (rotation/retention) from
 * mtime — a fresh mtime would break retention.
 */
async function restoreTransactionLogs(
	backupDir: string,
	dbDir: string,
	backupId?: number
): Promise<void> {
	const logsRoot = join(backupDir, TRANSACTION_LOGS_DIRNAME);
	if (!(await exists(logsRoot))) {
		return; // this backup directory has no transaction log snapshots
	}

	// Resolve the restored backup id (the latest, when unspecified).
	let id = backupId;
	if (id === undefined) {
		const list = await new Promise<BackupInfo[]>((resolve, reject) =>
			nativeBackupList(resolve, reject, backupDir)
		);
		if (list.length === 0) {
			return;
		}
		id = Math.max(...list.map((info) => info.backupId));
	}

	const logsSrc = join(logsRoot, String(id));
	if (!(await exists(logsSrc))) {
		// The restored backup captured no logs — do not touch the destination's
		// existing transaction logs.
		return;
	}

	// This backup has logs: wipe the destination, then restore its snapshot.
	const logsDest = join(dbDir, TRANSACTION_LOGS_DIRNAME);
	await rm(logsDest, { recursive: true, force: true });
	await cp(logsSrc, logsDest, { recursive: true, preserveTimestamps: true });
}

/**
 * Backup management operations that act on a backup directory and do not
 * require an open database. To create a backup, use the `db.backup()` instance
 * method instead (it needs a live database for a consistent snapshot).
 *
 * @example
 * ```typescript
 * import { backups } from '@harperfast/rocksdb-js';
 *
 * const id = await db.backup('/path/to/backups');
 * db.close();
 * await backups.restore('/path/to/backups', '/path/to/restored-db');
 * ```
 */
export const backups = {
	/**
	 * Restores a database from a backup directory into a (closed) database
	 * directory. The default mode purges the destination directory, so it must
	 * not point at a live database.
	 */
	async restore(backupDir: string, dbDir: string, options?: RestoreOptions): Promise<void> {
		// Normalize before comparing so trailing slashes or relative/absolute
		// variants of the same directory can't slip past the guard and let a
		// destructive restore purge the backup directory itself.
		if (resolvePath(backupDir) === resolvePath(dbDir)) {
			throw new Error('Backup directory and database directory must be different');
		}

		const walDir = options?.walDir ?? dbDir;
		await mkdir(dbDir, { recursive: true });
		if (walDir !== dbDir) {
			await mkdir(walDir, { recursive: true });
		}

		await new Promise<void>((resolve, reject) =>
			nativeBackupRestore(resolve, reject, backupDir, dbDir, walDir, {
				backupId: options?.backupId,
				keepLogFiles: options?.keepLogFiles,
				mode: options?.mode,
			})
		);

		// Restore the transaction log snapshot (if this backup included one). Wipes
		// the destination first so an older restore leaves no newer log stragglers.
		await restoreTransactionLogs(backupDir, dbDir, options?.backupId);
	},

	/**
	 * Lists the non-corrupt backups in a backup directory, ordered by id.
	 */
	list(backupDir: string): Promise<BackupInfo[]> {
		return new Promise((resolve, reject) => nativeBackupList(resolve, reject, backupDir));
	},

	/**
	 * Deletes a specific backup. Shared files are reference-counted and only
	 * removed once no remaining backup references them.
	 */
	async delete(backupDir: string, backupId: number): Promise<void> {
		return withBackupDirLock(backupDir, async () => {
			await new Promise<void>((resolve, reject) =>
				nativeBackupDelete(resolve, reject, backupDir, backupId)
			);
			// Remove only this backup's own log snapshot: precise (no dependence on
			// the backup listing, which omits corrupt backups) and best-effort (the
			// backup is already gone, so a cleanup failure must not fail the delete).
			try {
				await rm(join(backupDir, TRANSACTION_LOGS_DIRNAME, String(backupId)), {
					recursive: true,
					force: true,
				});
			} catch {
				// Best-effort: at worst an orphaned subtree remains for a later purge.
			}
		});
	},

	/**
	 * Deletes all but the newest `keepCount` backups.
	 */
	async purge(backupDir: string, keepCount: number): Promise<void> {
		return withBackupDirLock(backupDir, async () => {
			await new Promise<void>((resolve, reject) =>
				nativeBackupPurge(resolve, reject, backupDir, keepCount)
			);
			// Best-effort: the backups are already purged, so a log-cleanup failure
			// must not fail the purge.
			try {
				await pruneOrphanedTransactionLogs(backupDir);
			} catch {
				// Best-effort orphan cleanup.
			}
		});
	},

	/**
	 * Verifies a backup's file sizes, and optionally their checksums (which
	 * requires reading all backed-up data).
	 */
	verify(
		backupDir: string,
		backupId: number,
		options?: { verifyWithChecksum?: boolean }
	): Promise<void> {
		return new Promise((resolve, reject) =>
			nativeBackupVerify(resolve, reject, backupDir, backupId, options?.verifyWithChecksum ?? false)
		);
	},
};
