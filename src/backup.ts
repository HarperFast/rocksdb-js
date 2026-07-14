import {
	nativeBackupDelete,
	nativeBackupList,
	fileLockRelease,
	tryFileLock,
	nativeBackupPurge,
	nativeBackupRestore,
	nativeBackupVerify,
} from './load-binding.js';
import { validateTransactionLogStore } from './validate-transaction-log.js';
import { access, cp, mkdir, readdir, rm } from 'node:fs/promises';
import { isAbsolute, join, relative, resolve as resolvePath, sep } from 'node:path';

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
 * `fn` settles. The lock is exclusive by default — used by the writable-engine
 * operations `backups.delete` and `backups.purge`; backup creation
 * (`Store.backup`) takes the same exclusive lock on the same file natively
 * inside `Database::Backup` (see `runCreateBackup` in
 * `src/binding/database/backup.cpp`), where the backup directory is also
 * created. Throws immediately (without running `fn`) if the backup directory
 * is missing — `tryFileLock` creates missing parents, but `delete`/`purge`/
 * `restore` on a nonexistent directory is a caller error and must not conjure
 * an empty one — or if a conflicting holder has the directory lock.
 *
 * `backups.restore` holds the lock in `shared` mode for its source read:
 * shared holders coexist (concurrent restores are safe), but a writer's
 * exclusive acquisition rejects while a restore is in flight — and vice versa.
 * Without this, a `purge`/`delete` racing a restore can delete the very files
 * the restore is copying, after the restore's default `purgeAllFiles` mode has
 * already wiped the destination — failing the restore with no fallback. A
 * rejected writer, by contrast, just retries later.
 *
 * The remaining read-only operations (`list`, `verify`) use
 * `BackupEngineReadOnly` and are not locked: concurrent readers are safe, and
 * locking them would make cheap listings reject during a long backup. Running
 * them concurrently with a `delete`/`purge` on the same directory is a
 * caller-managed hazard, matching RocksDB's one-writable-engine-per-dir model.
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
export async function withBackupDirLock<T>(
	backupDir: string,
	fn: () => Promise<T>,
	options?: { shared?: boolean }
): Promise<T> {
	if (!(await exists(backupDir))) {
		throw new Error(`Backup directory does not exist: ${backupDir}`);
	}
	const token = tryFileLock(join(backupDir, '.backup.lock'), options?.shared === true);
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
 * Throws if `backupDir` resolves to the database directory itself or any path
 * beneath it. Backing up into the live database directory would write backup
 * files (the `.backup.lock`, `meta/`, `shared/`, `private/` subtrees) on top of
 * RocksDB's own files, and every subsequent backup would recursively capture
 * the prior ones. Both paths are resolved first so trailing slashes and
 * relative/absolute variants of the same location cannot slip past the check.
 *
 * Only the "backup dir at or under the database dir" direction is rejected —
 * the case the user hits by pointing a backup at `<db>` or `<db>/backups`. The
 * reverse (a database nested under the backup dir) is left to the caller: a
 * backup engine writes into named subtrees, not on top of arbitrary sibling
 * files, so it does not clobber the database in place.
 *
 * On case-insensitive filesystems `<db>` and `<db>/../DB/backup` are the same
 * directory on disk, but `relative()` compares case-sensitively and would call
 * the second one "outside". We case-fold both paths on the platforms whose
 * default filesystem is case-insensitive (macOS, Windows) so a casing
 * difference cannot slip a backup into the database directory. This is a
 * platform heuristic, not a per-volume probe (a case-sensitive macOS/Windows
 * volume exists), so it can over-reject a legitimately differently-cased
 * sibling there — the safe direction for a guard against clobbering the live
 * database.
 */
export function assertBackupDirOutsideDatabase(dbPath: string, backupDir: string): void {
	let resolvedDb = resolvePath(dbPath);
	let resolvedBackup = resolvePath(backupDir);
	if (process.platform === 'win32' || process.platform === 'darwin') {
		resolvedDb = resolvedDb.toLowerCase();
		resolvedBackup = resolvedBackup.toLowerCase();
	}
	const rel = relative(resolvedDb, resolvedBackup);
	const outside = rel === '..' || rel.startsWith(`..${sep}`) || isAbsolute(rel);
	if (!outside) {
		throw new Error(
			`Backup directory must not be inside the database directory: ${backupDir} is at or within ${dbPath}`
		);
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
	 *
	 * Holds the backup directory lock in shared mode for the duration: multiple
	 * restores can read concurrently, but a writer (`db.backup()`, `delete`,
	 * `purge`) rejects while a restore is in flight rather than deleting files
	 * out from under it — and a restore rejects while a writer holds the lock.
	 *
	 * The shared lock needs no write access to `backupDir`, so restoring from a
	 * read-only backup directory (immutable/WORM store, read-only-mounted volume)
	 * works: the lock is taken on an existing `.backup.lock` read-only, degrading
	 * to a no-op only when the media is read-only for every process (`EROFS`). A
	 * mere permission denial hard-fails instead — it doesn't prove a privileged
	 * writer isn't running.
	 */
	async restore(backupDir: string, dbDir: string, options?: RestoreOptions): Promise<void> {
		// Normalize before comparing so trailing slashes or relative/absolute
		// variants of the same directory can't slip past the guard and let a
		// destructive restore purge the backup directory itself.
		if (resolvePath(backupDir) === resolvePath(dbDir)) {
			throw new Error('Backup directory and database directory must be different');
		}

		return withBackupDirLock(
			backupDir,
			async () => {
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
			{ shared: true }
		);
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
	 *
	 * When the backup was created with `transactionLogs: true`, the backup's
	 * transaction log snapshot (`<backupDir>/transaction_logs/<backupId>/`) is
	 * also validated — every log file's header and entry framing must be intact
	 * (snapshots are copied on committed entry boundaries, so even a torn tail
	 * is a verification failure). Set `verifyTransactionLogs: false` to skip.
	 */
	async verify(
		backupDir: string,
		backupId: number,
		options?: { verifyWithChecksum?: boolean; verifyTransactionLogs?: boolean }
	): Promise<void> {
		await new Promise<void>((resolve, reject) =>
			nativeBackupVerify(resolve, reject, backupDir, backupId, options?.verifyWithChecksum ?? false)
		);

		if (options?.verifyTransactionLogs === false) {
			return;
		}

		const logsDir = join(backupDir, TRANSACTION_LOGS_DIRNAME, String(backupId));
		if (!(await exists(logsDir))) {
			// this backup captured no transaction logs
			return;
		}

		const failures: string[] = [];
		for (const entry of await readdir(logsDir, { withFileTypes: true })) {
			if (!entry.isDirectory()) {
				continue;
			}
			const result = await validateTransactionLogStore(join(logsDir, entry.name), {
				strict: true,
			});
			if (!result.valid) {
				const details = [
					...result.errors,
					...result.files.flatMap((file) => file.errors.map((error) => `${file.file}: ${error}`)),
				];
				failures.push(`${entry.name}: ${details.join('; ')}`);
			}
		}
		if (failures.length > 0) {
			throw new Error(
				`Backup ${backupId} transaction log verification failed: ${failures.join('\n')}`
			);
		}
	},
};
