import {
	nativeBackupDelete,
	nativeBackupList,
	nativeBackupLockRelease,
	nativeBackupLockTryAcquire,
	nativeBackupPurge,
	nativeBackupRestore,
	nativeBackupVerify,
} from './load-binding.js';
import { mkdir } from 'node:fs/promises';
import { resolve as resolvePath } from 'node:path';

/**
 * Acquires the on-disk lock for a backup directory and returns an opaque token
 * to pass to `releaseBackupDirLock`. Throws if another writer currently holds
 * the lock.
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
 * The lock is taken entirely in native code (`nativeBackupLockTryAcquire`),
 * which opens, locks, and later closes the file's OS handle without ever
 * exposing a descriptor to JS — the addon statically links its own C runtime,
 * so a Node/libuv fd is not usable across that boundary. The kernel owning the
 * lock buys two properties a pidfile cannot:
 *
 * - No staleness heuristic. The lock is released when the holder's handle closes
 *   — normal release, crash, `kill -9`, container exit — so there is nothing to
 *   reclaim and no liveness check. Pid liveness in particular is meaningless
 *   across container pid namespaces (every container has a pid 1).
 * - True cross-context exclusion. The lock conflicts across processes, containers
 *   sharing a volume (same kernel), and `worker_threads`.
 *
 * The lock file is never deleted, only locked and unlocked; an unlocked, empty
 * `.backup.lock` at the directory root is the steady state.
 */
function acquireBackupDirLock(backupDir: string): number {
	const token = nativeBackupLockTryAcquire(backupDir);
	if (token === 0) {
		throw new Error(`Backup directory is locked: ${backupDir}`);
	}
	return token;
}

/**
 * Releases a lock acquired by `acquireBackupDirLock`. The native side closes the
 * file handle, which releases the kernel lock. The lock file itself is
 * intentionally left in place (see `acquireBackupDirLock`).
 */
function releaseBackupDirLock(token: number): void {
	nativeBackupLockRelease(token);
}

/**
 * Runs `fn` while holding the on-disk lock for `backupDir`, releasing it when
 * `fn` settles. Used by the writable-engine operations (`Store.backup`,
 * `backups.delete`, `backups.purge`). Throws immediately (without running `fn`)
 * if another writer holds the directory lock.
 *
 * Read-only operations (`list`, `verify`, and a restore's source read) use
 * `BackupEngineReadOnly` and are not locked: concurrent readers are safe.
 * Running a reader concurrently with a `delete`/`purge` on the same directory is
 * a caller-managed hazard, matching RocksDB's one-writable-engine-per-dir model.
 */
export async function withBackupDirLock<T>(backupDir: string, fn: () => Promise<T>): Promise<T> {
	const token = acquireBackupDirLock(backupDir);
	try {
		return await fn();
	} finally {
		releaseBackupDirLock(token);
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
	 * Flush the memtable before backing up. Defaults to `true` when the database
	 * was opened with `disableWAL` (otherwise unflushed data would be lost from
	 * the backup), and `false` otherwise.
	 */
	flushBeforeBackup?: boolean;

	/**
	 * Arbitrary application metadata stored with the backup and returned by
	 * `backups.list()`.
	 */
	metadata?: string;

	/**
	 * Share table/blob files between backups in the same directory to enable
	 * incremental backups. Defaults to `true`.
	 */
	shareTableFiles?: boolean;

	/**
	 * Distinguish shared files by checksum to avoid collisions across databases.
	 * Defaults to `true`. Only relevant when `shareTableFiles` is enabled.
	 */
	shareFilesWithChecksum?: boolean;

	/**
	 * Include write-ahead log files in the backup. Defaults to `true`.
	 */
	backupLogFiles?: boolean;

	/**
	 * `fsync` backup files for crash consistency. Defaults to `true`.
	 */
	sync?: boolean;

	/**
	 * Number of background threads used to copy files. Defaults to `1`.
	 */
	maxBackgroundOperations?: number;
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

		return new Promise((resolve, reject) =>
			nativeBackupRestore(resolve, reject, backupDir, dbDir, walDir, {
				backupId: options?.backupId,
				keepLogFiles: options?.keepLogFiles,
				mode: options?.mode,
			})
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
		return withBackupDirLock(
			backupDir,
			() =>
				new Promise((resolve, reject) => nativeBackupDelete(resolve, reject, backupDir, backupId))
		);
	},

	/**
	 * Deletes all but the newest `keepCount` backups.
	 */
	async purge(backupDir: string, keepCount: number): Promise<void> {
		return withBackupDirLock(
			backupDir,
			() =>
				new Promise((resolve, reject) => nativeBackupPurge(resolve, reject, backupDir, keepCount))
		);
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
