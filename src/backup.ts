import {
	nativeBackupDelete,
	nativeBackupList,
	nativeBackupPurge,
	nativeBackupRestore,
	nativeBackupVerify,
} from './load-binding.js';
import { randomBytes } from 'node:crypto';
import { linkSync, readFileSync, renameSync, unlinkSync, writeFileSync } from 'node:fs';
import { mkdir } from 'node:fs/promises';
import { join, resolve as resolvePath } from 'node:path';

/** Name of the on-disk lock file placed at the root of a backup directory. */
const LOCK_FILENAME = '.backup.lock';

/** Removes `path`, ignoring any error — used for cleanup that must not throw. */
function bestEffortUnlink(path: string): void {
	try {
		unlinkSync(path);
	} catch {
		// Cleanup only: the file may already be gone, or removal may transiently
		// fail. Never let this mask the operation's real result.
	}
}

/**
 * Returns whether a process with `pid` currently exists. Signal `0` performs an
 * existence/permission check without delivering a signal: it throws `ESRCH` when
 * no such process exists and `EPERM` when the process exists but is owned by
 * another user (still "running" for our purposes). Note this reports the current
 * process's own pid as running, which is intentional — a lock held by this very
 * process is still a live lock.
 */
function isProcessRunning(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (err) {
		return (err as NodeJS.ErrnoException).code === 'EPERM';
	}
}

/**
 * Attempts to claim `lockPath` by hard-linking `tempPath` onto it. Returns
 * `true` on success, `false` if the lock already exists (`EEXIST`). `link` is
 * the atomic primitive: it fails if the target exists, giving true mutual
 * exclusion (unlike `rename`, which silently overwrites).
 */
function tryClaimLock(tempPath: string, lockPath: string): boolean {
	try {
		linkSync(tempPath, lockPath);
		return true;
	} catch (err) {
		if ((err as NodeJS.ErrnoException).code === 'EEXIST') {
			return false;
		}
		throw err;
	}
}

/**
 * Reads the pid recorded in the lock file, or `undefined` if the file is missing
 * or does not contain a usable pid. A positive integer is required: `process.kill`
 * treats `0` and negatives as process groups, so any other value is treated as
 * "no valid holder" (a stale lock to be broken).
 */
function readLockHolder(lockPath: string): number | undefined {
	let content: string;
	try {
		content = readFileSync(lockPath, 'utf8');
	} catch (err) {
		if ((err as NodeJS.ErrnoException).code === 'ENOENT') {
			return undefined;
		}
		throw err;
	}
	const pid = Number.parseInt(content.trim(), 10);
	return Number.isInteger(pid) && pid > 0 ? pid : undefined;
}

/**
 * Acquires an on-disk lock for a backup directory and returns the lock file
 * path (pass it to `releaseBackupDirLock`). Throws if the directory is already
 * locked by a running process.
 *
 * RocksDB only serializes work within a single `BackupEngine` instance and has
 * no lock on the backup directory itself. Two writable engines — in any
 * processes — creating/deleting/purging backups in the same directory
 * concurrently race on the per-backup staging directory and both fail,
 * potentially leaving the directory with no usable backup. An in-memory lock
 * cannot prevent this across processes, so the lock lives on disk: a
 * `.backup.lock` file containing the holder's pid.
 *
 * Acquisition takes at most two claim attempts:
 *   1. Claim the lock — write this pid to a uniquely-named temp file and
 *      hard-link it into place.
 *   2. If the lock already exists and names a *running* pid (possibly this
 *      process), the directory is in use — throw.
 *   3. Otherwise the lock is stale (missing, dead, or unparseable pid). Break it
 *      atomically and claim once more; if a racing process reclaimed first, the
 *      directory is now theirs and we throw.
 *
 * Writing the pid to a temp file first means the lock file, once it exists,
 * always has complete content — a concurrent reader can never observe a
 * half-written pid.
 */
function acquireBackupDirLock(backupDir: string): string {
	const lockPath = join(backupDir, LOCK_FILENAME);
	const tempPath = join(backupDir, `temp_${randomBytes(8).toString('hex')}`);
	const grabbedPath = `${tempPath}.stale`;

	try {
		writeFileSync(tempPath, `${process.pid}`);
	} catch (err) {
		if ((err as NodeJS.ErrnoException).code === 'ENOENT') {
			// The backup directory doesn't exist. Surface a clear error rather than a
			// raw ENOENT naming our internal temp file.
			throw new Error(`Backup directory does not exist: ${backupDir}`);
		}
		throw err;
	}

	try {
		if (tryClaimLock(tempPath, lockPath)) {
			return lockPath;
		}

		// A lock already exists. If a running process holds it, the directory is
		// in use.
		const holder = readLockHolder(lockPath);
		if (holder !== undefined && isProcessRunning(holder)) {
			throw new Error(`Backup directory is locked by running process ${holder}: ${lockPath}`);
		}

		// Stale lock (missing, dead, or unparseable pid). Break it by moving it
		// aside with rename, which fails with ENOENT if another process already
		// moved it. But rename is content-blind: between our staleness read above
		// and this rename, a racing process may have reclaimed and installed a
		// *live* lock, which we'd move aside. So re-check what we actually grabbed
		// and, if it names a running process, put it back (via link, which won't
		// clobber a still-newer claim) and abort. Never blindly discard it.
		try {
			renameSync(lockPath, grabbedPath);
		} catch (err) {
			if ((err as NodeJS.ErrnoException).code !== 'ENOENT') {
				throw err;
			}
			// Another process already broke the stale lock — fall through and retry.
		}
		const grabbedHolder = readLockHolder(grabbedPath);
		if (grabbedHolder !== undefined && isProcessRunning(grabbedHolder)) {
			try {
				linkSync(grabbedPath, lockPath);
			} catch {
				// The directory was reclaimed again in the meantime; leave that lock be.
			}
			throw new Error(
				`Backup directory is locked by running process ${grabbedHolder}: ${lockPath}`
			);
		}

		// One more attempt. If a racing process reclaimed the directory first, it
		// now holds the lock and this backup fails (the caller may retry).
		if (tryClaimLock(tempPath, lockPath)) {
			return lockPath;
		}
		throw new Error(`Backup directory is locked: ${lockPath}`);
	} finally {
		bestEffortUnlink(tempPath);
		bestEffortUnlink(grabbedPath);
	}
}

/**
 * Releases a lock acquired by `acquireBackupDirLock`, but only if the lock file
 * still names this process — so we never delete a lock another process reclaimed
 * after ours was considered stale. Best-effort: never throws, so it can't mask
 * the result of the operation it was guarding.
 */
function releaseBackupDirLock(lockPath: string): void {
	try {
		if (readFileSync(lockPath, 'utf8').trim() === `${process.pid}`) {
			unlinkSync(lockPath);
		}
	} catch {
		// A missing or unreadable lock leaves nothing safe to do here; a lock that
		// still names this process would be reclaimed as stale once we exit.
	}
}

/**
 * Runs `fn` while holding the on-disk lock for `backupDir`, releasing it when
 * `fn` settles. Used by the writable-engine operations (`Store.backup`,
 * `backups.delete`, `backups.purge`). Throws immediately (without running `fn`)
 * if the directory is already locked by a running process.
 *
 * Read-only operations (`list`, `verify`, and a restore's source read) use
 * `BackupEngineReadOnly` and are not locked: concurrent readers are safe.
 * Running a reader concurrently with a `delete`/`purge` on the same directory is
 * a caller-managed hazard, matching RocksDB's one-writable-engine-per-dir model.
 */
export async function withBackupDirLock<T>(backupDir: string, fn: () => Promise<T>): Promise<T> {
	const lockPath = acquireBackupDirLock(backupDir);
	try {
		return await fn();
	} finally {
		releaseBackupDirLock(lockPath);
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
