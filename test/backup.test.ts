import {
	backups,
	fileLockRelease,
	registryStatus,
	RocksDatabase,
	tryFileLock,
} from '../src/index.js';
import { dbRunner, generateDBPath } from './lib/util.js';
import {
	chmodSync,
	existsSync,
	mkdirSync,
	readdirSync,
	rmSync,
	statSync,
	writeFileSync,
} from 'node:fs';
import { basename, dirname, join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

/** Name of the on-disk backup lock file (mirrors LOCK_FILENAME in src/backup.ts). */
const LOCK_FILENAME = '.backup.lock';

const tempDirs: string[] = [];

/**
 * Returns a unique, not-yet-created temp directory path that is cleaned up
 * after each test.
 */
function tempDir(): string {
	const dir = generateDBPath();
	tempDirs.push(dir);
	return dir;
}

async function readAll(db: RocksDatabase, count: number): Promise<void> {
	for (let i = 0; i < count; ++i) {
		expect(await db.get(`key-${i}`)).toBe(`value-${i}`);
	}
}

async function writeAll(db: RocksDatabase, count: number, prefix = 'value'): Promise<void> {
	for (let i = 0; i < count; ++i) {
		await db.put(`key-${i}`, `${prefix}-${i}`);
	}
	await db.flush();
}

/** Recursively chmod a tree (children before the containing directory). */
function chmodTree(path: string, dirMode: number, fileMode: number): void {
	if (statSync(path).isDirectory()) {
		for (const entry of readdirSync(path)) {
			chmodTree(join(path, entry), dirMode, fileMode);
		}
		chmodSync(path, dirMode);
	} else {
		chmodSync(path, fileMode);
	}
}

describe('Backups', () => {
	afterEach(() => {
		for (const dir of tempDirs) {
			rmSync(dir, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
		}
		tempDirs.length = 0;
	});

	it('should back up and restore a database round-trip', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 100);

			const backupDir = tempDir();
			const id = await db.backup(backupDir);
			expect(id).toBe(1);

			const restoreDir = tempDir();
			await backups.restore(backupDir, restoreDir);

			const restored = new RocksDatabase(restoreDir);
			restored.open();
			try {
				await readAll(restored, 100);
			} finally {
				restored.close();
			}
		}));

	it('should create the backup directory (including parents) if missing', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 5);

			const backupDir = join(tempDir(), 'nested', 'backups');
			expect(existsSync(backupDir)).toBe(false);

			const id = await db.backup(backupDir);
			expect(id).toBe(1);
			expect(existsSync(backupDir)).toBe(true);
		}));

	it('should reject backing up into the database directory or a subdirectory', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 5);

			const dbPath = db.store.path;
			const before = readdirSync(dbPath).sort();

			// The database directory itself.
			await expect(db.backup(dbPath)).rejects.toThrow(/must not be inside the database directory/);
			// A subdirectory of the database directory.
			await expect(db.backup(join(dbPath, 'backups'))).rejects.toThrow(
				/must not be inside the database directory/
			);
			// Trailing-slash / relative variants must not slip past the guard.
			await expect(db.backup(join(dbPath, 'nested', '..'))).rejects.toThrow(
				/must not be inside the database directory/
			);

			// The guard runs before any directory is created or files are written.
			expect(readdirSync(dbPath).sort()).toEqual(before);
			expect(existsSync(join(dbPath, 'backups'))).toBe(false);
		}));

	// On case-insensitive filesystems (macOS/Windows defaults) a casing
	// difference must not slip a backup into the database directory.
	it.runIf(process.platform === 'darwin' || process.platform === 'win32')(
		'should reject a case-differing path into the database directory on case-insensitive filesystems',
		() =>
			dbRunner(async ({ db }) => {
				await writeAll(db, 5);

				const dbPath = db.store.path;
				// Same on-disk directory, different casing of the db segment.
				const cased = join(dirname(dbPath), basename(dbPath).toUpperCase(), 'backups');
				await expect(db.backup(cased)).rejects.toThrow(/must not be inside the database directory/);
			})
	);

	it('should allow backing up to a sibling directory of the database', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 5);

			const backupDir = join(db.store.path, '..', 'sibling-backups');
			tempDirs.push(backupDir);
			expect(await db.backup(backupDir)).toBe(1);
		}));

	it('should create incremental backups and list them', () =>
		dbRunner(async ({ db }) => {
			const backupDir = tempDir();

			await writeAll(db, 50);
			expect(await db.backup(backupDir)).toBe(1);

			await db.put('extra', 'value');
			await db.flush();
			expect(await db.backup(backupDir)).toBe(2);

			const list = await backups.list(backupDir);
			expect(list.length).toBe(2);
			expect(list.map((b) => b.backupId)).toEqual([1, 2]);
			for (const info of list) {
				expect(info.timestamp).toBeGreaterThan(0);
				expect(info.size).toBeGreaterThan(0);
				expect(info.numberFiles).toBeGreaterThan(0);
			}
		}));

	it('should back up with the disk-space preflight enabled (default) and disabled', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 20);

			// Default: the preflight is on and a normal DB has ample room, so the
			// backup succeeds (guards against the check false-rejecting). The reject
			// and degrade branches are covered deterministically in the native test
			// (test/native/backup_disk_space_test.cc), which can fake free space.
			const enabledDir = tempDir();
			expect(await db.backup(enabledDir)).toBe(1);

			// Explicitly disabling the check must also succeed and skip the preflight.
			const disabledDir = tempDir();
			expect(await db.backup(disabledDir, { checkDiskSpace: false })).toBe(1);

			// The preflight also sizes the transaction-log snapshot (written to the
			// same volume); a normal DB has room, so this succeeds with both the
			// check and log capture enabled.
			const logsDir = tempDir();
			expect(await db.backup(logsDir, { checkDiskSpace: true, transactionLogs: true })).toBe(1);
		}));

	it('should store and return application metadata', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 5);

			const backupDir = tempDir();
			await db.backup(backupDir, { metadata: 'nightly-2026-06-04' });

			const list = await backups.list(backupDir);
			expect(list.length).toBe(1);
			expect(list[0].appMetadata).toBe('nightly-2026-06-04');
		}));

	it('should restore a specific backup id', () =>
		dbRunner(async ({ db }) => {
			const backupDir = tempDir();

			await writeAll(db, 10, 'first');
			expect(await db.backup(backupDir)).toBe(1);

			await writeAll(db, 10, 'second');
			expect(await db.backup(backupDir)).toBe(2);

			const restoreDir = tempDir();
			await backups.restore(backupDir, restoreDir, { backupId: 1 });

			const restored = new RocksDatabase(restoreDir);
			restored.open();
			try {
				expect(await restored.get('key-0')).toBe('first-0');
			} finally {
				restored.close();
			}
		}));

	it('should restore the latest backup by default', () =>
		dbRunner(async ({ db }) => {
			const backupDir = tempDir();

			await writeAll(db, 10, 'first');
			await db.backup(backupDir);

			await writeAll(db, 10, 'second');
			await db.backup(backupDir);

			const restoreDir = tempDir();
			await backups.restore(backupDir, restoreDir);

			const restored = new RocksDatabase(restoreDir);
			restored.open();
			try {
				expect(await restored.get('key-0')).toBe('second-0');
			} finally {
				restored.close();
			}
		}));

	it('should delete a specific backup', () =>
		dbRunner(async ({ db }) => {
			const backupDir = tempDir();
			await writeAll(db, 5);

			await db.backup(backupDir);
			await db.backup(backupDir);
			await db.backup(backupDir);

			await backups.delete(backupDir, 2);

			const list = await backups.list(backupDir);
			expect(list.map((b) => b.backupId)).toEqual([1, 3]);
		}));

	it('should purge old backups keeping the newest', () =>
		dbRunner(async ({ db }) => {
			const backupDir = tempDir();
			await writeAll(db, 5);

			await db.backup(backupDir);
			await db.backup(backupDir);
			await db.backup(backupDir);

			await backups.purge(backupDir, 1);

			const list = await backups.list(backupDir);
			expect(list.map((b) => b.backupId)).toEqual([3]);
		}));

	it('should verify a good backup and reject a missing one', () =>
		dbRunner(async ({ db }) => {
			const backupDir = tempDir();
			await writeAll(db, 5);
			await db.backup(backupDir);

			await expect(backups.verify(backupDir, 1)).resolves.toBeUndefined();
			await expect(
				backups.verify(backupDir, 1, { verifyWithChecksum: true })
			).resolves.toBeUndefined();

			await expect(backups.verify(backupDir, 999)).rejects.toThrow();
		}));

	it('should preserve unflushed data when WAL is disabled', () =>
		dbRunner({ dbOptions: [{ disableWAL: true }] }, async ({ db }) => {
			// No explicit flush — backup() should flush by default because the WAL
			// is disabled, otherwise this data would be lost from the backup.
			for (let i = 0; i < 25; ++i) {
				await db.put(`key-${i}`, `value-${i}`);
			}

			const backupDir = tempDir();
			await db.backup(backupDir);

			const restoreDir = tempDir();
			await backups.restore(backupDir, restoreDir);

			const restored = new RocksDatabase(restoreDir);
			restored.open();
			try {
				await readAll(restored, 25);
			} finally {
				restored.close();
			}
		}));

	it('should restore with a custom walDir', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 20);

			const backupDir = tempDir();
			await db.backup(backupDir);

			const restoreDir = tempDir();
			const walDir = tempDir();
			await backups.restore(backupDir, restoreDir, { walDir });

			const restored = new RocksDatabase(restoreDir);
			restored.open();
			try {
				await readAll(restored, 20);
			} finally {
				restored.close();
			}
		}));

	it('should support non-incremental backups (shareTableFiles: false)', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 10);

			const backupDir = tempDir();
			expect(await db.backup(backupDir, { shareTableFiles: false })).toBe(1);

			const list = await backups.list(backupDir);
			expect(list.length).toBe(1);

			const restoreDir = tempDir();
			await backups.restore(backupDir, restoreDir);

			const restored = new RocksDatabase(restoreDir);
			restored.open();
			try {
				await readAll(restored, 10);
			} finally {
				restored.close();
			}
		}));

	it('should restore using an incremental restore mode', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 15);

			const backupDir = tempDir();
			await db.backup(backupDir);

			const restoreDir = tempDir();
			await backups.restore(backupDir, restoreDir, { mode: 'keepLatestDbSessionIdFiles' });

			const restored = new RocksDatabase(restoreDir);
			restored.open();
			try {
				await readAll(restored, 15);
			} finally {
				restored.close();
			}
		}));

	it('should reject restoring into the backup directory, including path variants', () =>
		dbRunner(async ({ db }) => {
			const backupDir = tempDir();
			await writeAll(db, 5);
			await db.backup(backupDir);

			await expect(backups.restore(backupDir, backupDir)).rejects.toThrow(/must be different/);
			// Trailing slash resolves to the same directory and must still be rejected.
			await expect(backups.restore(backupDir, `${backupDir}/`)).rejects.toThrow(
				/must be different/
			);
		}));

	it('should back up a database opened in read-only mode', () =>
		dbRunner(
			{ skipOpen: true, dbOptions: [{}, { readOnly: true }] },
			async ({ db }, { db: readOnlyDB }) => {
				// create the database with a read-write handle first
				db.open();
				await writeAll(db, 25);
				db.close();

				readOnlyDB.open();
				expect(readOnlyDB.readOnly).toBe(true);

				const backupDir = tempDir();
				expect(await readOnlyDB.backup(backupDir)).toBe(1);

				const restoreDir = tempDir();
				await backups.restore(backupDir, restoreDir);

				const restored = new RocksDatabase(restoreDir);
				restored.open();
				try {
					await readAll(restored, 25);
				} finally {
					restored.close();
				}
			}
		));

	it.skipIf(process.platform === 'win32')(
		'should restore while a read-only handle is open on the database',
		() =>
			dbRunner(
				{ skipOpen: true, dbOptions: [{}, { readOnly: true }] },
				async ({ db, dbPath }, { db: readOnlyDB }) => {
					const backupDir = tempDir();

					db.open();
					await writeAll(db, 10, 'original');
					await db.backup(backupDir);
					await writeAll(db, 10, 'changed');
					db.close();

					readOnlyDB.open();
					expect(await readOnlyDB.get('key-0')).toBe('changed-0');

					// The destructive restore purges and rewrites the database directory.
					// POSIX allows unlinking files the read-only handle still has open, so
					// the restore succeeds; the read-only handle keeps serving its stale
					// snapshot until reopened. Skipped on Windows, where deleting open
					// files is not permitted.
					await backups.restore(backupDir, dbPath);

					expect(await readOnlyDB.get('key-0')).toBe('changed-0');
					readOnlyDB.close();

					const reopened = new RocksDatabase(dbPath);
					reopened.open();
					try {
						expect(await reopened.get('key-0')).toBe('original-0');
					} finally {
						reopened.close();
					}
				}
			)
	);

	it('should not crash when closing during a backup', () =>
		dbRunner({ skipOpen: true }, async ({ db }) => {
			db.open();
			await writeAll(db, 200);

			const backupDir = tempDir();
			const backupPromise = db.backup(backupDir);

			// Close while the backup is in flight. The descriptor is kept alive for
			// the duration of the copy, so this must settle cleanly (resolve or
			// reject) rather than crash.
			db.close();

			await expect(
				backupPromise.then(
					() => 'settled',
					() => 'settled'
				)
			).resolves.toBe('settled');

			// The backup's descriptor ref made close() skip the registry purge; the
			// backup must retry it on release so the entry does not leak (a leaked
			// entry keeps the RocksDB open forever and shows up in registryStatus()
			// long after every handle is closed).
			expect(registryStatus().length).toBe(0);
		}));

	it('should reject listing a non-existent backup directory', async () => {
		const backupDir = join(tempDir(), 'does-not-exist');
		await expect(backups.list(backupDir)).rejects.toThrow();
	});

	it('should reject a lock-taking op on a non-existent directory with a clear error', async () => {
		// delete/purge/restore must not conjure an empty backup directory
		// (tryFileLock creates missing parents); withBackupDirLock checks the
		// directory exists and surfaces a clear error.
		const backupDir = join(tempDir(), 'does-not-exist');
		await expect(backups.delete(backupDir, 1)).rejects.toThrow(/does not exist/);
		await expect(backups.purge(backupDir, 1)).rejects.toThrow(/does not exist/);
		await expect(backups.restore(backupDir, tempDir())).rejects.toThrow(/does not exist/);
		expect(existsSync(backupDir)).toBe(false);
	});

	it('should reject a concurrent backup to a directory locked by a running process', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 100);

			const backupDir = tempDir();
			// The on-disk lock lets exactly one writer hold the directory. The loser
			// sees the live lock file and rejects rather than racing the winner's
			// BackupEngine (which would corrupt the staging directory).
			const results = await Promise.allSettled([db.backup(backupDir), db.backup(backupDir)]);
			const fulfilled = results.filter((r) => r.status === 'fulfilled');
			const rejected = results.filter((r) => r.status === 'rejected');
			expect(fulfilled.length).toBe(1);
			expect(rejected.length).toBe(1);
			expect((rejected[0] as PromiseRejectedResult).reason.message).toMatch(/lock|claim/i);

			// The winner produced a valid backup and released the lock, so a
			// subsequent backup succeeds. The lock file itself stays behind by
			// design — only the kernel lock on it is released, never the file.
			const list = await backups.list(backupDir);
			expect(list.map((b) => b.backupId)).toEqual([1]);
			expect(existsSync(join(backupDir, LOCK_FILENAME))).toBe(true);
			expect(await db.backup(backupDir)).toBe(2);
		}));

	it('should reject a concurrent backup from a second database to the same directory', () =>
		dbRunner({ dbOptions: [{}, { path: generateDBPath() }] }, async ({ db }, { db: db2 }) => {
			await writeAll(db, 100);
			await writeAll(db2, 100);

			// Two independent databases (the cross-process case, simulated in one
			// process) can only be distinguished by the on-disk lock, not by any
			// in-memory state. Exactly one backup wins the directory.
			const backupDir = tempDir();
			const results = await Promise.allSettled([db.backup(backupDir), db2.backup(backupDir)]);
			expect(results.filter((r) => r.status === 'fulfilled').length).toBe(1);
			const rejected = results.filter((r) => r.status === 'rejected');
			expect(rejected.length).toBe(1);
			expect((rejected[0] as PromiseRejectedResult).reason.message).toMatch(/lock|claim/i);

			const list = await backups.list(backupDir);
			expect(list.map((b) => b.backupId)).toEqual([1]);
			await expect(
				backups.verify(backupDir, 1, { verifyWithChecksum: true })
			).resolves.toBeUndefined();
		}));

	it('should ignore a leftover lock file from a crashed process', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 50);

			const backupDir = tempDir();
			mkdirSync(backupDir, { recursive: true });
			// A crashed backup leaves the lock file behind, but the kernel released
			// its lock when the holder died — the file (whatever its content, here
			// stale pidfile-style diagnostics) carries no lock of its own, so the
			// next backup just acquires. No staleness heuristic is involved.
			writeFileSync(join(backupDir, LOCK_FILENAME), 'pid 99999 on some-dead-host');

			expect(await db.backup(backupDir)).toBe(1);
			expect(existsSync(join(backupDir, LOCK_FILENAME))).toBe(true);
		}));

	it('should reject writers while a restore holds the shared lock, but allow the restore', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 50);

			const backupDir = tempDir();
			await db.backup(backupDir);

			// Simulate an in-flight restore by holding the shared lock it takes.
			const sharedToken = tryFileLock(join(backupDir, LOCK_FILENAME), true);
			expect(sharedToken).toBeGreaterThan(0);
			try {
				// Writers must reject rather than delete files out from under the
				// restore (whose destination is already purged — a failed restore
				// would leave no usable database).
				await expect(backups.purge(backupDir, 1)).rejects.toThrow(/locked/);
				await expect(backups.delete(backupDir, 1)).rejects.toThrow(/locked/);
				await expect(db.backup(backupDir)).rejects.toThrow(/locked/);

				// A restore coexists with another shared holder.
				const restoreDir = tempDir();
				await backups.restore(backupDir, restoreDir);
				const restored = new RocksDatabase(restoreDir);
				restored.open();
				try {
					await readAll(restored, 50);
				} finally {
					restored.close();
				}
			} finally {
				fileLockRelease(sharedToken);
			}

			// Once the restore's shared lock is gone, writers proceed.
			await expect(backups.purge(backupDir, 1)).resolves.toBeUndefined();
		}));

	it('should reject a restore while a writer holds the lock', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 50);

			const backupDir = tempDir();
			await db.backup(backupDir);

			// Simulate an in-flight backup/delete/purge by holding the exclusive lock.
			const exclusiveToken = tryFileLock(join(backupDir, LOCK_FILENAME));
			expect(exclusiveToken).toBeGreaterThan(0);
			try {
				await expect(backups.restore(backupDir, tempDir())).rejects.toThrow(/locked/);
			} finally {
				fileLockRelease(exclusiveToken);
			}

			const restoreDir = tempDir();
			await backups.restore(backupDir, restoreDir);
			const restored = new RocksDatabase(restoreDir);
			restored.open();
			try {
				await readAll(restored, 50);
			} finally {
				restored.close();
			}
		}));

	// Restoring from an immutable/WORM or read-only-mounted backup store is a
	// legitimate disaster-recovery pattern. A restore is pure-read, so its shared
	// lock must not require write access to the backup directory (Windows chmod
	// semantics differ, so POSIX-only).
	it.skipIf(process.platform === 'win32')('should restore from a read-only backup directory', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 50);

			const backupDir = tempDir();
			await db.backup(backupDir);

			chmodTree(backupDir, 0o555, 0o444);
			try {
				const restoreDir = tempDir();
				await backups.restore(backupDir, restoreDir);
				const restored = new RocksDatabase(restoreDir);
				restored.open();
				try {
					await readAll(restored, 50);
				} finally {
					restored.close();
				}
			} finally {
				chmodTree(backupDir, 0o755, 0o644); // let afterEach clean up
			}
		})
	);

	it('should allow concurrent backups to different directories', () =>
		dbRunner(async ({ db }) => {
			await writeAll(db, 100);

			const dirA = tempDir();
			const dirB = tempDir();
			const [idA, idB] = await Promise.all([db.backup(dirA), db.backup(dirB)]);
			expect(idA).toBe(1);
			expect(idB).toBe(1);

			for (const dir of [dirA, dirB]) {
				const list = await backups.list(dir);
				expect(list.map((b) => b.backupId)).toEqual([1]);
				await expect(backups.verify(dir, 1, { verifyWithChecksum: true })).resolves.toBeUndefined();
			}

			// Both backups are independently restorable.
			const restoreDir = tempDir();
			await backups.restore(dirA, restoreDir);
			const restored = new RocksDatabase(restoreDir);
			restored.open();
			try {
				await readAll(restored, 100);
			} finally {
				restored.close();
			}
		}));
});
