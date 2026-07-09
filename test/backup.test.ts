import { backups, registryStatus, RocksDatabase } from '../src/index.js';
import { dbRunner, generateDBPath } from './lib/util.js';
import { existsSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
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
		// delete/purge do not create the directory; the on-disk lock must surface a
		// clear "does not exist" error rather than a raw ENOENT naming a temp file.
		const backupDir = join(tempDir(), 'does-not-exist');
		await expect(backups.delete(backupDir, 1)).rejects.toThrow(/does not exist/);
		await expect(backups.purge(backupDir, 1)).rejects.toThrow(/does not exist/);
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
