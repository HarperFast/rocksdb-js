import { RocksDatabase, stats } from '../src/index.ts';
import { dbRunner, generateDBPath, runFixture } from './lib/util.ts';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const killFixture = join(__dirname, 'fixtures', 'fork-sync-wal-kill.mts');
const failureFixture = join(__dirname, 'fixtures', 'fork-sync-wal-failure.mts');

describe('syncWalSync', () => {
	it('performs a WAL sync on the calling thread, counted by the engine', () =>
		dbRunner(
			{ dbOptions: [{ enableStats: true, statsLevel: stats.StatsLevel.All }] },
			async ({ db }) => {
				db.putSync('counted', 1);
				const before = db.getStat('rocksdb.wal.synced') as number;
				expect(db.syncWalSync()).toBeUndefined();
				expect(db.getStat('rocksdb.wal.synced')).toBeGreaterThan(before);
				expect(db.getSync('counted')).toBe(1);
			}
		));

	it('keeps writes made before it across a kill right after it', async () => {
		const dbPath = generateDBPath();
		try {
			await runFixture(killFixture, dbPath, { killsItself: true });
			const db = RocksDatabase.open(dbPath);
			try {
				for (let i = 0; i < 100; i++) {
					expect(db.getSync(`key-${i}`)).toEqual({ i });
				}
			} finally {
				db.close();
			}
		} finally {
			if (!process.env.KEEP_FILES) rmSync(dbPath, { recursive: true, force: true });
		}
	}, 20_000);

	it('throws, and the async form rejects, when the sync fails', async () => {
		const dbPath = generateDBPath();
		try {
			await runFixture(failureFixture, dbPath, { env: { ROCKSDB_JS_SYNC_WAL_FAILURE: '2' } });
		} finally {
			if (!process.env.KEEP_FILES) rmSync(dbPath, { recursive: true, force: true });
		}
	}, 20_000);

	it('returns on a database opened with disableWAL', () =>
		dbRunner({ dbOptions: [{ disableWAL: true }] }, async ({ db }) => {
			db.putSync('skipped', 'wal');
			expect(db.syncWalSync()).toBeUndefined();
		}));

	it('returns without syncing on a read-only database', () =>
		dbRunner({ dbOptions: [{}, { readOnly: true }] }, async ({ db }, { db: readOnly }) => {
			db.putSync('key', 'value');
			expect(readOnly.syncWalSync()).toBeUndefined();
		}));

	it('throws on a closed database and works again after reopen', () =>
		dbRunner(async ({ db }) => {
			db.putSync('a', 1);
			db.close();
			expect(() => db.syncWalSync()).toThrow('Database not open');
			db.open();
			db.putSync('b', 2);
			expect(db.syncWalSync()).toBeUndefined();
			expect(db.getSync('b')).toBe(2);
		}));
});

describe('syncWal', () => {
	it('performs a WAL sync, counted by the engine', () =>
		dbRunner(
			{ dbOptions: [{ enableStats: true, statsLevel: stats.StatsLevel.All }] },
			async ({ db }) => {
				db.putSync('counted', 1);
				const before = db.getStat('rocksdb.wal.synced') as number;
				await db.syncWal();
				expect(db.getStat('rocksdb.wal.synced')).toBeGreaterThan(before);
			}
		));

	it('syncs the WAL of an open database and keeps the data readable', () =>
		dbRunner(async ({ db }) => {
			db.putSync('before', 'sync');
			await expect(db.syncWal()).resolves.toBeUndefined();
			expect(db.getSync('before')).toBe('sync');
			await expect(db.syncWal()).resolves.toBeUndefined();
		}));

	it('resolves on a database opened with disableWAL', () =>
		dbRunner({ dbOptions: [{ disableWAL: true }] }, async ({ db }) => {
			db.putSync('skipped', 'wal');
			await expect(db.syncWal()).resolves.toBeUndefined();
		}));

	it('resolves without syncing on a read-only database', () =>
		dbRunner({ dbOptions: [{}, { readOnly: true }] }, async ({ db }, { db: readOnly }) => {
			db.putSync('key', 'value');
			expect(readOnly.readOnly).toBe(true);
			await expect(readOnly.syncWal()).resolves.toBeUndefined();
		}));

	it('rejects on a closed database', () =>
		dbRunner({ skipOpen: true }, async ({ db }) => {
			await expect(db.syncWal()).rejects.toThrow('Database not open');
		}));

	it('rejects after a close and is admitted again after reopen', () =>
		dbRunner(async ({ db }) => {
			db.putSync('a', 1);
			db.close();
			await expect(db.syncWal()).rejects.toThrow('Database not open');
			db.open();
			db.putSync('b', 2);
			await expect(db.syncWal()).resolves.toBeUndefined();
			expect(db.getSync('a')).toBe(1);
			expect(db.getSync('b')).toBe(2);
		}));
});
