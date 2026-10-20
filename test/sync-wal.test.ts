import { stats } from '../src/index.ts';
import { dbRunner } from './lib/util.ts';
import { describe, expect, it } from 'vitest';

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
