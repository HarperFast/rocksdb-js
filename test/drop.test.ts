import { RocksDatabase } from '../src/index.js';
import { dbRunner } from './lib/util.js';
import { describe, expect, it } from 'vitest';

describe('Drop', () => {
	it('should error if database is not open', () =>
		dbRunner({ skipOpen: true }, async ({ db }) => {
			expect(() => db.dropSync()).toThrow('Database not open');
			await expect(db.drop()).rejects.toThrow('Database not open');
		}));

	it('should drop (clear) default column family', () =>
		dbRunner(({ db }) => {
			db.putSync('key', 'value');
			expect(db.getSync('key')).toBe('value');
			db.dropSync();
			expect(db.columns).toEqual(['default']);
			db.close();
			db.open();
			expect(db.getSync('key')).toBeUndefined();
		}));

	it('should drop (clear) default column family asynchronously', () =>
		dbRunner(async ({ db }) => {
			db.putSync('key', 'value');
			expect(db.getSync('key')).toBe('value');
			await db.drop();
			expect(db.columns).toEqual(['default']);
			db.close();
			db.open();
			expect(db.getSync('key')).toBeUndefined();
		}));

	it('should drop a column family', () =>
		dbRunner({ dbOptions: [{ name: 'test' }] }, ({ db }) => {
			db.putSync('key', 'value');
			expect(db.getSync('key')).toBe('value');
			expect(db.columns).toEqual(['default', 'test']);
			db.dropSync();
			expect(db.columns).toEqual(['default']);
			db.close();
			db.open();
			expect(db.getSync('key')).toBeUndefined();
		}));

	it('should drop a column family asynchronously', () =>
		dbRunner({ dbOptions: [{ name: 'test' }] }, async ({ db }) => {
			db.putSync('key', 'value');
			expect(db.getSync('key')).toBe('value');
			expect(db.columns).toEqual(['default', 'test']);
			await db.drop();
			expect(db.columns).toEqual(['default']);
			db.close();
			db.open();
			expect(db.getSync('key')).toBeUndefined();
		}));

	// Regression test for the "ghost table" bug: a drop must free the column
	// family name immediately, even while other instances still hold the
	// dropped column family open. Before the fix, the dropped entry stayed in
	// the registry's by-name map whenever any other handle referenced it, so a
	// reopen-by-name reused the dangling handle and every write failed with
	// "Invalid column family specified in write batch" until a full process
	// restart.
	it('should free the column family name immediately on drop, even with another instance holding it', () =>
		dbRunner(
			{ dbOptions: [{ name: 'test' }, { name: 'test' }] },
			async ({ db: db1, dbPath }, { db: db2 }) => {
				db1.putSync('key', 'value');
				db2.putSync('key2', 'value2');
				expect(db1.columns).toEqual(['default', 'test']);
				expect(db2.columns).toEqual(['default', 'test']);

				await db1.drop();

				// the name is freed immediately: the registry no longer lists it
				expect(db1.columns).toEqual(['default']);
				expect(db2.columns).toEqual(['default']);

				// an instance still holding the dropped column family can read it
				// until it closes
				expect(db2.getSync('key')).toBe('value');
				expect(db2.getSync('key2')).toBe('value2');

				// reopening by the same name creates a fresh, WRITABLE column
				// family instead of reusing the dropped handle
				const db3 = RocksDatabase.open(dbPath, { name: 'test' });
				try {
					db3.putSync('key3', 'value3');
					expect(db3.getSync('key3')).toBe('value3');
					// the fresh column family does not see the dropped data
					expect(db3.getSync('key')).toBeUndefined();
					expect(db3.columns).toEqual(['default', 'test']);
				} finally {
					db3.close();
				}

				// writes through the dropped handle fail. NOTE: this must stay the
				// LAST assertion - the failed write contaminates the env's shared
				// write path, so writes through OTHER handles in this database fail
				// afterward too (the same poison the eviction fix prevents callers
				// from triggering accidentally via reopen-by-name)
				expect(() => db2.putSync('key4', 'value4')).toThrow();
			}
		));

	// Dropping an already-dropped column family must be idempotent. Harper
	// broadcasts drops to all worker threads, so multiple handles to the same
	// shared column family can each issue a drop; RocksDB rejects the second
	// with "Column family already dropped!". The family is gone either way, so
	// the redundant drop is treated as success instead of a failed operation.
	it('should treat dropping an already-dropped column family as a no-op (sync)', () =>
		dbRunner({ dbOptions: [{ name: 'test' }, { name: 'test' }] }, ({ db: db1 }, { db: db2 }) => {
			db1.dropSync();
			expect(db1.columns).toEqual(['default']);
			// db2 still holds the dropped column family; re-dropping must not throw
			expect(() => db2.dropSync()).not.toThrow();
			expect(db2.columns).toEqual(['default']);
		}));

	it('should treat dropping an already-dropped column family as a no-op (async)', () =>
		dbRunner(
			{ dbOptions: [{ name: 'test' }, { name: 'test' }] },
			async ({ db: db1 }, { db: db2 }) => {
				await db1.drop();
				expect(db1.columns).toEqual(['default']);
				// db2 still holds the dropped column family; re-dropping must resolve
				await expect(db2.drop()).resolves.toBeUndefined();
				expect(db2.columns).toEqual(['default']);
			}
		));

	// A stale handle's tolerated (already-dropped) re-drop must NOT unregister a
	// freshly-created column family that reuses the same name. db1 really drops
	// 'test' (and unregisters it); db3 recreates a fresh 'test'; db2 is a stale
	// handle to the original dropped family - its no-op drop must leave db3's
	// fresh family registered and usable, not erase it by name.
	it('should not unregister a freshly-recreated same-name column family on a stale drop', () =>
		dbRunner(
			{ dbOptions: [{ name: 'test' }, { name: 'test' }] },
			({ db: db1, dbPath }, { db: db2 }) => {
				db1.dropSync();
				const db3 = RocksDatabase.open(dbPath, { name: 'test' });
				try {
					db3.putSync('k', 'v');
					expect(db3.columns).toContain('test');
					// stale re-drop of the original family - must be a no-op for the registry
					db2.dropSync();
					// db3's fresh family is untouched: still registered and writable
					expect(db3.columns).toContain('test');
					expect(db3.getSync('k')).toBe('v');
					db3.putSync('k2', 'v2');
					expect(db3.getSync('k2')).toBe('v2');
				} finally {
					db3.close();
				}
			}
		));
});
