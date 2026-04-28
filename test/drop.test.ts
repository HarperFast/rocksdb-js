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

	it('should drop a column family with two database instances', () =>
		dbRunner(
			{ dbOptions: [{ name: 'test' }, { name: 'test' }] },
			async ({ db: db1 }, { db: db2 }) => {
				db1.putSync('key', 'value');
				db2.putSync('key2', 'value2');
				expect(db1.getSync('key')).toBe('value');
				expect(db2.getSync('key2')).toBe('value2');
				expect(db1.columns).toEqual(['default', 'test']);
				expect(db2.columns).toEqual(['default', 'test']);
				await db1.drop();
				expect(db1.columns).toEqual(['default', 'test']);
				expect(db2.columns).toEqual(['default', 'test']);
				expect(db1.getSync('key')).toBe('value');
				expect(db2.getSync('key2')).toBe('value2');

				// close db1, db2 will keep column family alive
				db1.close();
				db1.open();
				expect(db1.getSync('key')).toBe('value');
				expect(db2.getSync('key2')).toBe('value2');
				expect(db1.columns).toEqual(['default', 'test']);
				expect(db2.columns).toEqual(['default', 'test']);

				// close db1 and reopen it, then do the same to db2, keeping column family alive
				db1.close();
				expect(db2.columns).toEqual(['default', 'test']);
				db1.open();
				expect(db1.columns).toEqual(['default', 'test']);
				expect(db2.columns).toEqual(['default', 'test']);
				db2.close();
				expect(db1.columns).toEqual(['default', 'test']);
				db2.open();
				expect(db1.columns).toEqual(['default', 'test']);
				expect(db2.columns).toEqual(['default', 'test']);

				expect(db1.getSync('key')).toBe('value');
				expect(db2.getSync('key2')).toBe('value2');

				// close both databases, column family should be deleted
				db1.close();
				db2.close();
				db1.open();
				expect(db1.columns).toEqual(['default', 'test']);
				db2.open();
				expect(db1.columns).toEqual(['default', 'test']);
				expect(db2.columns).toEqual(['default', 'test']);
				expect(db1.getSync('key')).toBeUndefined();
				expect(db2.getSync('key2')).toBeUndefined();
			}
		));
});
