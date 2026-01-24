import { describe, expect, it } from 'vitest';
import { dbRunner } from './lib/util.js';

describe('Drop', () => {
	it('should error if database is not open', () =>
		dbRunner({
			skipOpen: true,
		}, async ({ db }) => {
			expect(() => db.dropSync()).toThrow('Database not open');
			await expect(db.drop()).rejects.toThrow('Database not open');
		}));

	it('should drop (clear) default column family', () =>
		dbRunner(({ db }) => {
			db.putSync('key', 'value');
			expect(db.getSync('key')).toBe('value');
			db.dropSync();
			db.close();
			db.open();
			expect(db.getSync('key')).toBeUndefined();
		}));

	it('should drop (clear) default column family asynchronously', () =>
		dbRunner(async ({ db }) => {
			db.putSync('key', 'value');
			expect(db.getSync('key')).toBe('value');
			await db.drop();
			db.close();
			db.open();
			expect(db.getSync('key')).toBeUndefined();
		}));

	it('should drop a column family', () =>
		dbRunner({
			dbOptions: [{ name: 'test' }],
		}, ({ db }) => {
			db.putSync('key', 'value');
			expect(db.getSync('key')).toBe('value');
			db.dropSync();
			db.close();
			db.open();
			expect(db.getSync('key')).toBeUndefined();
		}));

	it('should drop a column family asynchronously', () =>
		dbRunner({
			dbOptions: [{ name: 'test' }],
		}, async ({ db }) => {
			db.putSync('key', 'value');
			expect(db.getSync('key')).toBe('value');
			await db.drop();
			db.close();
			db.open();
			expect(db.getSync('key')).toBeUndefined();
		}));
});