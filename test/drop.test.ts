import { describe, expect, it } from 'vitest';
import { dbRunner } from './lib/util.js';

describe('Drop', () => {
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