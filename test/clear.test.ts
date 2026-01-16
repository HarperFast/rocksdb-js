import { describe, expect, it } from 'vitest';
import { dbRunner } from './lib/util.js';

describe('Clear', () => {
	describe('clear()', () => {
		it('should error if database is not open', () =>
			dbRunner({ skipOpen: true }, async ({ db }) => {
				await expect(db.clear()).rejects.toThrow('Database not open');
			}));

		it('should clear all data in a database', () =>
			dbRunner(async ({ db }) => {
				for (let i = 0; i < 1000; ++i) {
					db.putSync(`foo-${i}`, `bar-${i}`);
				}
				expect(db.getSync('foo-0')).toBe('bar-0');
				await db.clear();
				expect(db.getSync('foo-0')).toBeUndefined();
				expect(db.getSync('foo-900')).toBeUndefined();
				expect(Array.from(db.getRange({})).length).toBe(0);
			}));

		it('should clear a database with no data', () =>
			dbRunner(async ({ db }) => {
				await db.clear();
				expect(Array.from(db.getRange({})).length).toBe(0);
			}));

		it('should cancel clear if database closed while clearing', () =>
			dbRunner(async ({ db }) => {
				for (let i = 0; i < 100_000; ++i) {
					db.putSync(`foo-${i}`, `bar-${i}`);
				}
				const promise = db.clear();
				db.close();
				// Now that we are using a fast DeleteFilesInRange operation, this expectation no longer reliable, since it normally
				// completes before the database is closed. So we just run the test to make sure nothing crashes
				// await expect(promise).rejects.toThrow('Database closed during clear operation');
				promise.catch(() => {}); // silence expected error (that might happen depending on timing)
			}), 10_000);

		it('should only remove entries in one column family', () =>
			dbRunner({ dbOptions: [{}, { name: 'second' }] }, async ({ db }, { db: db2 }) => {
				for (let i = 0; i < 10; i++) {
					db.putSync(i, i);
					db2.putSync(i, i);
				}

				await db.clear();
				for (let i = 0; i < 10; i++) {
					expect(db.getSync(i)).toBeUndefined();
					expect(db2.getSync(i)).toBe(i);
				}
			}));
	});

	describe('clearSync()', () => {
		it('should error if database is not open', () =>
			dbRunner({ skipOpen: true }, async ({ db }) => {
				expect(() => db.clearSync()).toThrow('Database not open');
			}));

		it('should clear all data in a database', () =>
			dbRunner(async ({ db }) => {
				for (let i = 0; i < 1000; ++i) {
					db.putSync(`foo-${i}`, `bar-${i}`);
				}
				expect(db.getSync('foo-0')).toBe('bar-0');
				db.clearSync();
				expect(db.getSync('foo-0')).toBeUndefined();
				expect(db.getSync('foo-900')).toBeUndefined();
				expect(Array.from(db.getRange({})).length).toBe(0);
			}));

		it('should clear a database with no data', () =>
			dbRunner(async ({ db }) => {
				db.clearSync();
				expect(Array.from(db.getRange({})).length).toBe(0);
			}));
	});
});
