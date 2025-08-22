import { describe, expect, it } from 'vitest';
import { dbRunner } from './lib/util.js';

describe('Clear', () => {
	describe('clear()', () => {
		it('should error if database is not open', () => dbRunner({
			skipOpen: true
		}, async ({ db }) => {
			await expect(db.clear()).rejects.toThrow('Database not open');
		}));

		it('should clear all data in a database with default batch size', () => dbRunner(async ({ db }) => {
			for (let i = 0; i < 1000; ++i) {
				db.putSync(`foo-${i}`, `bar-${i}`);
			}
			expect(db.getSync('foo-0')).toBe('bar-0');
			await expect(db.clear()).resolves.toBe(1000);
			expect(db.getSync('foo-0')).toBeUndefined();
		}));

		it('should clear a database with no data', () => dbRunner(async ({ db }) => {
			await expect(db.clear()).resolves.toBe(0);
		}));

		it('should clear all data in a database using single small batch', () => dbRunner(async ({ db }) => {
			for (let i = 0; i < 10; ++i) {
				db.putSync(`foo-${i}`, `bar-${i}`);
			}
			expect(db.getSync('foo-0')).toBe('bar-0');
			await expect(db.clear({ batchSize: 100 })).resolves.toBe(10);
			expect(db.getSync('foo-0')).toBeUndefined();
		}));

		it('should clear all data in a database using multiple small batches', () => dbRunner(async ({ db }) => {
			for (let i = 0; i < 987; ++i) {
				db.putSync(`foo-${i}`, `bar-${i}`);
			}
			expect(db.getSync('foo-0')).toBe('bar-0');
			await expect(db.clear({ batchSize: 100 })).resolves.toBe(987);
			expect(db.getSync('foo-0')).toBeUndefined();
		}));

		it('should cancel clear if database closed while clearing', () => dbRunner(async ({ db }) => {
			// note: this test can be flaky because sometimes it can clear
			// faster than the close, so set the batch size to 1 to slow it
			// down a little
			for (let i = 0; i < 100000; ++i) {
				db.putSync(`foo-${i}`, `bar-${i}`);
			}
			const promise = db.clear({ batchSize: 1 });
			db.close();
			await expect(promise).rejects.toThrow('Database closed during clear operation');
		}));
	});

	describe('clearSync()', () => {
		it('should error if database is not open', () => dbRunner({
			skipOpen: true
		}, async ({ db }) => {
			expect(() => db.clearSync()).toThrow('Database not open');
		}));

		it('should clear all data in a database with default batch size', () => dbRunner(async ({ db }) => {
			for (let i = 0; i < 1000; ++i) {
				db.putSync(`foo-${i}`, `bar-${i}`);
			}
			expect(db.getSync('foo-0')).toBe('bar-0');
			expect(db.clearSync()).toBe(1000);
			expect(db.getSync('foo-0')).toBeUndefined();
		}));

		it('should clear a database with no data', () => dbRunner(async ({ db }) => {
			expect(db.clearSync()).toBe(0);
		}));

		it('should clear all data in a database using single small batch', () => dbRunner(async ({ db }) => {
			for (let i = 0; i < 10; ++i) {
				db.putSync(`foo-${i}`, `bar-${i}`);
			}
			expect(db.getSync('foo-0')).toBe('bar-0');
			expect(db.clearSync({ batchSize: 100 })).toBe(10);
			expect(db.getSync('foo-0')).toBeUndefined();
		}));

		it('should clear all data in a database using multiple small batches', () => dbRunner(async ({ db }) => {
			for (let i = 0; i < 987; ++i) {
				db.putSync(`foo-${i}`, `bar-${i}`);
			}
			expect(db.getSync('foo-0')).toBe('bar-0');
			expect(db.clearSync({ batchSize: 100 })).toBe(987);
			expect(db.getSync('foo-0')).toBeUndefined();
		}));
	});
});
