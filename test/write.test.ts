import { describe, expect, it } from 'vitest';
import { dbRunner } from './lib/util.js';

describe('Write operations', () => {
	describe('put()', () => {
		it('should set and get a value using default column family', () => dbRunner(async ({ db }) => {
			await db.put('foo', 'bar1');
			const value = await db.get('foo');
			expect(value).toBe('bar1');
		}));

		it('should set and get a value using custom column family', () => dbRunner({
			dbOptions: [ { name: 'foo' } ]
		}, async ({ db }) => {
			await db.put('foo', 'bar2');
			const value = await db.get('foo');
			expect(value).toBe('bar2');
		}));

		it('should throw an error if key is not specified', () => dbRunner(async ({ db }) => {
			await expect((db.put as any)()).rejects.toThrow('Key is required');
		}));

		it('should throw an error if database is closed', () => dbRunner(async ({ db }) => {
			await db.close();
			await expect((db.put as any)()).rejects.toThrow('Database not open');
		}));
	});

	describe('putSync()', () => {
		it('should set and get a value using default column family', () => dbRunner(async ({ db }) => {
			db.putSync('foo', 'bar1');
			const value = await db.get('foo');
			expect(value).toBe('bar1');
		}));

		it('should set and get a value using custom column family', () => dbRunner({
			dbOptions: [ { name: 'foo' } ]
		}, async ({ db }) => {
			db.putSync('foo', 'bar2');
			const value = await db.get('foo');
			expect(value).toBe('bar2');
		}));

		it('should throw an error if key is not specified', () => dbRunner(async ({ db }) => {
			expect(() => (db.putSync as any)()).toThrow('Key is required');
		}));

		it('should throw an error if database is closed', () => dbRunner(async ({ db }) => {
			await db.close();
			expect(() => (db.putSync as any)()).toThrow('Database not open');
		}));
	});

	describe('remove()', () => {
		it('should not error if removing a non-existent key', () => dbRunner(async ({ db }) => {
			await db.remove('baz');
		}));

		it('should remove a key', () => dbRunner(async ({ db }) => {
			let value = await db.get('foo');
			expect(value).toBeUndefined();
			await db.put('foo', 'bar3');
			value = await db.get('foo');
			expect(value).toBe('bar3');
			await db.remove('foo');
			value = await db.get('foo');
			expect(value).toBeUndefined();
		}));

		it('should throw an error if key is not specified', () => dbRunner(async ({ db }) => {
			await expect((db.remove as any)()).rejects.toThrow('Key is required');
		}));

		it('should throw an error if database is closed', () => dbRunner(async ({ db }) => {
			await db.close();
			await expect((db.remove as any)()).rejects.toThrow('Database not open');
		}));
	});

	describe('removeRangeSync()', () => {
		it('should remove a range of keys', () => dbRunner(async ({ db }) => {
			db.putSync('a', '1');
			db.putSync('b', '2');
			db.putSync('c', '3');
			db.putSync('d', '4');

			db.removeRangeSync(null, 'z');

			expect(db.getSync('a')).toBeUndefined();
			expect(db.getSync('b')).toBeUndefined();
			expect(db.getSync('c')).toBeUndefined();
			expect(db.getSync('d')).toBeUndefined();
		}));

		it('should throw an error if start or end key is missing', () => dbRunner(async ({ db }) => {
			expect(() => (db.removeRangeSync as any)()).toThrow('Key is required');
			expect(() => (db.removeRangeSync as any)('a')).toThrow('Key is required');
		}));

		it('should throw an error if database is closed', () => dbRunner(async ({ db }) => {
			await db.close();
			expect(() => (db.removeRangeSync as any)('a', 'z')).toThrow('Database not open');
		}));
	});
});
