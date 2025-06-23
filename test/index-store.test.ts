import { describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';

describe('Index Store', () => {
	describe('getValues()', () => {
		it('should set a simple key', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, { dupSort: true });

				const key = 'foo';
				await db.put(key, 'bar1');
				expect(await db.get(key)).toBe('bar1');

				await db.putSync(key, 'bar2');
				expect(db.getSync(key)).toBe('bar1');

				let results = db.getValues(key).asArray;
				expect(results).toEqual(['bar1', 'bar2']);

				await db.remove('foo');
				expect(await db.get('foo')).toBeUndefined();

				results = await db.getRange({ key }).asArray;
				expect(results).toEqual([]);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should set a non-simple key', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, { dupSort: true });

				const key = ['foo', 'bar1'];
				await db.put(key, 'bar1');
				expect(await db.get(key)).toBe('bar1');

				db.putSync(key, 'bar2');
				expect(db.getSync(key)).toBe('bar1');

				let results = db.getValues(key).asArray;
				expect(results).toEqual(['bar1', 'bar2']);

				await db.remove(key);
				expect(await db.get(key)).toBeUndefined();

				results = await db.getRange({ key }).asArray;
				expect(results).toEqual([]);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should remove a specific value', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, { dupSort: true });
				await db.put('foo', 'bar1');
				await db.put('foo', 'bar2');
				await db.put('foo', 'bar3');

				await db.remove('foo', 'bar2');

				expect(db.getValues('foo').asArray).toEqual(['bar1', 'bar3']);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should error if the database is not open', () => {
			const db = new RocksDatabase(generateDBPath());
			expect(() => db.getValues('foo')).toThrow('Database not open');
		});
	});

	describe('put()', () => {
		it('should error if the database is not open', async () => {
			const db = new RocksDatabase(generateDBPath());
			expect(() => db.putSync('foo', 'bar')).toThrow('Database not open');
			await expect(db.put('foo', 'bar')).rejects.toThrow('Database not open');
		});
	});
});
