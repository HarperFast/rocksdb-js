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
				await db.put(key, 'bar2');

				expect(await db.get(key)).toBe('bar2');
				expect((await db.get([key, 'bar1']))?.length).toBe(0);
				expect((await db.get([key, 'bar2']))?.length).toBe(0);

				let results = db.getValues(key).asArray;
				expect(results).toEqual([
					{ key, value: 'bar1' },
					{ key, value: 'bar2' },
				]);

				await db.remove('foo');
				expect(await db.get('foo')).toBeUndefined();

				results = await db.getRange({ key }).asArray;
				expect(results).toEqual([]);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it.only('should set a non-simple key', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, { dupSort: true });

				const key = ['foo', 'bar1'];
				await db.put(key, 'bar1');
				await db.put(key, 'bar2');

				expect(await db.get(key)).toBe('bar2');
				expect((await db.get([key, 'bar1']))?.length).toBe(0);
				expect((await db.get([key, 'bar2']))?.length).toBe(0);

				let results = db.getRange({ key }).asArray;
				expect(results).toEqual([
					{ key: key, value: 'bar1' },
					{ key: key, value: 'bar2' },
				]);

				await db.remove(key);
				expect(await db.get(key)).toBeUndefined();

				results = await db.getRange({ key }).asArray;
				expect(results).toEqual([]);
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
