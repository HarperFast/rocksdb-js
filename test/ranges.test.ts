import { describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';
import type { Key } from '../src/encoding.js';

describe('Ranges', () => {
	describe('getRange()', () => {
		const testKeys = [
			Symbol.for('test'),
			false,
			true,
			-33,
			-1.1,
			3.3,
			5,
			[5, 4],
			[5, 55],
			[5, 'words after number'],
			[6, 'abc'],
			['Test', null, 1],
			['Test', Symbol.for('test'), 2],
			['Test', 'not null', 3],
			'hello',
			['hello', 3],
			['hello', 'world'],
			['uid', 'I-7l9ySkD-wAOULIjOEnb', 'Rwsu6gqOw8cqdCZG5_YNF'],
			'z',
		];

		it('should query a range synchronously', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath);

				for (const key of testKeys) {
					await db.put(key, 'value');
				}

				const opts = {
					start: Symbol.for('A')
				};

				const returnedKeys: Key[] = [];
				for (const { key, value } of db.getRange(opts)) {
					returnedKeys.push(key);
					expect(value).toBe(db.getSync(key));
				}
				expect(testKeys).toEqual(returnedKeys);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should query a range asynchronously', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath);

				for (const key of testKeys) {
					await db.put(key, 'value');
				}

				const opts = {
					start: Symbol.for('A')
				};

				const returnedKeys: Key[] = [];
				for await (const { key, value } of db.getRange(opts)) {
					returnedKeys.push(key);
					expect(value).toBe(db.getSync(key));
				}
				expect(testKeys).toEqual(returnedKeys);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it.skip('should get a range within a transaction', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath);

				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, 'value');
				}

				const opts = {
					start: 'b',
					end: 'd'
				};

				const returnedKeys: Key[] = [];
				for await (const { key, value } of db.getRange(opts)) {
					returnedKeys.push(key);
					expect(value).toBe(db.getSync(key));
				}
				expect(['b', 'c']).toEqual(returnedKeys);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it.skip('should get all values', async () => {
			//
		});

		it.skip('should get all values in range', async () => {
			//
		});

		it.skip('should get iterate in reverse', async () => {
			//
		});

		it.skip('should get keys only', async () => {
			//
		});

		it('should return a range as an array', async () => {
			// .asArray
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath);

				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const array = iter.asArray;
				expect(Array.isArray(array)).toBe(true);

				expect(array).toMatchObject([
					{ key: 'a', value: 'value a' },
					{ key: 'b', value: 'value b' },
					{ key: 'c', value: 'value c' },
					{ key: 'd', value: 'value d' },
					{ key: 'e', value: 'value e' },
				]);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it.skip('should return an item at a specific index', async () => {
			// .at()
		});

		it.skip('should concat two ranges', async () => {
			// .concat()
		});

		it.skip('should drop items from a range', async () => {
			// .drop()
		});

		it.skip('should check if every item in a range passes a test', async () => {
			// .every()
		});

		it.skip('should filter items in a range', async () => {
			// .filter()
		});

		it.skip('should find the first item in a range', async () => {
			// .find()
		});

		it.skip('should flatten a range', async () => {
			// .flatMap()
		});

		it.skip('should call a function for each item in a range', async () => {
			// .forEach()
		});

		it('should map each item in a range', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath);

				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, 'value');
				}

				const opts = {
					start: Symbol.for('A')
				};

				const iter = db.getRange(opts);
				const mapped = iter.map(item => {
					return {
						...item,
						value: item.value + '!'
					};
				});

				expect(Array.from(mapped)).toEqual([
					{ key: 'a', value: 'value!' },
					{ key: 'b', value: 'value!' },
					{ key: 'c', value: 'value!' },
					{ key: 'd', value: 'value!' },
					{ key: 'e', value: 'value!' },
				]);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it.skip('should map an error', async () => {
			// .mapError()
		});

		it.skip('should reduce a range', async () => {
			// .reduce()
		});

		it.skip('should slice a range', async () => {
			// .slice()
		});

		it.skip('should take items from a range', async () => {
			// .take()
		});

		it.skip('should check if some items in a range pass a test', async () => {
			// .some()
		});
	});
});
