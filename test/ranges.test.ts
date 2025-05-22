import { describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';

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

				const returnedKeys: string[] = [];
				for (const { key, value } of db.getRange<{ key: any; value: any }>(opts)) {
					returnedKeys.push(key);
					// console.log({key, value});
					// expect(value).toBe(db.getSync(key));
				}
				// expect(testKeys).toEqual(returnedKeys);
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

				const returnedKeys: string[] = [];
				for await (const { key, value } of db.getRange<{ key: any; value: any }>(opts)) {
					returnedKeys.push(key);
					// console.log({key, value});
					// expect(value).toBe(db.getSync(key));
				}
				// expect(testKeys).toEqual(returnedKeys);
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

				const returnedKeys: string[] = [];
				for await (const { key, value } of db.getRange<{ key: any; value: any }>(opts)) {
					returnedKeys.push(key);
					console.log({key, value});
					// expect(value).toBe(db.getSync(key));
				}
				// expect(['b', 'c']).toEqual(returnedKeys);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should get all values', async () => {
			//
		});

		it('should get all values in range', async () => {
			//
		});

		it('should get iterate in reverse', async () => {
			//
		});

		it('should get keys only', async () => {
			//
		});

		it.only('should map a function over a range', async () => {
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

				const iter = db.getRange<{ key: any; value: any }>(opts);
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
	});
});
