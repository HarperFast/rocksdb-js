import { assert, describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';
import { Key } from '../src/encoding.js';
import type { Transaction } from '../src/transaction.js';

describe('Index Store', () => {
	describe('get()', () => {
		it('should get a value', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, { dupSort: true });

				await db.put('foo', 'bar1');
				expect(await db.get('foo')).toBe('bar1');

				await db.put('foo', 'bar2');
				expect(await db.get('foo')).toBe('bar1');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should get a simple key in a transaction', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, { dupSort: true });
				await db.put('foo', 'bar');
				await db.transaction(async (txn: Transaction) => {
					expect(await txn.get('foo')).toBe('bar');

					await txn.put('foo', 'bar1');
					expect(await txn.get('foo')).toBe('bar');

					await txn.put('foo', 'bar2');
					expect(await txn.get('foo')).toBe('bar');
				});
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should error if the database is not open', async () => {
			const db = new RocksDatabase(generateDBPath(), { dupSort: true });
			await expect(db.get('foo')).rejects.toThrow('Database not open');
		});
	});

	describe('getRange()', () => {
		it('should get a range of keys', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, { dupSort: true });

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

		it('should get a range of keys in a transaction', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, { dupSort: true });

				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, 'value');
				}

				await db.transaction(async (txn: Transaction) => {
					const opts = {
						start: 'b',
						end: 'd'
					};

					const returnedKeys: Key[] = [];
					for await (const { key, value } of txn.getRange(opts)) {
						returnedKeys.push(key);
						expect(value).toBe(txn.getSync(key));
					}
					expect(['b', 'c']).toEqual(returnedKeys);
				});
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it.only('should query a bunch of keys', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, { dupSort: true });

				const keys = [
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
					// ['Test', null, 1],
					['Test', Symbol.for('test'), 2],
					['Test', 'not null', 3],
					'hello',
					['hello', 3],
					['hello', 'world'],
					['uid', 'I-7l9ySkD-wAOULIjOEnb', 'Rwsu6gqOw8cqdCZG5_YNF'],
					'z',
				];
				for (const key of keys) {
					// console.log(key);
					await db.put(key, key);
				}
				console.log('--------------------------------');

				let returnedKeys: Key[] = [];
				for (const { key, value } of db.getRange({
					// start: Symbol.for('A'),
				})) {
					console.log();
					const db_val = db.get(key);
					console.log({ key, value, db_val });
					returnedKeys.push(key);
					expect(db_val).toBe(value);
					console.log();
				}
				expect(returnedKeys).toEqual(keys);
				console.log('--------------------------------');

				returnedKeys = [];
				for (const { key, value } of db.getRange({
					reverse: true,
				})) {
					// console.log({ key, value, db_val: db.get(key)});
					returnedKeys.unshift(key);
					expect(db.get(key)).toBe(value);
				}
				keys.shift(); // remove the symbol test, it should be omitted
				expect(returnedKeys).toEqual(keys);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should error if the database is not open', async () => {
			const db = new RocksDatabase(generateDBPath(), { dupSort: true });
			expect(() => db.getRange()).toThrow('Database not open');
		});
	});

	describe('getSync()', () => {
		it('should get a simple key', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, { dupSort: true });
				db.putSync('foo', 'bar1');
				expect(db.getSync('foo')).toBe('bar1');

				db.putSync('foo', 'bar2');
				expect(db.getSync('foo')).toBe('bar1');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should error if the database is not open', () => {
			const db = new RocksDatabase(generateDBPath(), { dupSort: true });
			expect(() => db.getSync('foo')).toThrow('Database not open');
		});
	});

	describe('getValues()', () => {
		it('should get all values for a simple key', async () => {
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

				results = await db.getValues(key).asArray;
				expect(results).toEqual([]);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should get all values for a non-simple key', async () => {
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

		it('should get all values for a simple key in a transaction', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, { dupSort: true });

				await db.transaction(async (txn: Transaction) => {
					const key = 'foo';
					await txn.put(key, 'bar1');
					expect(await txn.get(key)).toBe('bar1');

					await txn.putSync(key, 'bar2');
					expect(txn.getSync(key)).toBe('bar1');

					let results = txn.getValues(key).asArray;
					expect(results).toEqual(['bar1', 'bar2']);

					await txn.remove('foo');
					expect(await txn.get('foo')).toBeUndefined();

					results = await txn.getValues(key).asArray;
					expect(results).toEqual([]);
				});
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should get all values for a non-simple key in a transaction', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, { dupSort: true });

				await db.transaction(async (txn: Transaction) => {
					const key = ['foo', 'bar1'];
					await txn.put(key, 'bar1');
					expect(await txn.get(key)).toBe('bar1');

					txn.putSync(key, 'bar2');
					expect(txn.getSync(key)).toBe('bar1');

					let results = txn.getValues(key).asArray;
					expect(results).toEqual(['bar1', 'bar2']);

					await txn.remove(key);
					expect(await txn.get(key)).toBeUndefined();

					results = await txn.getRange({ key }).asArray;
					expect(results).toEqual([]);
				});
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

	describe('getValuesCount()', () => {
		it('should get the number of values for a key', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, { dupSort: true });
				await db.put('foo', 'bar1');
				expect(db.getValuesCount('foo')).toBe(1);
				await db.put('foo', 'bar2');
				expect(db.getValuesCount('foo')).toBe(2);
				await db.put('foo', 'bar3');
				expect(db.getValuesCount('foo')).toBe(3);
				await db.remove('foo', 'bar2');
				expect(db.getValuesCount('foo')).toBe(2);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should get the number of values for a key in a transaction', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, { dupSort: true });
				await db.transaction(async (txn: Transaction) => {
					await txn.put('foo', 'bar1');
					expect(txn.getValuesCount('foo')).toBe(1);
					await txn.put('foo', 'bar2');
					expect(txn.getValuesCount('foo')).toBe(2);
				});
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should error if the database is not open', () => {
			const db = new RocksDatabase(generateDBPath());
			expect(() => db.getValuesCount('foo')).toThrow('Database not open');
		});
	});

	describe('put()', () => {
		it('should put a simple key', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, { dupSort: true });
				await db.put('foo', 'bar1');
				expect(await db.get('foo')).toBe('bar1');

				await db.put('foo', 'bar2');
				expect(await db.get('foo')).toBe('bar1');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should throw an error if the key is not specified', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, { dupSort: true });
				await expect((db.put as any)()).rejects.toThrow('Key is required');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should error if the database is not open', async () => {
			const db = new RocksDatabase(generateDBPath());
			await expect(db.put('foo', 'bar')).rejects.toThrow('Database not open');
		});
	});

	describe('putSync()', () => {
		it('should put a simple key', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, { dupSort: true });
				db.putSync('foo', 'bar1');
				expect(db.getSync('foo')).toBe('bar1');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should throw an error if the key is not specified', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, { dupSort: true });
				expect(() => {
					assert(db);
					(db.putSync as any)();
				}).toThrow('Key is required');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should error if the database is not open', () => {
			const db = new RocksDatabase(generateDBPath());
			expect(() => db.putSync('foo', 'bar')).toThrow('Database not open');
		});
	});

	describe('remove()', () => {
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

		it('should error if the database is not open', async () => {
			const db = new RocksDatabase(generateDBPath());
			await expect(db.remove('foo')).rejects.toThrow('Database not open');
		});
	});

	describe('removeSync()', () => {
		it('should remove a specific value', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, { dupSort: true });
				db.putSync('foo', 'bar1');
				db.putSync('foo', 'bar2');
				db.putSync('foo', 'bar3');
				db.removeSync('foo', 'bar2');
				expect(db.getValues('foo').asArray).toEqual(['bar1', 'bar3']);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should error if the database is not open', () => {
			const db = new RocksDatabase(generateDBPath());
			expect(() => db.removeSync('foo')).toThrow('Database not open');
		});
	});
});
