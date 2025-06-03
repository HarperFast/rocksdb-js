import { describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';
import { RangeIterable } from '../src/iterator.js';
import type { Key } from '../src/encoding.js';

async function initTestDB(test: (db: RocksDatabase) => Promise<void>, name?: string) {
	let db: RocksDatabase | null = null;
	const dbPath = generateDBPath();

	try {
		db = await RocksDatabase.open(dbPath, { name });
		await test(db);
	} finally {
		db?.close();
		await rimraf(dbPath);
	}
}

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
			await initTestDB(async db => {
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
			});
		});

		it('should query a range synchronously for a column family', async () => {
			await initTestDB(async db => {
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
			}, 'foo');
		});

		it('should query a range asynchronously', async () => {
			await initTestDB(async db => {
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
			});
		});

		it('should get a range within a transaction', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, 'value');
				}

				const opts = {
					start: 'b',
					end: 'd'
				};

				await db.transaction(async txn => {
					const returnedKeys: Key[] = [];
					for await (const { key, value } of txn.getRange(opts)) {
						returnedKeys.push(key);
						expect(value).toBe(db.getSync(key));
					}
					expect(['b', 'c']).toEqual(returnedKeys);
				});
			});
		});

		it('should get a range within a transaction for a column family', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, 'value');
				}

				const opts = {
					start: 'b',
					end: 'd'
				};

				await db.transaction(async txn => {
					const returnedKeys: Key[] = [];
					for await (const { key, value } of txn.getRange(opts)) {
						returnedKeys.push(key);
						expect(value).toBe(db.getSync(key));
					}
					expect(['b', 'c']).toEqual(returnedKeys);
				});
			}, 'foo');
		});

		it('should iterate over a range asynchronously', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, 'value');
				}

				const returnedKeys: Key[] = [];
				for await (const { key, value } of db.getRange()) {
					returnedKeys.push(key);
					expect(value).toBe(db.getSync(key));
				}
				expect(['a', 'b', 'c', 'd', 'e']).toEqual(returnedKeys);
			});
		});

		it('should close iterator if returning before iterator is done', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange()[Symbol.iterator]();
				const first = iter.next();
				expect(first).toEqual({ done: false, value: { key: 'a', value: 'value a' } });

				if (iter.return) {
					const rval = iter.return();
					expect(rval).toEqual({ done: true, value: undefined });
				} else {
					throw new Error('Iterator does not support return');
				}
			});
		});

		it('should close iterator if throwing before iterator is done', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				try {
					const iter = db.getRange()[Symbol.iterator]();
					const first = iter.next();
					expect(first).toEqual({ done: false, value: { key: 'a', value: 'value a' } });

					if (iter.throw) {
						iter.throw(new Error('test'));
					} else {
						throw new Error('Iterator does not support throw');
					}
				} catch (error) {
					console.log(error);
				}
			});
		});

		it('should get iterate in reverse', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const returnedKeys: Key[] = [];
				for (const { key, value } of db.getRange({ reverse: true })) {
					returnedKeys.push(key);
					expect(value).toBe(db.getSync(key));
				}
				expect(['e', 'd', 'c', 'b', 'a']).toEqual(returnedKeys);
			});
		});

		it('should include end key', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange({ end: 'c', inclusiveEnd: true });
				expect(Array.from(iter)).toEqual([
					{ key: 'a', value: 'value a' },
					{ key: 'b', value: 'value b' },
					{ key: 'c', value: 'value c' }
				]);
			});
		});

		it('should exclude start key', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange({ start: 'a', exclusiveStart: true });
				expect(Array.from(iter)).toEqual([
					{ key: 'b', value: 'value b' },
					{ key: 'c', value: 'value c' },
					{ key: 'd', value: 'value d' },
					{ key: 'e', value: 'value e' }
				]);
			});
		});

		it('should get keys only', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange({ values: false });
				expect(Array.from(iter)).toEqual([
					{ key: 'a' },
					{ key: 'b' },
					{ key: 'c' },
					{ key: 'd' },
					{ key: 'e' }
				]);
			});
		});

		it('should error if database is not open', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				expect(() => {
					db = new RocksDatabase(dbPath);
					db.getRange();
				}).toThrow('Database not open');
			} finally {
				await rimraf(dbPath);
			}
		});
	});

	describe('getKeys()', () => {
		it('should get keys only', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getKeys();
				expect(Array.from(iter)).toEqual([
					{ key: 'a' },
					{ key: 'b' },
					{ key: 'c' },
					{ key: 'd' },
					{ key: 'e' }
				]);
			});
		});

		it('should get keys only in a column family', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getKeys();
				expect(Array.from(iter)).toEqual([
					{ key: 'a' },
					{ key: 'b' },
					{ key: 'c' },
					{ key: 'd' },
					{ key: 'e' }
				]);
			}, 'foo');
		});

		it('should get keys only for a transaction', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				await db.transaction(async txn => {
					const iter = txn.getKeys();
					expect(Array.from(iter)).toEqual([
						{ key: 'a' },
						{ key: 'b' },
						{ key: 'c' },
						{ key: 'd' },
						{ key: 'e' }
					]);
				});
			});
		});

		it('should get keys only for a transaction and column family', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				await db.transaction(async txn => {
					const iter = txn.getKeys();
					expect(Array.from(iter)).toEqual([
						{ key: 'a' },
						{ key: 'b' },
						{ key: 'c' },
						{ key: 'd' },
						{ key: 'e' }
					]);
				});
			}, 'foo');
		});

		it('should error if database is not open', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				expect(() => {
					db = new RocksDatabase(dbPath);
					db.getKeys();
				}).toThrow('Database not open');
			} finally {
				await rimraf(dbPath);
			}
		});
	});

	describe('getKeysCount()', () => {
		it('should get the number of keys for a range', async () => {
			await initTestDB(async db => {
				let count = db.getKeysCount();
				expect(count).toBe(0);

				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				count = db.getKeysCount();
				expect(count).toBe(5);

				count = db.getKeysCount({ start: 'b' });
				expect(count).toBe(4);

				count = db.getKeysCount({ start: 'b', exclusiveStart: true });
				expect(count).toBe(3);

				count = db.getKeysCount({ end: 'c' });
				expect(count).toBe(2);

				count = db.getKeysCount({ end: 'c', inclusiveEnd: true });
				expect(count).toBe(3);

				count = db.getKeysCount({ start: 'b', end: 'd' });
				expect(count).toBe(2);

				count = db.getKeysCount({ start: 'b', end: 'd', inclusiveEnd: true });
				expect(count).toBe(3);

				count = db.getKeysCount({ start: 'b', end: 'd', exclusiveStart: true });
				expect(count).toBe(1);

				count = db.getKeysCount({ start: 'b', end: 'd', inclusiveEnd: true, exclusiveStart: true });
				expect(count).toBe(2);
			});
		});

		it('should get the number of keys for a range in a column family', async () => {
			await initTestDB(async db => {
				let count = db.getKeysCount();
				expect(count).toBe(0);

				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				count = db.getKeysCount();
				expect(count).toBe(5);

				count = db.getKeysCount({ start: 'b' });
				expect(count).toBe(4);

				count = db.getKeysCount({ start: 'b', exclusiveStart: true });
				expect(count).toBe(3);

				count = db.getKeysCount({ end: 'c' });
				expect(count).toBe(2);

				count = db.getKeysCount({ end: 'c', inclusiveEnd: true });
				expect(count).toBe(3);

				count = db.getKeysCount({ start: 'b', end: 'd' });
				expect(count).toBe(2);

				count = db.getKeysCount({ start: 'b', end: 'd', inclusiveEnd: true });
				expect(count).toBe(3);

				count = db.getKeysCount({ start: 'b', end: 'd', exclusiveStart: true });
				expect(count).toBe(1);

				count = db.getKeysCount({ start: 'b', end: 'd', inclusiveEnd: true, exclusiveStart: true });
				expect(count).toBe(2);
			}, 'foo');
		});

		it('should get the number of keys for a range in a transaction', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				await db.transaction(async txn => {
					let count = txn.getKeysCount();
					expect(count).toBe(5);

					count = db.getKeysCount({ start: 'b' });
					expect(count).toBe(4);

					count = db.getKeysCount({ start: 'b', exclusiveStart: true });
					expect(count).toBe(3);

					count = db.getKeysCount({ end: 'c' });
					expect(count).toBe(2);

					count = db.getKeysCount({ end: 'c', inclusiveEnd: true });
					expect(count).toBe(3);

					count = db.getKeysCount({ start: 'b', end: 'd' });
					expect(count).toBe(2);

					count = db.getKeysCount({ start: 'b', end: 'd', inclusiveEnd: true });
					expect(count).toBe(3);

					count = db.getKeysCount({ start: 'b', end: 'd', exclusiveStart: true });
					expect(count).toBe(1);

					count = db.getKeysCount({ start: 'b', end: 'd', inclusiveEnd: true, exclusiveStart: true });
					expect(count).toBe(2);
				});
			});
		});

		it('should get the number of keys for a range in a transaction and column family', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				await db.transaction(async txn => {
					let count = txn.getKeysCount();
					expect(count).toBe(5);

					count = db.getKeysCount({ start: 'b' });
					expect(count).toBe(4);

					count = db.getKeysCount({ start: 'b', exclusiveStart: true });
					expect(count).toBe(3);

					count = db.getKeysCount({ end: 'c' });
					expect(count).toBe(2);

					count = db.getKeysCount({ end: 'c', inclusiveEnd: true });
					expect(count).toBe(3);

					count = db.getKeysCount({ start: 'b', end: 'd' });
					expect(count).toBe(2);

					count = db.getKeysCount({ start: 'b', end: 'd', inclusiveEnd: true });
					expect(count).toBe(3);

					count = db.getKeysCount({ start: 'b', end: 'd', exclusiveStart: true });
					expect(count).toBe(1);

					count = db.getKeysCount({ start: 'b', end: 'd', inclusiveEnd: true, exclusiveStart: true });
					expect(count).toBe(2);
				});
			}, 'foo');
		});

		it('should error if database is not open', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				expect(() => {
					db = new RocksDatabase(dbPath);
					db.getKeysCount();
				}).toThrow('Database not open');
			} finally {
				await rimraf(dbPath);
			}
		});
	});

	describe('getValues()', () => {
		it('should get values for a key', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getValues('b');
				expect(Array.from(iter)).toEqual([{ key: 'b', value: 'value b' }]);
			});
		});

		it('should get values for a key in a column family', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getValues('b');
				expect(Array.from(iter)).toEqual([{ key: 'b', value: 'value b' }]);
			}, 'foo');
		});

		it('should get values for a key in a transaction', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				await db.transaction(async txn => {
					const iter = txn.getValues('b');
					expect(Array.from(iter)).toEqual([{ key: 'b', value: 'value b' }]);
				});
			});
		});

		it('should get values for a key in a transaction and column family', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				await db.transaction(async txn => {
					const iter = txn.getValues('b');
					expect(Array.from(iter)).toEqual([{ key: 'b', value: 'value b' }]);
				});
			}, 'foo');
		});

		it('should error if database is not open', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				expect(() => {
					db = new RocksDatabase(dbPath);
					db.getValues('b');
				}).toThrow('Database not open');
			} finally {
				await rimraf(dbPath);
			}
		});
	});

	describe('getValuesCount()', () => {
		it('should get the number of values for a key', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const count = db.getValuesCount('b');
				expect(count).toEqual(1);
			});
		});

		it('should get the number of values for a key in a column family', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const count = db.getValuesCount('b');
				expect(count).toEqual(1);
			}, 'foo');
		});

		it('should get the number of values for a key in a transaction', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				await db.transaction(async txn => {
					const count = txn.getValuesCount('b');
					expect(count).toEqual(1);
				});
			});
		});

		it('should get the number of values for a key in a transaction and column family', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				await db.transaction(async txn => {
					const count = txn.getValuesCount('b');
					expect(count).toEqual(1);
				});
			}, 'foo');
		});

		it('should error if database is not open', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				expect(() => {
					db = new RocksDatabase(dbPath);
					db.getValuesCount('b');
				}).toThrow('Database not open');
			} finally {
				await rimraf(dbPath);
			}
		});
	});

	describe('RangeIterable.asArray', () => {
		it('should return a iterable as an array', async () => {
			await initTestDB(async db => {
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
			});
		});

		it('should return an array as an array', async () => {
			const iter = new RangeIterable([1, 2, 3, 4]);
			const array = iter.asArray;
			expect(array).toEqual([1, 2, 3, 4]);
		});
	});

	describe('RangeIterable.at()', () => {
		it('should return an item at a specific index of an iterable', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				expect(iter.at(2)).toEqual({ key: 'c', value: 'value c' });
			});

			const iter = new RangeIterable([1, 2, 3, 4]);
			expect(iter.at(2)).toEqual(3);
		});

		it('should return an item at a specific index of an array', async () => {
			const iter = new RangeIterable([1, 2, 3, 4]);
			expect(iter.at(2)).toEqual(3);
		});
	});

	describe('RangeIterable.concat()', () => {
		it('should concat two ranges', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange({ start: 'a', end: 'c' });
				const iter2 = db.getRange({ start: 'c', end: 'e' });
				const concat = iter.concat(iter2);
				expect(Array.from(concat)).toEqual([
					{ key: 'a', value: 'value a' },
					{ key: 'b', value: 'value b' },
					{ key: 'c', value: 'value c' },
					{ key: 'd', value: 'value d' },
				]);
			});
		});

		it('should concat two arrays', async () => {
			const iter = new RangeIterable([1, 2, 3, 4]);
			const iter2 = new RangeIterable([5, 6, 7, 8]);
			const concat = iter.concat(iter2);
			expect(Array.from(concat)).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
		});
	});

	describe('RangeIterable.drop()', () => {
		it('should drop items from a range', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const dropped = iter.drop(2);
				expect(Array.from(dropped)).toEqual([{ key: 'c', value: 'value c' }, { key: 'd', value: 'value d' }]);
			});
		});

		it('should drop items from an array', async () => {
			const iter = new RangeIterable([1, 2, 3, 4]);
			const dropped = iter.drop(2);
			expect(Array.from(dropped)).toEqual([3, 4]);
		});
	});

	describe('RangeIterable.every()', () => {
		it('should return true if every item of an iterable passes a test', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const every = iter.every(item => (item.value as string).startsWith('value'));
				expect(every).toBe(true);
			});
		});

		it('should return false if any item of an iterable fails a test', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const every = iter.every(item => (item.value as string).endsWith('a'));
				expect(every).toBe(false);
			});
		});

		it('should return true if every item of an array passes a test', async () => {
			const iter = new RangeIterable([1, 2, 3, 4]);
			const every = iter.every(item => item > 0);
			expect(every).toBe(true);
		});

		it('should return false if any item of an array fails a test', async () => {
			const iter = new RangeIterable([1, 2, 3, 4]);
			const every = iter.every(item => item > 1);
			expect(every).toBe(false);
		});
	});

	describe('RangeIterable.filter()', () => {
		it('should filter items of an iterable', async () => {
			await initTestDB(async db => {
				let i = 0;
				for (const key of ['a', 'b', 'c', 'd']) {
					await db.put(key, i++);
				}

				const iter = db.getRange();
				const filtered = iter.filter(item => ((item.value as number) % 2 === 0));
				expect(Array.from(filtered)).toEqual([
					{ key: 'a', value: 0 },
					{ key: 'c', value: 2 }
				]);
			});
		});

		it('should filter items of an array', async () => {
			const iter = new RangeIterable([1, 2, 3, 4]);
			const filtered = iter.filter(item => item % 2 === 0);
			expect(Array.from(filtered)).toEqual([2, 4]);
		});
	});

	describe('RangeIterable.find()', () => {
		it('should find the first item of an iterable', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const found = iter.find(item => item.value === 'value c');
				expect(found).toEqual({ key: 'c', value: 'value c' });
			});
		});

		it('should find the first item of an array', async () => {
			const iter = new RangeIterable([1, 2, 3, 4]);
			const found = iter.find(item => item === 3);
			expect(found).toEqual(3);
		});
	});

	describe('RangeIterable.flatMap()', () => {
		it('should flatten a range', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const flattened = iter.flatMap(item => [item.value, item.value]);
				expect(Array.from(flattened)).toEqual([
					'value a', 'value a', 'value b', 'value b', 'value c', 'value c'
				]);
			});
		});

		it('should flatten an array', async () => {
			const iter = new RangeIterable([1, 2, 3, 4]);
			const flattened = iter.flatMap(item => [item, item]);
			expect(Array.from(flattened)).toEqual([1, 1, 2, 2, 3, 3, 4, 4]);
		});
	});

	describe('RangeIterable.forEach()', () => {
		it('should call a function for each item of an iterable', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const values: string[] = [];
				iter.forEach(item => values.push(item.value as string));
				expect(values).toEqual(['value a', 'value b', 'value c', 'value d', 'value e']);
			});
		});

		it('should call a function for each item of an array', async () => {
			const iter = new RangeIterable([1, 2, 3, 4]);
			const values: number[] = [];
			iter.forEach(item => values.push(item));
			expect(values).toEqual([1, 2, 3, 4]);
		});
	});

	describe('RangeIterable.map()', () => {
		it('should map each item of an iterable', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const mapped = iter.map(item => {
					return {
						...item,
						value: `${item.value}!`
					};
				});

				expect(Array.from(mapped)).toEqual([
					{ key: 'a', value: 'value a!' },
					{ key: 'b', value: 'value b!' },
					{ key: 'c', value: 'value c!' },
					{ key: 'd', value: 'value d!' },
					{ key: 'e', value: 'value e!' },
				]);
			});
		});

		it('should map each item of an array', async () => {
			const iter = new RangeIterable([1, 2, 3, 4]);
			const mapped = iter.map(item => item * 2);
			expect(Array.from(mapped)).toEqual([2, 4, 6, 8]);
		});
	});

	describe('RangeIterable.mapError()', () => {
		it('should catch errors thrown iterating over a range', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const mapped = iter
					.map(item => {
						if (item.value === 'value c') {
							throw new Error('found c');
						}
						return {
							...item,
							value: `${item.value}!`
						};
					})
					.mapError(error => {
						return new Error(`Error: ${error.message}`);
					});

				expect(Array.from(mapped)).toEqual([
					{ key: 'a', value: 'value a!' },
					{ key: 'b', value: 'value b!' },
					new Error('Error: found c'),
					{ key: 'd', value: 'value d!' },
					{ key: 'e', value: 'value e!' },
				]);
			});
		});

		it('should catch errors thrown iterating over an array', async () => {
			const iter = new RangeIterable([1, 2, 3, 4]);
			const mapped = iter
				.map(item => {
					if (item === 3) {
						throw new Error('found 3');
					}
					return item;
				})
				.mapError(error => {
					return new Error(`Error: ${error.message}`);
				});

			expect(Array.from(mapped)).toEqual([1, 2, new Error('Error: found 3'), 4]);
		});
	});

	describe('RangeIterable.reduce()', () => {
		it('should reduce a range', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const reduced = iter.reduce((acc, item) => acc + (item.value as string).length, 0);
				expect(reduced).toEqual(7 * 5);
			});
		});

		it('should reduce an array', async () => {
			const iter = new RangeIterable([1, 2, 3, 4]);
			const reduced = iter.reduce((acc, item) => acc + item, 0);
			expect(reduced).toEqual(10);
		});
	});

	describe('RangeIterable.slice()', () => {
		it('should slice a range', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const sliced = iter.slice(1, 3);
				expect(Array.from(sliced)).toEqual([{ key: 'b', value: 'value b' }, { key: 'c', value: 'value c' }]);
			});
		});

		it('should slice an array', async () => {
			const iter = new RangeIterable([1, 2, 3, 4]);
			const sliced = iter.slice(1, 3);
			expect(Array.from(sliced)).toEqual([2, 3]);
		});
	});

	describe('RangeIterable.some()', () => {
		it('should check if some items in a range pass a test', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				expect(iter.some(item => (item.value === 'value c'))).toBe(true);

				const iter2 = db.getRange();
				expect(iter2.some(item => (item.value === 'value f'))).toBe(false);
			});
		});

		it('should check if some items in an array pass a test', async () => {
			const iter = new RangeIterable([1, 2, 3, 4]);
			expect( iter.some(item => item === 5)).toBe(false);

			const iter2 = new RangeIterable([1, 2, 3, 4]);
			expect(iter2.some(item => item === 3)).toBe(true);
		});
	});

	describe('RangeIterable.take()', () => {
		it('should take items from a range', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const taken = iter.take(2);
				expect(Array.from(taken)).toEqual([{ key: 'a', value: 'value a' }, { key: 'b', value: 'value b' }]);
			});
		});

		it('should take items from an array', async () => {
			const iter = new RangeIterable([1, 2, 3, 4]);
			const taken = iter.take(2);
			expect(Array.from(taken)).toEqual([1, 2]);
		});
	});

	describe('RangeIterable chaining', () => {
		it('should chain multiple methods', async () => {
			await initTestDB(async db => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const results = iter
					.map((item, index) => {
						return {
							...item,
							value: `${item.value}${index % 2 === 0 ? '!' : ''}`
						};
					})
					.filter(item => item.value.endsWith('!'))
					.reduce((acc, item) => acc + item.value.length, 0);

				expect(results).toEqual('value ?!'.length * 3);
			});
		});
	});
});
