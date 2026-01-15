import { describe, expect, it } from 'vitest';
import type { Key } from '../src/encoding.js';
import { dbRunner } from './lib/util.js';

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

		it('should query a range synchronously', () =>
			dbRunner(async ({ db }) => {
				for (const key of testKeys) {
					await db.put(key, 'value');
				}

				const opts = { start: Symbol.for('A') };

				const returnedKeys: Key[] = [];
				for (const { key, value } of db.getRange(opts)) {
					returnedKeys.push(key);
					expect(value).toBe(db.getSync(key));
				}
				expect(testKeys).toEqual(returnedKeys);
			}));

		it('should query a range synchronously for a column family', () =>
			dbRunner({ dbOptions: [{ name: 'foo' }] }, async ({ db }) => {
				for (const key of testKeys) {
					await db.put(key, 'value');
				}

				const opts = { start: Symbol.for('A') };

				const returnedKeys: Key[] = [];
				for (const { key, value } of db.getRange(opts)) {
					returnedKeys.push(key);
					expect(value).toBe(db.getSync(key));
				}
				expect(testKeys).toEqual(returnedKeys);
			}));

		it('should query a range asynchronously', () =>
			dbRunner(async ({ db }) => {
				for (const key of testKeys) {
					await db.put(key, 'value');
				}

				const opts = { start: Symbol.for('A') };

				const returnedKeys: Key[] = [];
				for await (const { key, value } of db.getRange(opts)) {
					returnedKeys.push(key);
					expect(value).toBe(db.getSync(key));
				}
				expect(testKeys).toEqual(returnedKeys);
			}));

		it('should get a range within a transaction', () =>
			dbRunner(async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, 'value');
				}

				const opts = { start: 'b', end: 'd' };

				await db.transaction(async txn => {
					const returnedKeys: Key[] = [];
					for await (const { key, value } of txn.getRange(opts)) {
						returnedKeys.push(key);
						expect(value).toBe(db.getSync(key));
					}
					expect(['b', 'c']).toEqual(returnedKeys);
				});
			}));

		it('should get a range within a transaction for a column family', () =>
			dbRunner({ dbOptions: [{ name: 'foo' }] }, async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, 'value');
				}

				const opts = { start: 'b', end: 'd' };

				await db.transaction(async txn => {
					const returnedKeys: Key[] = [];
					for await (const { key, value } of txn.getRange(opts)) {
						returnedKeys.push(key);
						expect(value).toBe(db.getSync(key));
					}
					expect(['b', 'c']).toEqual(returnedKeys);
				});
			}));

		it('should iterate over a range asynchronously', () =>
			dbRunner(async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, 'value');
				}

				const returnedKeys: Key[] = [];
				for await (const { key, value } of db.getRange()) {
					returnedKeys.push(key);
					expect(value).toBe(db.getSync(key));
				}
				expect(['a', 'b', 'c', 'd', 'e']).toEqual(returnedKeys);
			}));

		it('should close iterator if returning before iterator is done', () =>
			dbRunner(async ({ db }) => {
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
			}));

		it('should close iterator if throwing before iterator is done', () =>
			dbRunner(async ({ db }) => {
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
			}));

		it('should get iterate in reverse', () =>
			dbRunner(async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const returnedKeys: Key[] = [];
				for (const { key, value } of db.getRange({ reverse: true })) {
					returnedKeys.push(key);
					expect(value).toBe(db.getSync(key));
				}
				expect(['e', 'd', 'c', 'b', 'a']).toEqual(returnedKeys);
			}));

		it('should include end key', () =>
			dbRunner(async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange({ end: 'c', inclusiveEnd: true });
				expect(Array.from(iter)).toEqual([{ key: 'a', value: 'value a' }, {
					key: 'b',
					value: 'value b',
				}, { key: 'c', value: 'value c' }]);
			}));

		it('should exclude start key', () =>
			dbRunner(async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange({ start: 'a', exclusiveStart: true });
				expect(Array.from(iter)).toEqual([
					{ key: 'b', value: 'value b' },
					{ key: 'c', value: 'value c' },
					{ key: 'd', value: 'value d' },
					{ key: 'e', value: 'value e' },
				]);
			}));

		it('should get keys only', () =>
			dbRunner(async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange({ values: false });
				expect(Array.from(iter)).toEqual([{ key: 'a' }, { key: 'b' }, { key: 'c' }, { key: 'd' }, {
					key: 'e',
				}]);
			}));

		it('should error if database is not open', () =>
			dbRunner({ skipOpen: true }, async ({ db }) => {
				expect(() => db.getRange()).toThrow('Database not open');
			}));
	});

	describe('getKeys()', () => {
		it('should get keys only', () =>
			dbRunner(async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getKeys();
				expect(Array.from(iter)).toEqual(['a', 'b', 'c', 'd', 'e']);
			}));

		it('should get keys only in a column family', () =>
			dbRunner({ dbOptions: [{ name: 'foo' }] }, async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getKeys();
				expect(Array.from(iter)).toEqual(['a', 'b', 'c', 'd', 'e']);
			}));

		it('should get keys only for a transaction', () =>
			dbRunner(async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				await db.transaction(async txn => {
					const iter = txn.getKeys();
					expect(Array.from(iter)).toEqual(['a', 'b', 'c', 'd', 'e']);
				});
			}));

		it('should get keys only for a transaction and column family', () =>
			dbRunner({ dbOptions: [{ name: 'foo' }] }, async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				await db.transaction(async txn => {
					const iter = txn.getKeys();
					expect(Array.from(iter)).toEqual(['a', 'b', 'c', 'd', 'e']);
				});
			}));

		it('should error if database is not open', () =>
			dbRunner({ skipOpen: true }, async ({ db }) => {
				expect(() => db.getKeys()).toThrow('Database not open');
			}));
	});

	describe('getKeysCount()', () => {
		it('should get the number of keys for a range', () =>
			dbRunner(async ({ db }) => {
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
			}));

		it('should get the number of keys for a range in a column family', () =>
			dbRunner({ dbOptions: [{ name: 'foo' }] }, async ({ db }) => {
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
			}));

		it('should get the number of keys for a range in a transaction', () =>
			dbRunner(async ({ db }) => {
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

					count = db.getKeysCount({
						start: 'b',
						end: 'd',
						inclusiveEnd: true,
						exclusiveStart: true,
					});
					expect(count).toBe(2);
				});
			}));

		it('should get the number of keys for a range in a transaction and column family', () =>
			dbRunner({ dbOptions: [{ name: 'foo' }] }, async ({ db }) => {
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

					count = db.getKeysCount({
						start: 'b',
						end: 'd',
						inclusiveEnd: true,
						exclusiveStart: true,
					});
					expect(count).toBe(2);
				});
			}));

		it('should error if database is not open', () =>
			dbRunner({ skipOpen: true }, async ({ db }) => {
				expect(() => db.getKeysCount()).toThrow('Database not open');
			}));
	});

	describe('asArray', () => {
		it('should return a iterable as an array', () =>
			dbRunner(async ({ db }) => {
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
			}));
	});

	/*
	describe('at()', () => {
		it('should return an item at a specific index of an iterable', () => dbRunner(async ({ db }) => {
			for (const key of ['a', 'b', 'c', 'd', 'e']) {
				await db.put(key, `value ${key}`);
			}

			const iter = db.getRange();
			expect(iter.at(2)).toEqual({ key: 'c', value: 'value c' });
		}));
	});
	*/

	describe('concat()', () => {
		it('should concat two ranges', () =>
			dbRunner(async ({ db }) => {
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
			}));
	});

	/*
	describe('drop()', () => {
		it('should drop items from a range', () => dbRunner(async ({ db }) => {
			for (const key of ['a', 'b', 'c', 'd']) {
				await db.put(key, `value ${key}`);
			}

			const iter = db.getRange();
			const dropped = iter.drop(2);
			expect(Array.from(dropped)).toEqual([{ key: 'c', value: 'value c' }, { key: 'd', value: 'value d' }]);
		}));
	});
	*/

	/*
	describe('every()', () => {
		it('should return true if every item of an iterable passes a test', () => dbRunner(async ({ db }) => {
			for (const key of ['a', 'b', 'c', 'd']) {
				await db.put(key, `value ${key}`);
			}

			const iter = db.getRange();
			const every = iter.every(item => (item.value as string).startsWith('value'));
			expect(every).toBe(true);
		}));

		it('should return false if any item of an iterable fails a test', () => dbRunner(async ({ db }) => {
			for (const key of ['a', 'b', 'c', 'd']) {
				await db.put(key, `value ${key}`);
			}

			const iter = db.getRange();
			const every = iter.every(item => (item.value as string).endsWith('a'));
			expect(every).toBe(false);
		}));
	});
	*/

	describe('filter()', () => {
		it('should filter items of an iterable', () =>
			dbRunner(async ({ db }) => {
				let i = 0;
				for (const key of ['a', 'b', 'c', 'd']) {
					await db.put(key, i++);
				}

				const iter = db.getRange();
				const filtered = iter.filter(item => ((item.value as number) % 2 === 0));
				expect(Array.from(filtered)).toEqual([{ key: 'a', value: 0 }, { key: 'c', value: 2 }]);
			}));
	});

	/*
	describe('find()', () => {
		it('should find the first item of an iterable', () => dbRunner(async ({ db }) => {
			for (const key of ['a', 'b', 'c', 'd']) {
				await db.put(key, `value ${key}`);
			}

			const iter = db.getRange();
			const found = iter.find(item => item.value === 'value c');
			expect(found).toEqual({ key: 'c', value: 'value c' });
		}));
	});
	*/

	describe('flatMap()', () => {
		it('should flatten a range', () =>
			dbRunner(async ({ db }) => {
				for (const key of ['a', 'b', 'c']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const flattened = iter.flatMap(item => [item.value, item.value]);
				expect(Array.from(flattened)).toEqual([
					'value a',
					'value a',
					'value b',
					'value b',
					'value c',
					'value c',
				]);
			}));
	});

	describe('forEach()', () => {
		it('should call a function for each item of an iterable', () =>
			dbRunner(async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const values: string[] = [];
				iter.forEach(item => values.push(item.value as string));
				expect(values).toEqual(['value a', 'value b', 'value c', 'value d', 'value e']);
			}));
	});

	describe('map()', () => {
		it('should map each item of an iterable', () =>
			dbRunner(async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const mapped = iter.map(item => {
					return { ...item, value: `${item.value}!` };
				});

				expect(Array.from(mapped)).toEqual([
					{ key: 'a', value: 'value a!' },
					{ key: 'b', value: 'value b!' },
					{ key: 'c', value: 'value c!' },
					{ key: 'd', value: 'value d!' },
					{ key: 'e', value: 'value e!' },
				]);
			}));
	});

	describe('mapError()', () => {
		it('should catch errors thrown iterating over a range', () =>
			dbRunner(async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const mapped = iter.map(item => {
					if (item.value === 'value c') {
						throw new Error('found c');
					}
					return { ...item, value: `${item.value}!` };
				}).mapError(error => {
					return new Error(`Error: ${(error as Error).message}`);
				});

				expect(Array.from(mapped)).toEqual([
					{ key: 'a', value: 'value a!' },
					{ key: 'b', value: 'value b!' },
					new Error('Error: found c'),
					{ key: 'd', value: 'value d!' },
					{ key: 'e', value: 'value e!' },
				]);
			}));
	});

	/*
	describe('reduce()', () => {
		it('should reduce a range', () => dbRunner(async ({ db }) => {
			for (const key of ['a', 'b', 'c', 'd', 'e']) {
				await db.put(key, `value ${key}`);
			}

			const iter = db.getRange();
			const reduced = iter.reduce((acc, item) => acc + (item.value as string).length, 0);
			expect(reduced).toEqual(7 * 5);
		}));
	});
	*/

	describe('slice()', () => {
		it('should slice a range', () =>
			dbRunner(async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const sliced = iter.slice(1, 3);
				expect(Array.from(sliced)).toEqual([{ key: 'b', value: 'value b' }, {
					key: 'c',
					value: 'value c',
				}]);
			}));
	});

	/*
	describe('some()', () => {
		it('should check if some items in a range pass a test', () => dbRunner(async ({ db }) => {
			for (const key of ['a', 'b', 'c', 'd', 'e']) {
				await db.put(key, `value ${key}`);
			}

			const iter = db.getRange();
			expect(iter.some(item => (item.value === 'value c'))).toBe(true);

			const iter2 = db.getRange();
			expect(iter2.some(item => (item.value === 'value f'))).toBe(false);
		}));
	});
	*/

	/*
	describe('take()', () => {
		it('should take items from a range', () => dbRunner(async ({ db }) => {
			for (const key of ['a', 'b', 'c', 'd', 'e']) {
				await db.put(key, `value ${key}`);
			}

			const iter = db.getRange();
			const taken = iter.take(2);
			expect(Array.from(taken)).toEqual([{ key: 'a', value: 'value a' }, { key: 'b', value: 'value b' }]);
		}));
	});
	*/

	describe('chaining', () => {
		it('should chain multiple methods', () =>
			dbRunner(async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const results = iter.map((item, index) => {
					return { ...item, value: `${item.value}${index % 2 === 0 ? '!' : ''}` };
				}).filter(item => item.value.endsWith('!'));

				expect(Array.from(results)).toEqual([{ key: 'a', value: 'value a!' }, {
					key: 'c',
					value: 'value c!',
				}, { key: 'e', value: 'value e!' }]);
			}));

		it('should map error', () =>
			dbRunner(async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const results = iter.map((item, index) => {
					if (index === 1) {
						throw new Error('test');
					}
					return { ...item, value: `${item.value}${index % 2 === 0 ? '!' : ''}` };
				}).mapError(error => error);

				const arr = Array.from(results);
				expect(arr.length).toEqual(5);
				expect(arr[0]).toEqual({ key: 'a', value: 'value a!' });
				expect(arr[1]).toBeInstanceOf(Error);
				expect((arr[1] as Error).message).toEqual('test');
				expect(arr[2]).toEqual({ key: 'c', value: 'value c!' });
				expect(arr[3]).toEqual({ key: 'd', value: 'value d' });
				expect(arr[4]).toEqual({ key: 'e', value: 'value e!' });
			}));

		it('should close iterator if throwing before iterator is done', () =>
			dbRunner(async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const results = iter.map((item, index) => {
					if (index === 1) {
						throw new Error('test');
					}
					return item;
				});

				const iterator = results[Symbol.iterator]();
				expect(iterator.next()).toEqual({ value: { key: 'a', value: 'value a' } });
				expect(() => iterator.next()).toThrow('test');
			}));

		it('should close iterator if returning before iterator is done', () =>
			dbRunner(async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const results = iter.map(item => {
					return { ...item, value: `${item.value}!` };
				}).mapError(error => error);

				const iterator = results[Symbol.iterator]();
				expect(iterator.return?.()).toEqual({ done: true, value: undefined });
				expect(() => iterator.next()).toThrow('Next failed: Iterator not initialized');
			}));

		it('should get range with shared structures key', () =>
			dbRunner(
				{ dbOptions: [{ sharedStructuresKey: Symbol.for('structures') }] },
				async ({ db }) => {
					db.putSync(Symbol.for('test'), 2);

					const data = { bar: 'baz' };

					db.putSync('foo', data);

					const iterable = db.getRange({ start: true });
					const results = iterable.asArray;
					expect(results).toHaveLength(1);
					expect(results[0].key).toEqual('foo');
					expect(results[0].value).toEqual(data);
				}
			));
	});
});
