import type { IteratorOptions } from '../src/dbi.ts';
import type { Key } from '../src/encoding.ts';
import { Transaction } from '../src/transaction.ts';
import { dbRunner, generateDBPath } from './lib/util.ts';
import { describe, expect, it } from 'vitest';

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

				await db.transaction(async (txn) => {
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

				await db.transaction(async (txn) => {
					const returnedKeys: Key[] = [];
					for await (const { key, value } of txn.getRange(opts)) {
						returnedKeys.push(key);
						expect(value).toBe(db.getSync(key));
					}
					expect(['b', 'c']).toEqual(returnedKeys);
				});
			}));

		it('should honor the transaction option across range APIs', () =>
			dbRunner(async ({ db }) => {
				await db.put('committed', 'before');
				const txn = new Transaction(db.store);
				try {
					await txn.put('staged', 'in-batch');
					const options = { start: 'a', end: 'z', transaction: txn };

					expect(await db.get('staged', { transaction: txn })).toBe('in-batch');
					expect(db.getRange(options).asArray).toEqual([
						{ key: 'committed', value: 'before' },
						{ key: 'staged', value: 'in-batch' },
					]);
					expect(db.getKeys(options).asArray).toEqual(['committed', 'staged']);
					expect(db.getKeysCount(options)).toBe(2);
					expect(db.store.getCount(db._context, options)).toBe(2);
					expect(txn.getRange({ start: 'a', end: 'z' }).asArray).toEqual([
						{ key: 'committed', value: 'before' },
						{ key: 'staged', value: 'in-batch' },
					]);
				} finally {
					txn.abort();
				}
			}));

		it('should use the transaction snapshot for routed and direct ranges', () =>
			dbRunner(async ({ db }) => {
				await db.put('committed', 'before');
				const txn = new Transaction(db.store);
				try {
					expect(await db.get('committed', { transaction: txn })).toBe('before');
					await db.transaction(async (writer) => {
						await writer.put('later', 'after');
					});

					const options = { start: 'a', end: 'z', transaction: txn };
					const expected = [{ key: 'committed', value: 'before' }];
					expect(db.getRange(options).asArray).toEqual(expected);
					expect(txn.getRange({ start: 'a', end: 'z' }).asArray).toEqual(expected);
				} finally {
					txn.abort();
				}
			}));

		it('should enforce bounds and limits for staged keys in both directions', () =>
			dbRunner(
				{ dbOptions: [{}, { path: generateDBPath() }] },
				async ({ db }, { db: reference }) => {
					for (const key of ['a', 'b', 'c', 'd', 'e', 'f', 'g']) {
						await reference.put(key, `value ${key}`);
					}
					for (const key of ['b', 'd', 'f']) {
						await db.put(key, `value ${key}`);
					}

					const txn = new Transaction(db.store);
					try {
						for (const key of ['a', 'c', 'e', 'g']) {
							await txn.put(key, `value ${key}`);
						}

						const forward = { start: 'a', end: 'e', exclusiveStart: true, inclusiveEnd: true };
						const reverse = { start: 'e', end: 'a', reverse: true };
						const reverseOpen = { ...reverse, exclusiveStart: false, inclusiveEnd: false };
						expect(reference.getKeys(forward).asArray).toEqual(['b', 'c', 'd', 'e']);
						expect(reference.getKeys(reverse).asArray).toEqual(['e', 'd', 'c', 'b']);
						expect(reference.getKeys(reverseOpen).asArray).toEqual(['d', 'c', 'b', 'a']);

						const variants: IteratorOptions[] = [
							{ start: 'a', end: 'e' },
							forward,
							{ start: 'a', end: 'e', limit: 2 },
							{ end: 'c' },
							{ key: 'c' },
							{ key: 'e' },
							reverse,
							reverseOpen,
							{ ...reverse, limit: 2 },
							{ start: 'g', reverse: true },
						];
						for (const variant of variants) {
							const expected = reference.getKeys(variant).asArray;
							expect(expected.length).toBeGreaterThan(0);
							expect(db.getKeys({ ...variant, transaction: txn }).asArray).toEqual(expected);
							expect(txn.getKeys(variant).asArray).toEqual(expected);
							if (!variant.reverse && variant.limit === undefined && variant.key === undefined) {
								expect(db.getKeysCount({ ...variant, transaction: txn })).toBe(expected.length);
								expect(db.store.getCount(db._context, { ...variant, transaction: txn })).toBe(
									expected.length
								);
							}
						}
					} finally {
						txn.abort();
					}
				}
			));

		it('should exclude a staged key that lands exactly on the encoded end bound', () =>
			dbRunner(
				{
					dbOptions: [{ keyEncoding: 'binary' }, { keyEncoding: 'binary', path: generateDBPath() }],
				},
				async ({ db }, { db: reference }) => {
					// `inclusiveEnd` makes the native bound `b\0`, so a key equal to `b\0` sits on it
					const [a, b, bNul, c] = ['a', 'b', 'b\0', 'c'].map((key) => Buffer.from(key, 'latin1'));
					for (const key of [a, b, bNul, c]) {
						await reference.put(key, key.toString('latin1'));
					}
					await db.put(a, 'a');
					await db.put(c, 'c');

					const txn = new Transaction(db.store);
					try {
						await txn.put(b, 'b');
						await txn.put(bNul, 'b\0');

						const decode = (keys: Uint8Array[]) =>
							keys.map((key) => Buffer.from(key).toString('latin1'));
						const variants: IteratorOptions[] = [
							{ start: b, end: a, reverse: true },
							{ start: a, end: b, inclusiveEnd: true },
						];
						for (const variant of variants) {
							const expected = decode(reference.getKeys(variant).asArray);
							expect(expected).toEqual(variant.reverse ? ['b'] : ['a', 'b']);
							expect(decode(db.getKeys({ ...variant, transaction: txn }).asArray)).toEqual(
								expected
							);
							expect(decode(txn.getKeys(variant).asArray)).toEqual(expected);
						}
					} finally {
						txn.abort();
					}
				}
			));

		it('should reflect staged overwrites and deletes in routed ranges', () =>
			dbRunner(async ({ db }) => {
				await db.put('a', 'committed-a');
				await db.put('b', 'committed-b');
				const txn = new Transaction(db.store);
				try {
					await txn.put('a', 'staged-a');
					await txn.remove('b');
					await txn.put('c', 'staged-c');

					const expected = [
						{ key: 'a', value: 'staged-a' },
						{ key: 'c', value: 'staged-c' },
					];
					expect(db.getRange({ transaction: txn }).asArray).toEqual(expected);
					expect(db.getKeysCount({ transaction: txn })).toBe(2);
					expect(txn.getRange().asArray).toEqual(expected);
					expect(db.getRange().asArray).toEqual([
						{ key: 'a', value: 'committed-a' },
						{ key: 'b', value: 'committed-b' },
					]);
				} finally {
					txn.abort();
				}
			}));

		it('should iterate staged keys over an empty database', () =>
			dbRunner(async ({ db }) => {
				const txn = new Transaction(db.store);
				try {
					await txn.put('b', 'staged-b');
					await txn.put('a', 'staged-a');
					expect(db.getKeys({ transaction: txn }).asArray).toEqual(['a', 'b']);
					expect(db.getKeys({ transaction: txn, reverse: true }).asArray).toEqual(['b', 'a']);
					expect(db.getKeysCount({ transaction: txn })).toBe(2);
					expect(db.getKeys().asArray).toEqual([]);
				} finally {
					txn.abort();
				}
			}));

		it('should establish the transaction snapshot on a first routed range read', () =>
			dbRunner(async ({ db }) => {
				await db.put('committed', 'before');
				const txn = new Transaction(db.store);
				try {
					expect(db.getKeys({ transaction: txn }).asArray).toEqual(['committed']);
					await db.transaction(async (writer) => {
						await writer.put('later', 'after');
					});
					expect(db.getKeys({ transaction: txn }).asArray).toEqual(['committed']);
					expect(db.getKeysCount({ transaction: txn })).toBe(1);
					expect(await db.get('later', { transaction: txn })).toBeUndefined();
					expect(db.getKeys().asArray).toEqual(['committed', 'later']);
				} finally {
					txn.abort();
				}
			}));

		it('should read the latest committed state through a disableSnapshot transaction', () =>
			dbRunner(async ({ db }) => {
				await db.put('committed', 'before');
				const txn = new Transaction(db.store, { disableSnapshot: true });
				try {
					expect(db.getKeys({ transaction: txn }).asArray).toEqual(['committed']);
					await db.transaction(async (writer) => {
						await writer.put('later', 'after');
					});
					expect(db.getKeys({ transaction: txn }).asArray).toEqual(['committed', 'later']);
					expect(db.getKeysCount({ transaction: txn })).toBe(2);
					expect(txn.getKeys().asArray).toEqual(['committed', 'later']);
				} finally {
					txn.abort();
				}
			}));

		it('should read the latest committed state through a tailing transactional range', () =>
			dbRunner(async ({ db }) => {
				await db.put('committed', 'before');
				const txn = new Transaction(db.store);
				try {
					expect(db.getKeys({ transaction: txn }).asArray).toEqual(['committed']);
					await db.transaction(async (writer) => {
						await writer.put('later', 'after');
					});
					expect(db.getKeys({ transaction: txn, tailing: true }).asArray).toEqual([
						'committed',
						'later',
					]);
					expect(db.getKeys({ transaction: txn }).asArray).toEqual(['committed']);
				} finally {
					txn.abort();
				}
			}));

		it('should honor the transaction option on a pessimistic database', () =>
			dbRunner({ dbOptions: [{ pessimistic: true }] }, async ({ db }) => {
				await db.put('committed', 'before');
				const txn = new Transaction(db.store);
				try {
					await txn.put('staged', 'in-batch');
					expect(db.getKeys({ transaction: txn }).asArray).toEqual(['committed', 'staged']);
					expect(db.getKeysCount({ transaction: txn })).toBe(2);
					await db.transaction(async (writer) => {
						await writer.put('later', 'after');
					});
					expect(db.getKeys({ transaction: txn }).asArray).toEqual(['committed', 'staged']);
				} finally {
					txn.abort();
				}
			}));

		it('should reject routed ranges and counts once the transaction starts committing', () =>
			dbRunner(async ({ db }) => {
				const txn = new Transaction(db.store);
				await txn.put('a', 'staged');
				const committing = txn.commit();
				expect(() => db.getRange({ transaction: txn })).toThrow('not in pending state');
				expect(() => db.getKeysCount({ transaction: txn })).toThrow('not in pending state');
				expect(() => txn.getRange()).toThrow('not in pending state');
				expect(() => txn.getKeysCount()).toThrow('not in pending state');
				await committing;
				expect(() => db.getRange({ transaction: txn })).toThrow('Transaction not found');
				expect(db.getKeys().asArray).toEqual(['a']);
			}));

		it('should reject a transaction that belongs to another database', () =>
			dbRunner({ dbOptions: [{}, { path: generateDBPath() }] }, async ({ db }, { db: other }) => {
				const txn = new Transaction(db.store);
				const otherTxn = new Transaction(other.store);
				try {
					// ids are allocated per database, so the first transaction of each shares an id
					expect(otherTxn.id).toBe(txn.id);
					await txn.put('staged', 'in-batch');
					await otherTxn.put('other-staged', 'other-batch');

					const rejected = 'Transaction belongs to a different database';
					expect(() => other.getRange({ transaction: txn })).toThrow(rejected);
					expect(() => other.getKeysCount({ transaction: txn })).toThrow(rejected);
					expect(() => other.getSync('other-staged', { transaction: txn })).toThrow(rejected);
					expect(db.getKeys({ transaction: txn }).asArray).toEqual(['staged']);
					expect(other.getKeys({ transaction: otherTxn }).asArray).toEqual(['other-staged']);
				} finally {
					otherTxn.abort();
					txn.abort();
				}
			}));

		for (const action of ['commit', 'commitSync', 'abort'] as const) {
			it(`should close routed and direct iterators on transaction ${action}`, () =>
				dbRunner(async ({ db }) => {
					await db.put('a', 'committed-a');
					await db.put('b', 'committed-b');
					const txn = new Transaction(db.store);
					await txn.put('c', 'staged-c');

					const routed = db.getRange({ transaction: txn })[Symbol.iterator]();
					const direct = txn.getRange()[Symbol.iterator]();
					const limited = db.getRange({ transaction: txn, limit: 1 })[Symbol.iterator]();
					const thrown = txn.getRange()[Symbol.iterator]();
					expect(routed.next().done).toBe(false);
					expect(direct.next().done).toBe(false);
					expect(limited.next().done).toBe(false);
					expect(thrown.next().done).toBe(false);

					if (action === 'commit') {
						await txn.commit();
					} else if (action === 'commitSync') {
						txn.commitSync();
					} else {
						txn.abort();
					}

					expect(() => routed.next()).toThrow('Iterator not initialized');
					expect(() => direct.next()).toThrow('Iterator not initialized');
					// cleanup after the transaction closed the iterator must not throw: a loop that
					// breaks, reaches its limit, or unwinds an error still calls return()/throw()
					expect(routed.return!().done).toBe(true);
					expect(direct.return!().done).toBe(true);
					expect(limited.next().done).toBe(true);
					expect(() => thrown.throw!(new Error('consumer error'))).toThrow('consumer error');
				}));
		}

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
					expect(error).toBeInstanceOf(Error);
					expect((error as Error).message).toBe('test');
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

		it('should get iterate in reverse with start and end', () =>
			dbRunner(async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e', 'f', 'g']) {
					await db.put(key, `value ${key}`);
				}

				const opts: IteratorOptions = { start: 'f', end: 'b', reverse: true };

				let returnedKeys: Key[] = [];
				for (const { key, value } of db.getRange(opts)) {
					returnedKeys.push(key);
					expect(value).toBe(db.getSync(key));
				}
				expect(['f', 'e', 'd', 'c']).toEqual(returnedKeys);

				opts.exclusiveStart = false;
				opts.inclusiveEnd = false;

				returnedKeys = [];
				for (const { key, value } of db.getRange(opts)) {
					returnedKeys.push(key);
					expect(value).toBe(db.getSync(key));
				}
				expect(['e', 'd', 'c', 'b']).toEqual(returnedKeys);
			}));

		it('should include end key', () =>
			dbRunner(async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange({ end: 'c', inclusiveEnd: true });
				expect(Array.from(iter)).toEqual([
					{ key: 'a', value: 'value a' },
					{
						key: 'b',
						value: 'value b',
					},
					{ key: 'c', value: 'value c' },
				]);
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
				expect(Array.from(iter)).toEqual([
					{ key: 'a' },
					{ key: 'b' },
					{ key: 'c' },
					{ key: 'd' },
					{
						key: 'e',
					},
				]);
			}));

		it('should iterate with advanced ReadOptions (fillCache)', () =>
			dbRunner(async ({ db }) => {
				for (const key of ['a', 'b', 'c']) {
					await db.put(key, `value ${key}`);
				}

				const results: { key: Key; value: unknown }[] = [];
				for (const { key, value } of db.getRange({ fillCache: true })) {
					results.push({ key, value });
				}
				expect(results).toEqual([
					{ key: 'a', value: 'value a' },
					{ key: 'b', value: 'value b' },
					{ key: 'c', value: 'value c' },
				]);
			}));

		it('should error if database is not open', () =>
			dbRunner({ skipOpen: true }, async ({ db }) => {
				expect(() => db.getRange()).toThrow('Database not open');
			}));
	});

	describe('limit option', () => {
		const seed = async (db: any, n = 10) => {
			let last;
			for (let i = 0; i < n; i++) last = db.put(`k-${String(i).padStart(2, '0')}`, i);
			await last;
		};

		it('should yield at most `limit` entries (forward)', () =>
			dbRunner(async ({ db }) => {
				await seed(db);
				const keys = db.getRange({ start: true, limit: 3 }).map((e: any) => e.key).asArray;
				expect(keys).toEqual(['k-00', 'k-01', 'k-02']);
			}));

		it('should yield at most `limit` entries (reverse)', () =>
			dbRunner(async ({ db }) => {
				await seed(db);
				const keys = db
					.getRange({ start: '\xff', reverse: true, limit: 2 })
					.map((e: any) => e.key).asArray;
				expect(keys).toEqual(['k-09', 'k-08']);
			}));

		it('should return the whole range when limit exceeds the entry count', () =>
			dbRunner(async ({ db }) => {
				await seed(db, 4);
				const keys = db.getRange({ start: true, limit: 100 }).map((e: any) => e.key).asArray;
				expect(keys).toEqual(['k-00', 'k-01', 'k-02', 'k-03']);
			}));

		it('should yield nothing for a limit of 0', () =>
			dbRunner(async ({ db }) => {
				await seed(db);
				expect(db.getRange({ start: true, limit: 0 }).asArray).toHaveLength(0);
			}));

		it('should apply the limit to getKeys() (values:false)', () =>
			dbRunner(async ({ db }) => {
				await seed(db);
				expect(db.getKeys({ start: true, limit: 2 }).asArray).toEqual(['k-00', 'k-01']);
			}));

		it('should treat a null limit as no limit', () =>
			dbRunner(async ({ db }) => {
				await seed(db, 4);
				// A nullish limit must not be read as 0 (which would yield nothing).
				const keys = db
					.getRange({ start: true, limit: null as any })
					.map((e: any) => e.key).asArray;
				expect(keys).toEqual(['k-00', 'k-01', 'k-02', 'k-03']);
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

				await db.transaction(async (txn) => {
					const iter = txn.getKeys();
					expect(Array.from(iter)).toEqual(['a', 'b', 'c', 'd', 'e']);
				});
			}));

		it('should get keys only for a transaction and column family', () =>
			dbRunner({ dbOptions: [{ name: 'foo' }] }, async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				await db.transaction(async (txn) => {
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

				await db.transaction(async (txn) => {
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

				await db.transaction(async (txn) => {
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
				const filtered = iter.filter((item) => (item.value as number) % 2 === 0);
				expect(Array.from(filtered)).toEqual([
					{ key: 'a', value: 0 },
					{ key: 'c', value: 2 },
				]);
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
				const flattened = iter.flatMap((item) => [item.value, item.value]);
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
				iter.forEach((item) => values.push(item.value as string));
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
				const mapped = iter.map((item) => {
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
				const mapped = iter
					.map((item) => {
						if (item.value === 'value c') {
							throw new Error('found c');
						}
						return { ...item, value: `${item.value}!` };
					})
					.mapError((error) => {
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
				expect(Array.from(sliced)).toEqual([
					{ key: 'b', value: 'value b' },
					{
						key: 'c',
						value: 'value c',
					},
				]);
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
				const results = iter
					.map((item, index) => {
						return { ...item, value: `${item.value}${index % 2 === 0 ? '!' : ''}` };
					})
					.filter((item) => item.value.endsWith('!'));

				expect(Array.from(results)).toEqual([
					{ key: 'a', value: 'value a!' },
					{
						key: 'c',
						value: 'value c!',
					},
					{ key: 'e', value: 'value e!' },
				]);
			}));

		it('should map error', () =>
			dbRunner(async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				const iter = db.getRange();
				const results = iter
					.map((item, index) => {
						if (index === 1) {
							throw new Error('test');
						}
						return { ...item, value: `${item.value}${index % 2 === 0 ? '!' : ''}` };
					})
					.mapError((error) => error);

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
				const results = iter
					.map((item) => {
						return { ...item, value: `${item.value}!` };
					})
					.mapError((error) => error);

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
