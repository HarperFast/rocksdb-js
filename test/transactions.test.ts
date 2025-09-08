import { assert, describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';
import type { Transaction } from '../src/transaction.js';
import { withResolvers } from '../src/util.js';

const testOptions = [
	{
		name: 'optimistic'
	},
	{
		name: 'pessimistic',
		options: { pessimistic: true }
	},
		{
		name: 'optimistic (disableSnapshot)',
		txnOptions: { disableSnapshot: true },
	},
	{
		name: 'pessimistic (disableSnapshot)',
		options: { pessimistic: true },
		txnOptions: { disableSnapshot: true },
	}
];

for (const { name, options, txnOptions } of testOptions) {
	describe(`transaction() (${name})`, () => {
		it(`${name} async should error if callback is not a function`, async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath, options);
				await expect(db.transaction('foo' as any, txnOptions)).rejects.toThrow(
					new TypeError('Callback must be a function')
				);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it(`${name} async should get a value`, async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath, options);
				await db.put('foo', 'bar');
				await db.transaction(async (txn: Transaction) => {
					const value = await txn.get('foo');
					expect(value).toBe('bar');
				}, txnOptions);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it(`${name} async should set a value`, async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			const afterCommit = withResolvers();
			const beforeCommit = withResolvers();
			const beginTransaction = withResolvers();
			const committed = withResolvers();

			try {
				db = RocksDatabase.open(dbPath, options)
					.on('aftercommit', (result) => afterCommit.resolve(result))
					.on('beforecommit', () => beforeCommit.resolve())
					.on('begin-transaction', () => beginTransaction.resolve())
					.on('committed', () => committed.resolve());

				await db.transaction(async (txn: Transaction) => {
					await txn.put('foo', 'bar2');
				}, txnOptions);
				const value = await db.get('foo');
				expect(value).toBe('bar2');

				await expect(Promise.all([
					beginTransaction.promise,
					beforeCommit.promise,
					afterCommit.promise,
					committed.promise,
				])).resolves.toEqual([
					undefined,
					undefined,
					{
						next: null,
						last: null,
						txnId: expect.any(Number)
					},
					undefined,
				]);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it(`${name} async should remove a value`, async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath, options);
				await db.put('foo', 'bar');

				await db.transaction(async (txn: Transaction) => {
					await txn.remove('foo');
				});

				const value = await db.get('foo');
				expect(value).toBeUndefined();
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it(`${name} async should rollback on error`, async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath, options);
				await db.put('foo', 'bar');

				await expect(db.transaction(async (txn: Transaction) => {
					await txn.put('foo', 'bar2');
					throw new Error('test');
				}, txnOptions)).rejects.toThrow('test');

				const value = await db.get('foo');
				expect(value).toBe('bar');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		if (txnOptions?.disableSnapshot) {
			it(`${name} async should throw error if snapshot is disabled`, async () => {
				let db: RocksDatabase | null = null;
				const dbPath = generateDBPath();

				try {
					db = RocksDatabase.open(dbPath, options);
					await db.put('foo', 'bar1');

					setTimeout(() => db?.put('foo', 'bar2'));

					try {
						await db.transaction(async (txn: Transaction) => {
							const before = await txn.get('foo');

							// let the first timeout set `bar2`
							await new Promise((resolve) => setTimeout(resolve, 100));

							// since there's no snapshot, the value should be the latest
							const after = await txn.get('foo');
							expect(before).not.toBe(after);
							expect(after).toBe('bar2');

							await txn.put('foo', 'bar3');
						}, txnOptions);
					} catch (error: unknown | Error & { code: string }) {
						expect(error).toBeInstanceOf(Error);
						if (error instanceof Error) {
							expect(error.message).toBe(`Transaction ${
								options?.pessimistic ? 'put' : 'commit'
							} failed: Resource busy`);
							if ('code' in error) {
								expect(error.code).toBe('ERR_BUSY');
							}
						}
					}

					const value = await db.get('foo');
					expect(value).toBe('bar3');
				} finally {
					db?.close();
					await rimraf(dbPath);
				}
			});
		} else {
			it(`${name} async should treat transaction as a snapshot`, async () => {
				let db: RocksDatabase | null = null;
				const dbPath = generateDBPath();

				try {
					db = RocksDatabase.open(dbPath, options);
					await db.put('foo', 'bar1');

					setTimeout(() => db?.put('foo', 'bar2'));

					try {
						await db.transaction(async (txn: Transaction) => {
							const before = await txn.get('foo');

							await new Promise((resolve) => setTimeout(resolve, 100));
							const after = await txn.get('foo');

							expect(before).toBe(after);

							await txn.put('foo', 'bar3');
						}, txnOptions);
					} catch (error: unknown | Error & { code: string }) {
						expect(error).toBeInstanceOf(Error);
						if (error instanceof Error) {
							if (options?.pessimistic) {
								expect(error.message).toBe('Transaction put failed: Resource busy');
								if ('code' in error) {
									expect(error.code).toBe('ERR_BUSY');
								}
							} else {
								expect(error.message).toBe('Transaction has already been committed');
								if ('code' in error) {
									expect(error.code).toBe('ERR_ALREADY_COMMITTED');
								}
							}
						}
					}

					const value = await db.get('foo');
					expect(value).toBe('bar2');
				} finally {
					db?.close();
					await rimraf(dbPath);
				}
			});
		}

		it(`${name} async should allow transactions across column families`, async () => {
			let db: RocksDatabase | null = null;
			let db2: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath, options);
				db2 = RocksDatabase.open(dbPath, { ...options, name: 'foo' });

				await db.put('foo', 'bar');
				await db2.put('foo2', 'baz');

				await db.transaction(async (txn: Transaction) => {
					assert(db);
					assert(db2);
					await db.put('foo', 'bar2', { transaction: txn });
					await db2.put('foo2', 'baz2', { transaction: txn });
				}, txnOptions);

				expect(await db.get('foo')).toBe('bar2');
				expect(await db2.get('foo2')).toBe('baz2');
			} finally {
				db?.close();
				db2?.close();
				await rimraf(dbPath);
			}
		});

		it(`${name} async should allow multiple transactions to run in parallel`, async () => {
			let db: RocksDatabase | null = null;
			let db2: RocksDatabase | null = null;
			const dbPath = generateDBPath();
			const dbPath2 = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath, options);
				db2 = RocksDatabase.open(dbPath2, options);

				await Promise.all([
					db.transaction(async (txn: Transaction) => {
						await new Promise((resolve) => setTimeout(resolve, 100));
						await db?.put('foo', 'bar2', { transaction: txn });
					}, txnOptions),
					db2.transaction(async (txn: Transaction) => {
						await new Promise((resolve) => setTimeout(resolve, 100));
						await db2?.put('foo2', 'baz3', { transaction: txn });
					}, txnOptions),
				]);

				expect(await db.get('foo')).toBe('bar2');
				expect(await db2.get('foo2')).toBe('baz3');
			} finally {
				db?.close();
				db2?.close();
				await rimraf(dbPath);
				await rimraf(dbPath2);
			}
		});

		it(`${name} async should close transaction and iterator if getRange throws`, async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath, options);
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				let txn: Transaction;

				await expect(db.transaction(async (t: Transaction) => {
					txn = t;
					const iter = txn.getRange();
					const results = iter
						.map(_item => {
							throw new Error('test');
						});
					const iterator = results[Symbol.asyncIterator]();
					await iterator.next();
				}, txnOptions)).rejects.toThrow('test');

				expect(() => db!.getSync('a', { transaction: txn })).toThrow('Transaction not found');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});

	describe(`transactionSync() (${name})`, () => {
		it(`${name} sync should error if callback is not a function`, async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath, options);
				expect(() => db!.transactionSync('foo' as any, txnOptions))
					.toThrow(new TypeError('Callback must be a function'));
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it(`${name} sync should get a value`, async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath, options);
				await db.put('foo', 'bar');
				db.transactionSync((txn: Transaction) => {
					const value = txn.getSync('foo');
					expect(value).toBe('bar');
				}, txnOptions);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it(`${name} sync should set a value`, async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath, options);

				db.transactionSync((txn: Transaction) => {
					txn.putSync('foo', 'bar2');
				}, txnOptions);

				const value = await db.get('foo');
				expect(value).toBe('bar2');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it(`${name} sync should remove a value`, async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath, options);
				await db.put('foo', 'bar');

				db.transactionSync((txn: Transaction) => {
					txn.removeSync('foo');
				}, txnOptions);

				const value = await db.get('foo');
				expect(value).toBeUndefined();
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it(`${name} sync should rollback on error`, async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath, options);
				await db.put('foo', 'bar');

				expect(() => db!.transactionSync((txn: Transaction) => {
					txn.putSync('foo', 'bar2');
					throw new Error('test');
				}, txnOptions)).toThrow('test');

				const value = await db.get('foo');
				expect(value).toBe('bar');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it(`${name} sync should abort once`, async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath, options);
				await db.put('foo', 'bar');

				db!.transactionSync((txn: Transaction) => {
					txn.abort();
				}, txnOptions);
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it(`${name} sync should allow transactions across column families`, async () => {
			let db: RocksDatabase | null = null;
			let db2: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath, options);
				db2 = RocksDatabase.open(dbPath, { ...options, name: 'foo' });

				await db.put('foo', 'bar');
				await db2.put('foo2', 'baz');

				db.transactionSync((txn: Transaction) => {
					assert(db);
					assert(db2);
					db.putSync('foo', 'bar2', { transaction: txn });
					db2.putSync('foo2', 'baz2', { transaction: txn });
				}, txnOptions);

				expect(await db.get('foo')).toBe('bar2');
				expect(await db2.get('foo2')).toBe('baz2');
			} finally {
				db?.close();
				db2?.close();
				await rimraf(dbPath);
			}
		});

		it(`${name} sync should close transaction and iterator if getRange throws`, async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath, options);
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					db.putSync(key, `value ${key}`);
				}

				let txn: Transaction;

				expect(() => db!.transactionSync((t: Transaction) => {
					txn = t;
					const iter = txn.getRange();
					const results = iter
						.map(_item => {
							throw new Error('test');
						});
					const iterator = results[Symbol.iterator]();
					iterator.next();
				}, txnOptions)).toThrow('test');

				expect(() => db!.getSync('a', { transaction: txn })).toThrow('Transaction not found');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});

	describe(`Error handling (${name})`, () => {
		it('should error if transaction is invalid', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath, options);
				await expect(db.get('foo', { transaction: 'bar' as any })).rejects.toThrow('Invalid transaction');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it('should error if transaction is not found', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = RocksDatabase.open(dbPath, options);
				await expect(db.get('foo', { transaction: { id: 9926 } as any })).rejects.toThrow('Transaction not found');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});
}
