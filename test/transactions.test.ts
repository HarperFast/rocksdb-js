import { describe, expect, it } from 'vitest';
import type { Transaction } from '../src/transaction.js';
import { withResolvers } from '../src/util.js';
import { dbRunner, generateDBPath } from './lib/util.js';

const testOptions = [
	{ name: 'optimistic' },
	{ name: 'pessimistic', options: { pessimistic: true } },
	{ name: 'optimistic (disableSnapshot)', txnOptions: { disableSnapshot: true } },
	{
		name: 'pessimistic (disableSnapshot)',
		options: { pessimistic: true },
		txnOptions: { disableSnapshot: true },
	},
];

for (const { name, options, txnOptions } of testOptions) {
	describe(`transaction() (${name})`, () => {
		it(`${name} async should error if callback is not a function`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				await expect(db.transaction('foo' as any, txnOptions)).rejects.toThrow(
					new TypeError('Callback must be a function')
				);
			}));

		it(`${name} async should get a value`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				await db.put('foo', 'bar');
				await db.transaction(async (txn: Transaction) => {
					const value = await txn.get('foo');
					expect(value).toBe('bar');
				}, txnOptions);
			}));

		it(`${name} async should set a value`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				const afterCommit = withResolvers<void>();
				const beforeCommit = withResolvers<void>();
				const beginTransaction = withResolvers<void>();
				const committed = withResolvers<void>();

				db.on('aftercommit', (result) => afterCommit.resolve(result));
				db.on('beforecommit', () => beforeCommit.resolve());
				db.on('begin-transaction', () => beginTransaction.resolve());
				db.on('committed', () => committed.resolve());

				await db.transaction(async (txn: Transaction) => {
					await txn.put('foo', 'bar2');
				}, txnOptions);
				const value = await db.get('foo');
				expect(value).toBe('bar2');

				await expect(
					Promise.all([
						beginTransaction.promise,
						beforeCommit.promise,
						afterCommit.promise,
						committed.promise,
					])
				).resolves.toEqual([undefined, undefined, {
					next: null,
					last: null,
					txnId: expect.any(Number),
				}, undefined]);
			}));

		it(`${name} async should remove a value`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				await db.put('foo', 'bar');

				await db.transaction(async (txn: Transaction) => {
					await txn.remove('foo');
				});

				const value = await db.get('foo');
				expect(value).toBeUndefined();
			}));

		it(`${name} async should rollback on error`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				await db.put('foo', 'bar');

				await expect(db.transaction(async (txn: Transaction) => {
					await txn.put('foo', 'bar2');
					throw new Error('test');
				}, txnOptions)).rejects.toThrow('test');

				const value = await db.get('foo');
				expect(value).toBe('bar');
			}));

		if (txnOptions?.disableSnapshot) {
			it(`${name} async should throw error if snapshot is disabled`, () =>
				dbRunner({ dbOptions: [options] }, async ({ db }) => {
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
							expect(error.message).toBe(
								`Transaction ${options?.pessimistic ? 'put' : 'commit'} failed: Resource busy`
							);
							if ('code' in error) {
								expect(error.code).toBe('ERR_BUSY');
							}
						}
					}

					const value = await db.get('foo');
					expect(value).toBe('bar3');
				}));
		} else {
			it(`${name} async should treat transaction as a snapshot`, () =>
				dbRunner({ dbOptions: [options] }, async ({ db }) => {
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
							expect(error.message).toBe(
								`Transaction ${options?.pessimistic ? 'put' : 'commit'} failed: Resource busy`
							);
							if ('code' in error) {
								expect(error.code).toBe('ERR_BUSY');
							}
						}
					}

					const value = await db.get('foo');
					expect(value).toBe('bar2');
				}));
		}

		it(`${name} async should allow transactions across column families`, () =>
			dbRunner(
				{ dbOptions: [options, { ...options, name: 'foo' }] },
				async ({ db }, { db: db2 }) => {
					await db.put('foo', 'bar');
					await db2.put('foo2', 'baz');

					await db.transaction(async (txn: Transaction) => {
						await db.put('foo', 'bar2', { transaction: txn });
						await db2.put('foo2', 'baz2', { transaction: txn });
					}, txnOptions);

					expect(await db.get('foo')).toBe('bar2');
					expect(await db2.get('foo2')).toBe('baz2');
				}
			));

		it(`${name} async should allow multiple transactions to run in parallel`, () =>
			dbRunner(
				{ dbOptions: [options, { ...options, path: generateDBPath() }] },
				async ({ db }, { db: db2 }) => {
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
				}
			));

		it(`${name} async should close transaction and iterator if getRange throws`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					await db.put(key, `value ${key}`);
				}

				let txn: Transaction;

				await expect(db.transaction(async (t: Transaction) => {
					txn = t;
					const iter = txn.getRange();
					const results = iter.map(_item => {
						throw new Error('test');
					});
					const iterator = results[Symbol.asyncIterator]();
					await iterator.next();
				}, txnOptions)).rejects.toThrow('test');

				expect(() => db.getSync('a', { transaction: txn })).toThrow('Transaction not found');
			}));

		it(`${name} async should commit in the callback`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				await db.transaction(async (txn: Transaction) => {
					await txn.put('foo', 'bar');
					await txn.commit();
				});
				await expect(db.get('foo')).toBe('bar');
			}));

		it(`${name} async should commit twice in the callback`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				await db.transaction(async (txn: Transaction) => {
					await txn.put('foo', 'bar');
					await txn.commit();
					await txn.commit();
				});
				await expect(db.get('foo')).toBe('bar');
			}));

		it(`${name} async should commit, then throw in the callback`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				await expect(db.transaction(async (txn: Transaction) => {
					await txn.put('foo', 'bar');
					await txn.commit();
					throw new Error('test');
				})).rejects.toThrow('test');
				await expect(db.get('foo')).toBe('bar');
			}));

		it(`${name} async should commit, then error when aborting in the callback`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				await expect(db.transaction(async (txn: Transaction) => {
					await txn.put('foo', 'bar');
					await txn.commit();
					txn.abort();
				})).rejects.toThrow('Transaction has already been committed');
				await expect(db.get('foo')).toBe('bar');
			}));

		it(`${name} async should commit, then abort in the callback`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				await expect(db.transaction(async (txn: Transaction) => {
					await txn.put('foo', 'bar');
					await txn.commit();
					txn.abort();
				})).rejects.toThrow('Transaction has already been committed');
				await expect(db.get('foo')).toBe('bar');
			}));

		it(`${name} async should abort in the callback`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				await db.transaction(async (txn: Transaction) => {
					await txn.put('foo', 'bar');
					txn.abort();
				});
				await expect(db.get('foo')).toBeUndefined();
			}));

		it(`${name} async should abort twice in the callback`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				await db.transaction(async (txn: Transaction) => {
					await txn.put('foo', 'bar');
					txn.abort();
					txn.abort();
				});
				await expect(db.get('foo')).toBeUndefined();
			}));

		it(`${name} async should abort, then throw in the callback`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				await expect(db.transaction(async (txn: Transaction) => {
					await txn.put('foo', 'bar');
					txn.abort();
					throw new Error('test');
				})).rejects.toThrow('test');
				await expect(db.get('foo')).toBeUndefined();
			}));

		it(`${name} async should abort, then error when aborting in the callback`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				await expect(db.transaction(async (txn: Transaction) => {
					await txn.put('foo', 'bar');
					txn.abort();
					await txn.commit();
				})).rejects.toThrow('Transaction has already been aborted');
				await expect(db.get('foo')).toBeUndefined();
			}));

		it(`${name} async should get and set timestamp`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				const start = Date.now() - 1000;
				await db.transaction(async (txn: Transaction) => {
					let ts = txn.getTimestamp();
					expect(ts).toBeGreaterThanOrEqual(start);
					expect(ts).toBeLessThanOrEqual(start + 2000);

					const newTs = Date.now();
					txn.setTimestamp(newTs);
					ts = txn.getTimestamp();
					expect(ts).toBeCloseTo(newTs, 6);

					txn.setTimestamp();
					ts = txn.getTimestamp();
					expect(ts).toBeGreaterThanOrEqual(newTs - 1000);

					expect(() => txn.setTimestamp(-1)).toThrow('Invalid timestamp, expected positive number');
					expect(() => txn.setTimestamp('foo' as any)).toThrow(
						'Invalid timestamp, expected positive number'
					);
					expect(() => txn.setTimestamp(null as any)).toThrow(
						'Invalid timestamp, expected positive number'
					);
					expect(() => txn.setTimestamp(new Date() as any)).toThrow(
						'Invalid timestamp, expected positive number'
					);
					expect(() => txn.setTimestamp(new Date(0) as any)).toThrow(
						'Invalid timestamp, expected positive number'
					);
					expect(() => txn.setTimestamp(new Date(1000) as any)).toThrow(
						'Invalid timestamp, expected positive number'
					);
				});
			}));
	});

	describe(`transactionSync() (${name})`, () => {
		it(`${name} sync should error if callback is not a function`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				expect(() => db.transactionSync('foo' as any, txnOptions)).toThrow(
					new TypeError('Callback must be a function')
				);
			}));

		it(`${name} sync should get a value`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				await db.put('foo', 'bar');
				db.transactionSync((txn: Transaction) => {
					const value = txn.getSync('foo');
					expect(value).toBe('bar');
				}, txnOptions);
			}));

		it(`${name} sync should set a value`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				db.transactionSync((txn: Transaction) => {
					txn.putSync('foo', 'bar2');
				}, txnOptions);

				const value = await db.get('foo');
				expect(value).toBe('bar2');
			}));

		it(`${name} sync should remove a value`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				await db.put('foo', 'bar');

				db.transactionSync((txn: Transaction) => {
					txn.removeSync('foo');
				}, txnOptions);

				const value = await db.get('foo');
				expect(value).toBeUndefined();
			}));

		it(`${name} sync should rollback on error`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				await db.put('foo', 'bar');

				expect(() =>
					db.transactionSync((txn: Transaction) => {
						txn.putSync('foo', 'bar2');
						throw new Error('test');
					}, txnOptions)
				).toThrow('test');

				const value = await db.get('foo');
				expect(value).toBe('bar');
			}));

		it(`${name} sync should abort once`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				await db.put('foo', 'bar');

				db.transactionSync((txn: Transaction) => {
					txn.abort();
				}, txnOptions);
			}));

		it(`${name} sync should allow transactions across column families`, () =>
			dbRunner(
				{ dbOptions: [options, { ...options, name: 'foo' }] },
				async ({ db }, { db: db2 }) => {
					await db.put('foo', 'bar');
					await db2.put('foo2', 'baz');

					db.transactionSync((txn: Transaction) => {
						db.putSync('foo', 'bar2', { transaction: txn });
						db2.putSync('foo2', 'baz2', { transaction: txn });
					}, txnOptions);

					expect(await db.get('foo')).toBe('bar2');
					expect(await db2.get('foo2')).toBe('baz2');
				}
			));

		it(`${name} sync should close transaction and iterator if getRange throws`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				for (const key of ['a', 'b', 'c', 'd', 'e']) {
					db.putSync(key, `value ${key}`);
				}

				let txn: Transaction;

				expect(() =>
					db.transactionSync((t: Transaction) => {
						txn = t;
						const iter = txn.getRange();
						const results = iter.map(_item => {
							throw new Error('test');
						});
						const iterator = results[Symbol.iterator]();
						iterator.next();
					}, txnOptions)
				).toThrow('test');

				expect(() => db.getSync('a', { transaction: txn })).toThrow('Transaction not found');
			}));

		it(`${name} sync should commit in the callback`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				db.transactionSync((txn: Transaction) => {
					txn.putSync('foo', 'bar');
					txn.commitSync();
				});
				await expect(db.get('foo')).toBe('bar');
			}));

		it(`${name} sync should commit twice in the callback`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				db.transactionSync((txn: Transaction) => {
					txn.putSync('foo', 'bar');
					txn.commitSync();
					txn.commitSync();
				});
				await expect(db.get('foo')).toBe('bar');
			}));

		it(`${name} sync should commit, then throw in the callback`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				expect(() =>
					db.transactionSync((txn: Transaction) => {
						txn.putSync('foo', 'bar');
						txn.commitSync();
						throw new Error('test');
					})
				).toThrow('test');
				expect(db.getSync('foo')).toBe('bar');
			}));

		it(`${name} sync should commit, then error when aborting in the callback`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				expect(() =>
					db.transactionSync((txn: Transaction) => {
						txn.putSync('foo', 'bar');
						txn.commitSync();
						txn.abort();
					})
				).toThrow('Transaction has already been committed');
				expect(db.getSync('foo')).toBe('bar');
			}));

		it(`${name} sync should commit, then abort in the callback`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				expect(() =>
					db.transactionSync((txn: Transaction) => {
						txn.putSync('foo', 'bar');
						txn.commitSync();
						txn.abort();
					})
				).toThrow('Transaction has already been committed');
				expect(db.getSync('foo')).toBe('bar');
			}));

		it(`${name} sync should abort in the callback`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				db.transactionSync((txn: Transaction) => {
					txn.putSync('foo', 'bar');
					txn.abort();
				});
				expect(db.getSync('foo')).toBeUndefined();
			}));

		it(`${name} sync should abort twice in the callback`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				db.transactionSync((txn: Transaction) => {
					txn.putSync('foo', 'bar');
					txn.abort();
					txn.abort();
				});
				expect(db.getSync('foo')).toBeUndefined();
			}));

		it(`${name} sync should abort, then throw in the callback`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				expect(() =>
					db.transactionSync((txn: Transaction) => {
						txn.putSync('foo', 'bar');
						txn.abort();
						throw new Error('test');
					})
				).toThrow('test');
				expect(db.getSync('foo')).toBeUndefined();
			}));

		it(`${name} sync should abort, then error when aborting in the callback`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				expect(() =>
					db.transactionSync((txn: Transaction) => {
						txn.putSync('foo', 'bar');
						txn.abort();
						txn.commitSync();
					})
				).toThrow('Transaction has already been aborted');
				expect(db.getSync('foo')).toBeUndefined();
			}));

		it(`${name} sync should get and set timestamp`, () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				const start = Date.now() - 1000;
				db.transactionSync((txn: Transaction) => {
					let ts = txn.getTimestamp();
					expect(ts).toBeGreaterThanOrEqual(start);
					expect(ts).toBeLessThanOrEqual(start + 2000);

					const newTs = Date.now();
					txn.setTimestamp(newTs);
					ts = txn.getTimestamp();
					expect(ts).toBeCloseTo(newTs, 6);

					txn.setTimestamp();
					ts = txn.getTimestamp();
					expect(ts).toBeGreaterThanOrEqual(newTs - 1000);

					expect(() => txn.setTimestamp(-1)).toThrow('Invalid timestamp, expected positive number');
					expect(() => txn.setTimestamp('foo' as any)).toThrow(
						'Invalid timestamp, expected positive number'
					);
					expect(() => txn.setTimestamp(null as any)).toThrow(
						'Invalid timestamp, expected positive number'
					);
					expect(() => txn.setTimestamp(new Date() as any)).toThrow(
						'Invalid timestamp, expected positive number'
					);
					expect(() => txn.setTimestamp(new Date(0) as any)).toThrow(
						'Invalid timestamp, expected positive number'
					);
					expect(() => txn.setTimestamp(new Date(1000) as any)).toThrow(
						'Invalid timestamp, expected positive number'
					);
				});
			}));
	});

	describe(`Error handling (${name})`, () => {
		it('should error if transaction is invalid', () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				await expect(db.get('foo', { transaction: 'bar' as any })).rejects.toThrow(
					'Invalid transaction'
				);
			}));

		it('should error if transaction is not found', () =>
			dbRunner({ dbOptions: [options] }, async ({ db }) => {
				await expect(db.get('foo', { transaction: { id: 9926 } as any })).rejects.toThrow(
					'Transaction not found'
				);
			}));
	});
}
