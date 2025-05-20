import { assert, describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';
import type { Transaction } from '../src/transaction.js';

const testOptions = [
	{
		name: 'optimistic',
		options: {},
	},
	{
		name: 'pessimistic',
		options: { pessimistic: true },
	}
];

for (const { name, options } of testOptions) {
	describe(`transaction() (${name})`, () => {
		it(`${name} async should error if callback is not a function`, async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, options);
				await expect(db.transaction('foo' as any)).rejects.toThrow(
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
				db = await RocksDatabase.open(dbPath, options);
				await db.put('foo', 'bar');
				await db.transaction(async (txn: Transaction) => {
					const value = await txn.get('foo');
					expect(value).toBe('bar');
				});
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it(`${name} async should set a value`, async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, options);
				await db.transaction(async (txn: Transaction) => {
					await txn.put('foo', 'bar2');
				});
				const value = await db.get('foo');
				expect(value).toBe('bar2');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it(`${name} async should remove a value`, async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, options);
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
				db = await RocksDatabase.open(dbPath, options);
				await db.put('foo', 'bar');

				await expect(db.transaction(async (txn: Transaction) => {
					await txn.put('foo', 'bar2');
					throw new Error('test');
				})).rejects.toThrow('test');

				const value = await db.get('foo');
				expect(value).toBe('bar');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it(`${name} async should treat transaction as a snapshot`, async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, options);
				await db.put('foo', 'bar1');

				setTimeout(() => db?.put('foo', 'bar2'));

				try {
					await db.transaction(async (txn: Transaction) => {
						const before = await txn.get('foo');

						await new Promise((resolve) => setTimeout(resolve, 100));
						const after = await txn.get('foo');

						expect(before).toBe(after);

						await txn.put('foo', 'bar3');
					});
				} catch (error: unknown | Error & { code: string }) {
					expect(error).toBeInstanceOf(Error);
					if (error instanceof Error) {
						expect(error.message).toBe(`Transaction ${
							options.pessimistic ? 'put' : 'commit'
						} failed: Resource busy`);
						if ('code' in error) {
							expect(error.code).toBe('ERR_BUSY');
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

		it(`${name} async should allow transactions across column families`, async () => {
			let db: RocksDatabase | null = null;
			let db2: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, options);
				db2 = await RocksDatabase.open(dbPath, { ...options, name: 'foo' });

				await db.put('foo', 'bar');
				await db2.put('foo2', 'baz');

				await db.transaction(async (txn: Transaction) => {
					assert(db);
					assert(db2);
					await db.put('foo', 'bar2', { transaction: txn });
					await db2.put('foo2', 'baz2', { transaction: txn });
				});

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
				db = await RocksDatabase.open(dbPath, options);
				db2 = await RocksDatabase.open(dbPath2, options);

				await Promise.all([
					db.transaction(async (txn: Transaction) => {
						await new Promise((resolve) => setTimeout(resolve, 100));
						await db?.put('foo', 'bar2', { transaction: txn });
					}),
					db2.transaction(async (txn: Transaction) => {
						await new Promise((resolve) => setTimeout(resolve, 100));
						await db2?.put('foo2', 'baz3', { transaction: txn });
					}),
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
	});

	describe(`transactionSync() (${name})`, () => {
		it(`${name} sync should error if callback is not a function`, async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, options);
				expect(() => db!.transactionSync('foo' as any))
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
				db = await RocksDatabase.open(dbPath, options);
				await db.put('foo', 'bar');
				db.transactionSync((txn: Transaction) => {
					const value = txn.getSync('foo');
					expect(value).toBe('bar');
				});
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});

		it(`${name} sync should set a value`, async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, options);

				db.transactionSync((txn: Transaction) => {
					txn.putSync('foo', 'bar2');
				});

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
				db = await RocksDatabase.open(dbPath, options);
				await db.put('foo', 'bar');

				db.transactionSync((txn: Transaction) => {
					txn.removeSync('foo');
				});

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
				db = await RocksDatabase.open(dbPath, options);
				await db.put('foo', 'bar');

				expect(() => db!.transactionSync((txn: Transaction) => {
					txn.putSync('foo', 'bar2');
					throw new Error('test');
				})).toThrow('test');

				const value = await db.get('foo');
				expect(value).toBe('bar');
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
				db = await RocksDatabase.open(dbPath, options);
				db2 = await RocksDatabase.open(dbPath, { ...options, name: 'foo' });

				await db.put('foo', 'bar');
				await db2.put('foo2', 'baz');

				db.transactionSync((txn: Transaction) => {
					assert(db);
					assert(db2);
					db.putSync('foo', 'bar2', { transaction: txn });
					db2.putSync('foo2', 'baz2', { transaction: txn });
				});

				expect(await db.get('foo')).toBe('bar2');
				expect(await db2.get('foo2')).toBe('baz2');
			} finally {
				db?.close();
				db2?.close();
				await rimraf(dbPath);
			}
		});
	});

	describe(`Error handling (${name})`, () => {
		it('should error if transaction is invalid', async () => {
			let db: RocksDatabase | null = null;
			const dbPath = generateDBPath();

			try {
				db = await RocksDatabase.open(dbPath, options);
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
				db = await RocksDatabase.open(dbPath, options);
				await expect(db.get('foo', { transaction: { id: 9926 } as any })).rejects.toThrow('Transaction not found');
			} finally {
				db?.close();
				await rimraf(dbPath);
			}
		});
	});
}
