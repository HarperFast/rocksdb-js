import { assert, describe, expect, it } from 'vitest';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';
import type { Transaction } from '../src/transaction.js';

describe('Transactions (pessimistic)', () => {
	it('should error if callback is not a function', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = await RocksDatabase.open(dbPath, { pessimistic: true });
			await expect(db.transaction('foo' as any)).rejects.toThrow(new TypeError('Callback must be a function'));
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});

	it('should get a value', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = await RocksDatabase.open(dbPath, { pessimistic: true });
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

	it('should set a value', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = await RocksDatabase.open(dbPath, { pessimistic: true });

			await db.transaction(async (txn: Transaction) => {
				await txn.put('foo', 'bar');
			});

			const value = await db.get('foo');
			expect(value).toBe('bar');
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});

	it('should remove a value', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = await RocksDatabase.open(dbPath, { pessimistic: true });

			await db.put('foo', 'bar');
			const value = await db.get('foo');
			expect(value).toBe('bar');

			await db.transaction(async (txn: Transaction) => {
				await txn.remove('foo');
			});

			const value2 = await db.get('foo');
			expect(value2).toBeUndefined();
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});

	it('should rollback on error', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = await RocksDatabase.open(dbPath, { pessimistic: true });
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

	it('should treat transaction as a snapshot', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = await RocksDatabase.open(dbPath, { pessimistic: true });
			await db.put('foo', 'bar1');

			setTimeout(() => {
				db?.put('foo', 'bar2');
			}, 50);

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
					expect(error.message).toBe('Transaction put failed: Resource busy');
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

	it('should allow transactions across column families', async () => {
		let db: RocksDatabase | null = null;
		let db2: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = await RocksDatabase.open(dbPath, { pessimistic: true });
			db2 = await RocksDatabase.open(dbPath, { name: 'foo', pessimistic: true });

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

	it('should allow multiple transactions to run in parallel', async () => {
		let db: RocksDatabase | null = null;
		let db2: RocksDatabase | null = null;
		const dbPath = generateDBPath();
		const dbPath2 = generateDBPath();

		try {
			db = await RocksDatabase.open(dbPath, { pessimistic: true });
			db2 = await RocksDatabase.open(dbPath2, { pessimistic: true });

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

	it('should error if transaction is invalid', async () => {
		let db: RocksDatabase | null = null;
		const dbPath = generateDBPath();

		try {
			db = await RocksDatabase.open(dbPath, { pessimistic: true });
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
			db = await RocksDatabase.open(dbPath, { pessimistic: true });
			await expect(db.get('foo', { transaction: { id: 9926 } as any })).rejects.toThrow('Transaction not found');
		} finally {
			db?.close();
			await rimraf(dbPath);
		}
	});
});
