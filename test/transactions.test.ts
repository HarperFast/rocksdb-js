import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { RocksDatabase } from '../src/index.js';
import { rimraf } from 'rimraf';
import type { Transaction } from '../src/transaction.js';

describe('Transactions', () => {
	let db: RocksDatabase | null = null;
	
	beforeEach(() => rimraf('/tmp/testdb'));

	afterEach(() => {
		if (db) {
			db.close();
			db = null;
		}
		return rimraf('/tmp/testdb');
	});

	it.only('should get a value', async () => {
		db = await RocksDatabase.open('/tmp/testdb');
		await db.put('foo', 'bar');
		await db.transaction(async (tx: Transaction) => {
			const value = await tx.get('foo');
			expect(value).toBe('bar');
		});
	});

	it('should set a value', async () => {
		db = await RocksDatabase.open('/tmp/testdb');
		await db.transaction(async (tx: Transaction) => {
			await tx.put('foo', 'bar');
		});
		const value = await db.get('foo');
		expect(value).toBe('bar');
	});

	it('should remove a value', async () => {
		db = await RocksDatabase.open('/tmp/testdb');
		await db.put('foo', 'bar');
		const value = await db.get('foo');
		expect(value).toBe('bar');
		await db.transaction(async (tx: Transaction) => {
			await tx.remove('foo');
		});
		const value2 = await db.get('foo');
		expect(value2).toBeUndefined();
	});

	it('should rollback on error', async () => {
		db = await RocksDatabase.open('/tmp/testdb');
		await db.put('foo', 'bar');
		await db.transaction(async (tx: Transaction) => {
			await tx.put('foo', 'bar2');
			throw new Error('test');
		});
		const value = await db.get('foo');
		expect(value).toBe('bar');
	});

	it('should treat transaction as a snapshot', async () => {
		db = await RocksDatabase.open('/tmp/testdb');
		let i = 0;
		let interval = setInterval(() => {
			i++;
			db?.put('foo', i.toString());
		}, 100);

		try {
			await db.transaction(async (tx: Transaction) => {
				await new Promise((resolve) => setTimeout(resolve, 250));
				const before = await tx.get('foo');
				await new Promise((resolve) => setTimeout(resolve, 250));
				const after = await tx.get('foo');
				expect(before).toBe(after);

				clearInterval(interval);
				await tx.put('foo', 'bar');
			});

			const value = await db.get('foo');
			expect(value).toBe('bar');
		} finally {
			clearInterval(interval);
		}
	});

	it('should error if callback is not a function', async () => {
		db = await RocksDatabase.open('/tmp/testdb');
		await expect(db.transaction('foo' as any)).rejects.toThrow(new TypeError('Callback must be a function'));
	});
});