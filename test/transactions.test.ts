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

	it('should get a value', async () => {
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
		await expect(db.transaction(async (tx: Transaction) => {
			await tx.put('foo', 'bar2');
			throw new Error('test');
		})).rejects.toThrow('test');
		const value = await db.get('foo');
		expect(value).toBe('bar');
	});

	it.only('should treat transaction as a snapshot', async () => {
		db = await RocksDatabase.open('/tmp/testdb');
		console.log('putting bar1');
		db.put('foo', 'bar1');

		setTimeout(() => {
			console.log('putting bar2');
			db?.put('foo', 'bar2');
			console.log('put bar2');
		}, 50);

		await db.transaction(async (tx: Transaction) => {
			const before = await tx.get('foo');
			console.log('before', before);

			await new Promise((resolve) => setTimeout(resolve, 100));
			const after = await tx.get('foo');

			console.log('after', after);
			expect(before).toBe(after);

			await tx.put('foo', 'bar3');
		});

		const value = await db.get('foo');
		console.log('value', value);
		expect(value).toBe('bar3');
	});

	it('should error if callback is not a function', async () => {
		db = await RocksDatabase.open('/tmp/testdb');
		await expect(db.transaction('foo' as any)).rejects.toThrow(new TypeError('Callback must be a function'));
	});
});
