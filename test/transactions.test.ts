import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { join } from 'node:path';
import { rimraf } from 'rimraf';
import { RocksDatabase } from '../src/index.js';
import { tmpdir } from 'node:os';
import type { Transaction } from '../src/transaction.js';

describe('Transactions', () => {
	let db: RocksDatabase | null = null;
	const dbPath = join(tmpdir(), 'testdb');

	beforeEach(() => rimraf(dbPath));

	afterEach(() => {
		if (db) {
			db.close();
			db = null;
		}
		return rimraf(dbPath);
	});

	it('should get a value', async () => {
		db = await RocksDatabase.open(dbPath);
		await db.put('foo', 'bar');
		await db.transaction(async (txn: Transaction) => {
			const value = await txn.get('foo');
			expect(value).toBe('bar');
		});
	});

	it('should set a value', async () => {
		db = await RocksDatabase.open(dbPath);
		await db.transaction(async (txn: Transaction) => {
			await txn.put('foo', 'bar');
		});
		const value = await db.get('foo');
		expect(value).toBe('bar');
	});

	it('should remove a value', async () => {
		db = await RocksDatabase.open(dbPath);
		await db.put('foo', 'bar');
		const value = await db.get('foo');
		expect(value).toBe('bar');
		await db.transaction(async (txn: Transaction) => {
			await txn.remove('foo');
		});
		const value2 = await db.get('foo');
		expect(value2).toBeUndefined();
	});

	it('should rollback on error', async () => {
		db = await RocksDatabase.open(dbPath);
		await db.put('foo', 'bar');
		await expect(db.transaction(async (txn: Transaction) => {
			await txn.put('foo', 'bar2');
			throw new Error('test');
		})).rejects.toThrow('test');
		const value = await db.get('foo');
		expect(value).toBe('bar');
	});

	/**
	 * TODO: This test is temporarily disabled because the current rocksdb-js
	 * design blocks the main thread causing a deadlock until RocksDB times out
	 */
	it.skip('should treat transaction as a snapshot', async () => {
		db = await RocksDatabase.open(dbPath);
		console.log('putting bar1');
		db.put('foo', 'bar1');

		setTimeout(() => {
			console.log('putting bar2');
			db?.put('foo', 'bar2');
			console.log('put bar2');
		}, 50);

		await db.transaction(async (txn: Transaction) => {
			const before = await txn.get('foo');
			console.log('before', before);

			await new Promise((resolve) => setTimeout(resolve, 100));
			const after = await txn.get('foo');

			console.log('after', after);
			expect(before).toBe(after);

			await txn.put('foo', 'bar3');
		});

		const value = await db.get('foo');
		console.log('value', value);
		expect(value).toBe('bar3');
	});

	it('should error if callback is not a function', async () => {
		db = await RocksDatabase.open(dbPath);
		await expect(db.transaction('foo' as any)).rejects.toThrow(new TypeError('Callback must be a function'));
	});
});
