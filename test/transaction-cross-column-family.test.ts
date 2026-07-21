import { RocksDatabase } from '../src/database.js';
import type { Transaction } from '../src/transaction.js';
import { generateDBPath } from './lib/util.js';
import { describe, expect, it } from 'vitest';

/**
 * A transaction belongs to a *database*, so it must serve reads on any column
 * family in that database — not only the one it was created against.
 *
 * `TransactionHandle::get` attempts a synchronous block-cache read first and
 * only falls back to async work when that misses, so these tests reopen the
 * database to guarantee a cold block cache. Reading a value still resident in
 * the block cache takes the synchronous path and passes even when the async
 * path is broken.
 */
describe('transaction reads across column families', () => {
	async function seedAndReopen() {
		const path = generateDBPath();
		let db = new RocksDatabase(path);
		let other = new RocksDatabase(path, { name: 'other' });
		db.open();
		other.open();
		await db.put('anchor', 'anchor-value');
		for (let i = 0; i < 25; i++) {
			await other.put(`key-${i}`, `value-${i}`);
		}
		// push the records out of the memtable and into SSTs
		await other.compact();
		await db.compact();
		other.close();
		db.close();

		// reopening drops the block cache, so the first read of each key misses
		// the block-cache tier and takes the async path under test
		db = new RocksDatabase(path);
		other = new RocksDatabase(path, { name: 'other' });
		db.open();
		other.open();
		return { db, other };
	}

	it('async get on another column family returns the record with a cold block cache', async () => {
		const { db, other } = await seedAndReopen();
		try {
			await db.transaction(async (txn: Transaction) => {
				// the transaction was created against `db`'s column family; every
				// read below is issued on `other`'s
				const values: (string | undefined)[] = [];
				for (let i = 0; i < 25; i++) {
					values.push(await other.get(`key-${i}`, { transaction: txn }));
				}
				expect(values).toEqual(Array.from({ length: 25 }, (_, i) => `value-${i}`));
			});
		} finally {
			other.close();
			db.close();
		}
	});

	it("async get on another column family does not read the transaction's own column family", async () => {
		const { db, other } = await seedAndReopen();
		try {
			await db.transaction(async (txn: Transaction) => {
				// 'anchor' exists only in db's column family. Reading it through
				// `other` must report not-found rather than resolving against the
				// transaction's own column family.
				expect(await other.get('anchor', { transaction: txn })).toBeUndefined();
			});
		} finally {
			other.close();
			db.close();
		}
	});

	it('getKeysCount on another column family counts that column family', async () => {
		const { db, other } = await seedAndReopen();
		try {
			await db.transaction(async (txn: Transaction) => {
				// db's column family holds exactly one key ('anchor'); other's holds 25
				expect(other.getKeysCount({ transaction: txn })).toBe(25);
				expect(db.getKeysCount({ transaction: txn })).toBe(1);
			});
		} finally {
			other.close();
			db.close();
		}
	});
});
