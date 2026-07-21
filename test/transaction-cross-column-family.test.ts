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
		let third = new RocksDatabase(path, { name: 'third' });
		db.open();
		other.open();
		third.open();
		await db.put('anchor', 'anchor-value');
		for (let i = 0; i < 25; i++) {
			await db.put(`own-${i}`, `own-value-${i}`);
			await other.put(`key-${i}`, `value-${i}`);
			await third.put(`third-${i}`, `third-value-${i}`);
		}
		// push the records out of the memtable and into SSTs
		await other.compact();
		await third.compact();
		await db.compact();
		third.close();
		other.close();
		db.close();

		// reopening drops the block cache, so the first read of each key misses
		// the block-cache tier and takes the async path under test
		db = new RocksDatabase(path);
		other = new RocksDatabase(path, { name: 'other' });
		third = new RocksDatabase(path, { name: 'third' });
		db.open();
		other.open();
		third.open();
		return { db, other, third };
	}

	it('async get on another column family returns the record with a cold block cache', async () => {
		const { db, other, third } = await seedAndReopen();
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
			third.close();
			other.close();
			db.close();
		}
	});

	it("async get on another column family does not read the transaction's own column family", async () => {
		const { db, other, third } = await seedAndReopen();
		try {
			await db.transaction(async (txn: Transaction) => {
				// 'anchor' exists only in db's column family. Reading it through
				// `other` must report not-found rather than resolving against the
				// transaction's own column family.
				expect(await other.get('anchor', { transaction: txn })).toBeUndefined();
			});
		} finally {
			third.close();
			other.close();
			db.close();
		}
	});

	// Reading the transaction's own column family must not poison the reads that
	// follow on another one, and vice versa. A single foreign read per transaction
	// would not catch an override that is applied once and then latches.
	it('alternates between the own and a foreign column family within one transaction', async () => {
		const { db, other, third } = await seedAndReopen();
		try {
			await db.transaction(async (txn: Transaction) => {
				for (let i = 0; i < 10; i++) {
					expect(await db.get(`own-${i}`, { transaction: txn })).toBe(`own-value-${i}`);
					expect(await other.get(`key-${i}`, { transaction: txn })).toBe(`value-${i}`);
				}
				// and the own column family still reads correctly afterwards
				expect(await db.get('anchor', { transaction: txn })).toBe('anchor-value');
			});
		} finally {
			third.close();
			other.close();
			db.close();
		}
	});

	// The reported failure aggregated four tables in one request, so cover more
	// than a single foreign column family per transaction.
	it('reads three column families within one transaction', async () => {
		const { db, other, third } = await seedAndReopen();
		try {
			await db.transaction(async (txn: Transaction) => {
				for (let i = 0; i < 10; i++) {
					expect(await other.get(`key-${i}`, { transaction: txn })).toBe(`value-${i}`);
					expect(await third.get(`third-${i}`, { transaction: txn })).toBe(`third-value-${i}`);
					expect(await db.get(`own-${i}`, { transaction: txn })).toBe(`own-value-${i}`);
				}
				// each column family sees only its own keys
				expect(await third.get('anchor', { transaction: txn })).toBeUndefined();
				expect(await other.get('third-0', { transaction: txn })).toBeUndefined();
			});
		} finally {
			third.close();
			other.close();
			db.close();
		}
	});

	it('getKeysCount on another column family counts that column family', async () => {
		const { db, other, third } = await seedAndReopen();
		try {
			await db.transaction(async (txn: Transaction) => {
				// db holds 'anchor' plus 25 own-* keys; the other two hold 25 each
				expect(other.getKeysCount({ transaction: txn })).toBe(25);
				expect(third.getKeysCount({ transaction: txn })).toBe(25);
				expect(db.getKeysCount({ transaction: txn })).toBe(26);
			});
		} finally {
			third.close();
			other.close();
			db.close();
		}
	});
});
