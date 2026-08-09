import { registryStatus } from '../src/index.js';
import { Transaction } from '../src/transaction.js';
import { dbRunner } from './lib/util.js';
import { setTimeout as delay } from 'node:timers/promises';
import { describe, expect, it } from 'vitest';

/**
 * A transaction dropped without commit()/abort() used to live forever: the JS wrapper's finalizer
 * only reset its own shared_ptr, while DBDescriptor::transactionAdd holds a strong one, so
 * TransactionHandle::close() — the only ClearSnapshot() path — was never reached. The handle's read
 * snapshot then pinned `rocksdb.oldest-snapshot-time` for the life of the process, and RocksDB kept
 * every obsolete version behind it: on a high-churn secondary index that reached ~4 keys per live
 * row in 5 days and degraded bounded range scans ~21x, recoverable only by restart
 * (HarperFast/harper#2107).
 */

function status(path: string) {
	const entry = registryStatus().find((db) => db.path === path);
	if (!entry) throw new Error(`No registry entry for ${path}`);
	return entry;
}

/**
 * V8 collects the dropped wrapper on its own schedule, and the finalizer runs after the GC pass, so
 * poll rather than assuming one gc() is enough. Fails the test by timing out on the assertion below.
 */
async function collectOrphans(path: string, timeoutMs = 5000) {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		global.gc?.();
		await delay(20);
		if (status(path).transactions === 0) return;
	}
}

describe('orphaned transactions', () => {
	it('should release a transaction dropped without commit or abort', () =>
		dbRunner(async ({ db, dbPath }) => {
			await db.put('foo', 'bar');

			await (async () => {
				const txn = new Transaction(db.store);
				// The snapshot is established lazily by the first read through the transaction.
				expect(await txn.get('foo')).toBe('bar');
				expect(db.getDBIntProperty('rocksdb.num-snapshots')).toBe(1);

				const [detail] = status(dbPath).transactionDetails;
				expect(detail.snapshotSet).toBe(true);
				expect(detail.state).toBe('pending');
			})();

			await collectOrphans(dbPath);

			expect(status(dbPath).transactions).toBe(0);
			expect(db.getDBIntProperty('rocksdb.num-snapshots')).toBe(0);
			expect(db.getDBIntProperty('rocksdb.oldest-snapshot-time')).toBe(0);
		}));

	it('should release a transaction dropped after a failed commit', () =>
		dbRunner(async ({ db, dbPath }) => {
			await db.put('foo', 'bar');

			let code: string | undefined;
			await (async () => {
				const txn = new Transaction(db.store);
				await txn.get('foo');
				txn.putSync('foo', 'from txn');
				// Conflicting outside write: the commit rejects, and rocksdb-js deliberately leaves
				// the handle open so the caller can retry or abort it. Here the caller does neither.
				await db.put('foo', 'from outside');
				// Keep only the code: TransactionRetryableError holds the Transaction in `.txn` so a
				// caller can retry through it, so retaining the error would keep the wrapper alive
				// and there would be nothing for GC to collect.
				await txn.commit().then(
					() => {
						throw new Error('expected the commit to reject');
					},
					(error) => {
						code = error.code;
					}
				);
			})();
			expect(code).toBe('ERR_BUSY');

			await collectOrphans(dbPath);

			expect(status(dbPath).transactions).toBe(0);
			expect(db.getDBIntProperty('rocksdb.num-snapshots')).toBe(0);
		}));

	it('should not disturb a transaction that is still referenced', () =>
		dbRunner(async ({ db, dbPath }) => {
			await db.put('foo', 'bar');

			const txn = new Transaction(db.store);
			await txn.get('foo');

			await collectOrphans(dbPath, 500);

			expect(status(dbPath).transactions).toBe(1);
			expect(db.getDBIntProperty('rocksdb.num-snapshots')).toBe(1);
			expect(await txn.get('foo')).toBe('bar');

			txn.abort();
			expect(status(dbPath).transactions).toBe(0);
		}));

	it('should let an in-flight commit finish when the wrapper is collected mid-commit', () =>
		dbRunner(async ({ db, dbPath }) => {
			let commit: Promise<unknown> | undefined;
			await (async () => {
				const txn = new Transaction(db.store);
				await txn.get('foo');
				txn.putSync('foo', 'committed');
				// Drop the wrapper (only the commit promise is retained) the way harper's
				// DatabaseTransaction does: it nulls `this.transaction` before awaiting the commit.
				commit = txn.commit();
			})();

			global.gc?.();
			await commit;

			expect(await db.get('foo')).toBe('committed');
			expect(status(dbPath).transactions).toBe(0);
			expect(db.getDBIntProperty('rocksdb.num-snapshots')).toBe(0);
		}));
});
