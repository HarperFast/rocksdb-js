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

// Node runs with --expose-gc; Bun exposes Bun.gc instead and leaves globalThis.gc undefined. Deno
// exposes neither: its Vitest workers are forked processes that never see --v8-flags (#770), so
// these cases skip there rather than fail, like every other GC-dependent test in this suite.
const forceGC: (() => void) | undefined =
	typeof globalThis.gc === 'function'
		? globalThis.gc
		: typeof (globalThis as { Bun?: { gc?: (sync: boolean) => void } }).Bun?.gc === 'function'
			? () => (globalThis as unknown as { Bun: { gc: (sync: boolean) => void } }).Bun.gc(true)
			: undefined;

const itWithGC = it.skipIf(!forceGC);

/**
 * V8 collects the dropped wrapper on its own schedule, and the finalizer runs after the GC pass, so
 * poll rather than assuming one gc() is enough. Fails the test by timing out on the assertion below.
 */
async function collectOrphans(path: string, timeoutMs = 5000) {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		forceGC!();
		await delay(20);
		if (status(path).transactions === 0) return;
	}
}

async function forceGCBriefly(durationMs = 50) {
	const deadline = Date.now() + durationMs;
	while (Date.now() < deadline) {
		forceGC!();
		await delay(5);
	}
}

describe('orphaned transactions', () => {
	itWithGC('should release a transaction dropped without commit or abort', () =>
		dbRunner(async ({ db, dbPath }) => {
			await db.put('foo', 'bar');

			await (async () => {
				const txn = new Transaction(db.store);
				// The snapshot is established lazily by the first read through the transaction.
				expect(await txn.get('foo')).toBe('bar');
				expect(db.getDBIntProperty('rocksdb.num-snapshots')).toBe(1);

				const [detail] = status(dbPath).transactionDetails;
				expect(detail.id).toBeTypeOf('number');
				expect(detail.ageMs).toBeGreaterThanOrEqual(0);
			})();

			await collectOrphans(dbPath);

			expect(status(dbPath).transactions).toBe(0);
			expect(db.getDBIntProperty('rocksdb.num-snapshots')).toBe(0);
			expect(db.getDBIntProperty('rocksdb.oldest-snapshot-time')).toBe(0);
		})
	);

	itWithGC('should release a transaction dropped after a failed commit', () =>
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
		})
	);

	itWithGC('should not disturb a transaction that is still referenced', () =>
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
		})
	);

	// The caller drops its own reference before the commit settles — what harper's
	// DatabaseTransaction does when it nulls `this.transaction` before awaiting. Note this does NOT
	// exercise finalization *during* Committing: the pending commit promise's executor still holds
	// the wrapper, so V8 cannot collect it until the commit settles. That deferral branch is
	// therefore unreachable from JS by construction, and this asserts the property that matters —
	// dropping the reference neither loses the write nor leaks the handle.
	itWithGC(
		'should commit and release when the caller drops its reference before the commit settles',
		() =>
			dbRunner(async ({ db, dbPath }) => {
				let commit: Promise<unknown> | undefined;
				await (async () => {
					const txn = new Transaction(db.store);
					await txn.get('foo');
					txn.putSync('foo', 'committed');
					commit = txn.commit();
				})();

				forceGC!();
				await commit;
				await collectOrphans(dbPath);

				expect(await db.get('foo')).toBe('committed');
				expect(status(dbPath).transactions).toBe(0);
				expect(db.getDBIntProperty('rocksdb.num-snapshots')).toBe(0);
			})
	);

	// getBinary does not retain the Transaction, so the wrapper can be collected while the
	// cache-miss get is still queued. The get must keep the native wrapper alive until execute
	// finishes, then the snapshot is released.
	itWithGC('should complete an in-flight async get when the transaction is dropped', () =>
		dbRunner(async ({ db, dbPath }) => {
			await db.put('foo', 'bar');
			await db.flush();

			let pending: Promise<unknown> | undefined;
			await (async () => {
				const txn = new Transaction(db.store);
				const result = txn.getBinary('foo');
				if (!(result instanceof Promise)) {
					throw new Error('expected a cache-miss async get');
				}
				pending = result;
			})();

			await forceGCBriefly();
			await expect(pending).resolves.toBeInstanceOf(Buffer);
			await collectOrphans(dbPath);

			expect(status(dbPath).transactions).toBe(0);
			expect(db.getDBIntProperty('rocksdb.num-snapshots')).toBe(0);
		})
	);

	itWithGC('should complete a database-routed async get when the transaction is dropped', () =>
		dbRunner(async ({ db, dbPath }) => {
			await db.put('foo', 'bar');
			await db.flush();

			let pending: Promise<unknown> | undefined;
			await (async () => {
				const txn = new Transaction(db.store);
				const result = db.getBinary('foo', { transaction: txn });
				if (!(result instanceof Promise)) {
					throw new Error('expected a cache-miss async get');
				}
				pending = result;
			})();

			await forceGCBriefly();
			await expect(pending).resolves.toBeInstanceOf(Buffer);
			await collectOrphans(dbPath);

			expect(status(dbPath).transactions).toBe(0);
			expect(db.getDBIntProperty('rocksdb.num-snapshots')).toBe(0);
		})
	);

	itWithGC('should complete two in-flight async gets when the transaction is dropped', () =>
		dbRunner(async ({ db, dbPath }) => {
			await db.put('foo', Buffer.alloc(16 * 1024, 1));
			await db.put('baz', Buffer.alloc(16 * 1024, 2));
			await db.flush();

			let first: Promise<unknown> | undefined;
			let second: Promise<unknown> | undefined;
			await (async () => {
				const txn = new Transaction(db.store);
				const firstResult = txn.getBinary('foo');
				const secondResult = txn.getBinary('baz');
				if (!(firstResult instanceof Promise) || !(secondResult instanceof Promise)) {
					throw new Error('expected cache-miss async gets');
				}
				first = firstResult;
				second = secondResult;
			})();

			await forceGCBriefly();
			await Promise.all([
				expect(first).resolves.toBeInstanceOf(Buffer),
				expect(second).resolves.toBeInstanceOf(Buffer),
			]);
			await collectOrphans(dbPath);

			expect(status(dbPath).transactions).toBe(0);
			expect(db.getDBIntProperty('rocksdb.num-snapshots')).toBe(0);
		})
	);

	// An orphan that never read holds no snapshot, so close() reaches ClearSnapshot with nothing set
	// — a different teardown path than every case above.
	itWithGC('should release a transaction dropped before it ever read', () =>
		dbRunner(async ({ db, dbPath }) => {
			await (async () => {
				const txn = new Transaction(db.store);
				expect(txn.id).toBeTypeOf('number');
				expect(status(dbPath).transactions).toBe(1);
				expect(db.getDBIntProperty('rocksdb.num-snapshots')).toBe(0);
			})();

			await collectOrphans(dbPath);

			expect(status(dbPath).transactions).toBe(0);
		})
	);
});
