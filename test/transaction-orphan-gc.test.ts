import { registryStatus } from '../src/index.js';
import { Transaction } from '../src/transaction.js';
import { dbRunner } from './lib/util.js';
import { spawn } from 'node:child_process';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { describe, expect, it } from 'vitest';

const dependentFixturePath = join(__dirname, 'fixtures', 'transaction-orphan-dependents.mts');

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
const itWithNodeGC = it.skipIf(Boolean(process.versions.bun || process.versions.deno));

function runDependentFixture(
	mode: 'async-get' | 'iterator' | 'routed-iterator',
	dbPath: string
): Promise<{ code: number | null; signal: NodeJS.Signals | null; stderr: string }> {
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, ['--expose-gc', dependentFixturePath, mode, dbPath], {
			env: {
				...process.env,
				ROCKSDB_JS_TXN_GET_DELAY_MS: mode === 'async-get' ? '5250' : undefined,
			},
		});
		let stderr = '';
		child.stderr.on('data', (chunk) => {
			stderr += chunk.toString();
		});
		child.on('close', (code, signal) => resolve({ code, signal, stderr }));
		child.on('error', reject);
	});
}

async function expectDependentFixtureSurvives(
	mode: 'async-get' | 'iterator' | 'routed-iterator'
): Promise<void> {
	const dbPath = join(process.cwd(), `.transaction-orphan-${mode}-${process.pid}-${Date.now()}`);
	try {
		const { code, signal, stderr } = await runDependentFixture(mode, dbPath);
		expect(signal, stderr).toBeNull();
		expect(code, stderr).toBe(0);
	} finally {
		if (!process.env.KEEP_FILES) {
			rmSync(dbPath, { force: true, recursive: true, maxRetries: 3, retryDelay: 100 });
		}
	}
}

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

describe('orphaned transactions', () => {
	itWithNodeGC(
		'should defer orphan cleanup past the async-work wait timeout',
		() => expectDependentFixtureSurvives('async-get'),
		15_000
	);

	itWithNodeGC(
		'should keep a transaction alive until its iterator closes',
		() => expectDependentFixtureSurvives('iterator'),
		15_000
	);

	itWithNodeGC(
		'should keep a transaction alive until a routed iterator closes',
		() => expectDependentFixtureSurvives('routed-iterator'),
		15_000
	);

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
