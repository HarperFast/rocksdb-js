import { RocksDatabase, registryStatus } from '../src/index.ts';
import { iteratorPoolStats } from '../src/load-binding.ts';
import { Transaction } from '../src/transaction.ts';
import { dbRunner, generateDBPath } from './lib/util.ts';
import { spawnSync } from 'node:child_process';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

/**
 * Transaction-backed `getRange()` reuses idle native iterators from a
 * per-transaction pool. Every case compares a pooled range against a fresh
 * one: passing any advanced ReadOptions (here `fillCache: false`, which is also
 * the default) takes the unpooled path with identical read options.
 */

const FRESH = { fillCache: false };

function delta(before: ReturnType<typeof iteratorPoolStats>) {
	const after = iteratorPoolStats();
	return {
		created: after.created - before.created,
		reused: after.reused - before.reused,
		fallbacks: after.fallbacks - before.fallbacks,
	};
}

function status(path: string) {
	const entry = registryStatus().find((db) => db.path === path);
	if (!entry) throw new Error(`No registry entry for ${path}`);
	return entry;
}

const forceGC: (() => void) | undefined =
	typeof globalThis.gc === 'function'
		? globalThis.gc
		: typeof (globalThis as { Bun?: { gc?: (sync: boolean) => void } }).Bun?.gc === 'function'
			? () => (globalThis as unknown as { Bun: { gc: (sync: boolean) => void } }).Bun.gc(true)
			: undefined;

const RANGE_OPTIONS: Record<string, unknown>[] = [
	{},
	{ start: 'c' },
	{ end: 'f' },
	{ start: 'b', end: 'e' },
	{ start: 'b', end: 'e', inclusiveEnd: true },
	{ start: 'b', end: 'e', exclusiveStart: true },
	{ start: 'b', end: 'e', exclusiveStart: true, inclusiveEnd: true },
	{ start: 'bb', end: 'dd' },
	{ start: 'c', end: 'c~' },
	{ key: 'c' },
	{ key: 'missing' },
	{ start: 'x', end: 'z' },
	{ reverse: true },
	{ reverse: true, start: 'e', end: 'b' },
	{ reverse: true, start: 'e', end: 'b', exclusiveStart: false },
	{ reverse: true, start: 'e', end: 'b', inclusiveEnd: false },
	{ reverse: true, start: 'e' },
	{ reverse: true, end: 'c' },
	{ start: 'b', limit: 2 },
	{ reverse: true, limit: 3 },
	{ start: 'b', end: 'f', values: false },
	{ reverse: true, values: false, limit: 2 },
];

describe('Transaction iterator pool', () => {
	it('returns the same rows as a fresh iterator for every range shape', () =>
		dbRunner(async ({ db }) => {
			for (const key of ['a', 'b', 'c', 'd', 'e', 'f', 'g']) {
				db.putSync(key, `committed-${key}`);
			}
			const txn = new Transaction(db.store);
			try {
				txn.putSync('bc', 'staged-bc');
				txn.putSync('d', 'staged-d');
				txn.removeSync('e');
				const before = iteratorPoolStats();
				// Two passes so the second reuses handles the first left in the pool,
				// each time pointed at a different range shape.
				for (let pass = 0; pass < 2; pass++) {
					for (const options of RANGE_OPTIONS) {
						const fresh = txn.getRange({ ...options, ...FRESH }).asArray;
						expect(txn.getRange(options).asArray, JSON.stringify(options)).toEqual(fresh);
						expect(db.getRange({ ...options, transaction: txn }).asArray).toEqual(fresh);
					}
				}
				const counts = delta(before);
				expect(counts.reused).toBeGreaterThanOrEqual(RANGE_OPTIONS.length * 4 - 2);
				expect(counts.created).toBeLessThanOrEqual(2);
			} finally {
				txn.abort();
			}
		}));

	it('sees writes made to the transaction after a pooled iterator exists', () =>
		dbRunner(async ({ db }) => {
			db.putSync('a', 1);
			db.putSync('c', 3);
			const txn = new Transaction(db.store);
			try {
				expect(txn.getRange().asArray.map((e) => e.key)).toEqual(['a', 'c']);
				const before = iteratorPoolStats();
				txn.putSync('b', 2);
				expect(txn.getRange().asArray).toEqual([
					{ key: 'a', value: 1 },
					{ key: 'b', value: 2 },
					{ key: 'c', value: 3 },
				]);
				txn.putSync('a', 10);
				txn.removeSync('c');
				txn.putSync('d', 4);
				expect(txn.getRange({ start: 'a' }).asArray).toEqual([
					{ key: 'a', value: 10 },
					{ key: 'b', value: 2 },
					{ key: 'd', value: 4 },
				]);
				expect(txn.getRange({ reverse: true }).asArray.map((e) => e.key)).toEqual(['d', 'b', 'a']);
				// A commit by another writer after the snapshot stays invisible to a reused iterator.
				db.putSync('bb', 'outside');
				expect(txn.getRange({ start: 'b', end: 'c' }).asArray.map((e) => e.key)).toEqual(['b']);
				expect(delta(before)).toMatchObject({ created: 0, reused: 4 });
			} finally {
				txn.abort();
			}
		}));

	it('returns the iterator to the pool on early break, return and limit', () =>
		dbRunner(async ({ db }) => {
			for (const key of ['a', 'b', 'c', 'd']) {
				db.putSync(key, key);
			}
			const txn = new Transaction(db.store);
			try {
				expect(txn.getRange().asArray.length).toBe(4);
				const before = iteratorPoolStats();
				for (const entry of txn.getRange()) {
					if (entry.key === 'b') break;
				}
				const iterator = txn.getRange()[Symbol.iterator]();
				iterator.next();
				iterator.return!();
				expect(() => iterator.next()).toThrow('Next failed: Iterator not initialized');
				expect(iterator.return!().done).toBe(true);
				expect(txn.getRange({ limit: 1 }).asArray.map((e) => e.key)).toEqual(['a']);
				expect(txn.getRange({ start: 'c' }).asArray.map((e) => e.key)).toEqual(['c', 'd']);
				expect(delta(before)).toMatchObject({ created: 0, reused: 4 });
			} finally {
				txn.abort();
			}
		}));

	it('gives nested and concurrent ranges their own iterators', () =>
		dbRunner(async ({ db }) => {
			for (const key of ['a', 'b', 'c', 'd']) {
				db.putSync(key, key);
			}
			const txn = new Transaction(db.store);
			try {
				const before = iteratorPoolStats();
				const pairs: string[] = [];
				for (const outer of txn.getRange()) {
					for (const inner of txn.getRange({ start: outer.key, reverse: true })) {
						pairs.push(`${outer.key}${inner.key}`);
					}
				}
				expect(pairs).toEqual(['aa', 'bb', 'ba', 'cc', 'cb', 'ca', 'dd', 'dc', 'db', 'da']);
				// outer + one inner handle, reused for every later inner range
				expect(delta(before)).toMatchObject({ created: 2, reused: 3 });

				const first = txn.getRange()[Symbol.iterator]();
				const second = txn.getRange({ reverse: true })[Symbol.iterator]();
				expect([first.next().value.key, second.next().value.key]).toEqual(['a', 'd']);
				expect([first.next().value.key, second.next().value.key]).toEqual(['b', 'c']);
				first.return!();
				second.return!();
				expect(delta(before)).toMatchObject({ created: 2, reused: 5 });
			} finally {
				txn.abort();
			}
		}));

	for (const action of ['commit', 'commitSync', 'abort'] as const) {
		it(`releases pooled iterators on ${action} and keeps terminal behavior`, () =>
			dbRunner(async ({ db, dbPath }) => {
				db.putSync('a', 'a');
				db.putSync('b', 'b');
				const baseline = status(dbPath);
				const txn = new Transaction(db.store);
				txn.putSync('c', 'c');
				const exhausted = txn.getRange()[Symbol.iterator]();
				while (!exhausted.next().done);
				const busy = txn.getRange()[Symbol.iterator]();
				busy.next();
				// A finished iterator keeps reporting done while the transaction is pending.
				expect(exhausted.next().done).toBe(true);
				expect(status(dbPath).closables).toBeGreaterThan(baseline.closables);

				if (action === 'commit') {
					await txn.commit();
				} else if (action === 'commitSync') {
					txn.commitSync();
				} else {
					txn.abort();
				}

				expect(() => exhausted.next()).toThrow('Iterator not initialized');
				expect(() => busy.next()).toThrow('Iterator not initialized');
				expect(busy.return!().done).toBe(true);
				expect(status(dbPath)).toMatchObject({
					closables: baseline.closables,
					transactions: baseline.transactions,
				});
			}));
	}

	it('does not pool without a snapshot or with advanced read options', () =>
		dbRunner(async ({ db }) => {
			db.putSync('a', 1);
			const txn = new Transaction(db.store, { disableSnapshot: true });
			try {
				const before = iteratorPoolStats();
				expect(txn.getRange().asArray.map((e) => e.key)).toEqual(['a']);
				// Without a snapshot reads see the latest commit, which a reused iterator would miss.
				db.putSync('b', 2);
				expect(txn.getRange().asArray.map((e) => e.key)).toEqual(['a', 'b']);
				expect(delta(before)).toEqual({ created: 0, reused: 0, fallbacks: 0 });
			} finally {
				txn.abort();
			}
			const snapshotTxn = new Transaction(db.store);
			try {
				const before = iteratorPoolStats();
				expect(snapshotTxn.getRange({ fillCache: true }).asArray.length).toBe(2);
				expect(snapshotTxn.getRange({ readaheadSize: 0 }).asArray.length).toBe(2);
				expect(delta(before)).toEqual({ created: 0, reused: 0, fallbacks: 0 });
			} finally {
				snapshotTxn.abort();
			}
		}));

	it('falls back to a bounded iterator across a run of deleted keys', () =>
		dbRunner(async ({ db }) => {
			db.putSync('a', 'a');
			for (let i = 0; i < 500; i++) {
				db.putSync(`m${1000 + i}`, 'x');
			}
			for (let i = 0; i < 500; i++) {
				db.removeSync(`m${1000 + i}`);
			}
			db.putSync('z', 'z');
			for (const flush of [false, true]) {
				if (flush) db.flushSync();
				const txn = new Transaction(db.store);
				try {
					txn.putSync('b', 'staged');
					txn.putSync('n', 'staged');
					const before = iteratorPoolStats();
					for (const options of [
						{ start: 'a', end: 'a~' },
						{ start: 'a', end: 'c' },
						{ start: 'a', end: 'm' },
						{ start: 'b', end: 'y' },
						{ start: 'm', end: 'y' },
						{ start: 'a' },
						{ reverse: true, start: 'y', end: 'm' },
						{ reverse: true, start: 'y', end: 'a' },
						{ reverse: true },
						{ start: 'y', end: 'z~' },
					]) {
						const fresh = txn.getRange({ ...options, ...FRESH }).asArray;
						expect(txn.getRange(options).asArray, JSON.stringify(options)).toEqual(fresh);
					}
					const counts = delta(before);
					expect(counts.fallbacks).toBeGreaterThan(0);
					expect(counts.reused).toBeGreaterThan(0);
				} finally {
					txn.abort();
				}
			}
		}));

	it('does not serve a dropped and recreated column family from the pool', () =>
		dbRunner(async ({ db, dbPath }) => {
			const column = RocksDatabase.open(dbPath, { name: 'pooled' });
			try {
				column.putSync('k', 'old');
				const txn = new Transaction(db.store);
				try {
					expect(column.getRange({ transaction: txn }).asArray.map((e) => e.value)).toEqual([
						'old',
					]);
					column.dropSync();
					column.close();
					column.open();
					column.putSync('k2', 'new');
					const fresh = column.getRange({ transaction: txn, ...FRESH }).asArray;
					expect(column.getRange({ transaction: txn }).asArray).toEqual(fresh);
				} finally {
					txn.abort();
				}
			} finally {
				column.close();
			}
		}));

	it('closes idle pooled iterators once the current macrotask ends', () =>
		dbRunner(async ({ db, dbPath }) => {
			db.putSync('a', 'a');
			const baseline = status(dbPath);
			const txn = new Transaction(db.store);
			try {
				const before = iteratorPoolStats();
				for (let i = 0; i < 3; i++) {
					expect(txn.getRange().asArray.length).toBe(1);
					expect(db.getRange({ transaction: txn }).asArray.length).toBe(1);
				}
				// Both routing forms use the database's own handle, so they share one iterator.
				expect(delta(before)).toMatchObject({ created: 1, reused: 5 });
				// The transaction and its one idle iterator.
				expect(status(dbPath).closables).toBe(baseline.closables + 2);
				// An idle iterator pins memtables; a transaction held open across I/O must not keep it.
				await new Promise((resolve) => setImmediate(resolve));
				expect(status(dbPath).closables).toBe(baseline.closables + 1);
				expect(txn.getRange().asArray.length).toBe(1);
				expect(delta(before)).toMatchObject({ created: 2, reused: 5 });
			} finally {
				txn.abort();
			}
		}));

	it('closes, rather than pools, an iterator that was busy when the pool was trimmed', () =>
		dbRunner(async ({ db, dbPath }) => {
			for (const key of ['a', 'b', 'c']) {
				db.putSync(key, key);
			}
			const baseline = status(dbPath);
			const txn = new Transaction(db.store);
			try {
				const iterator = txn.getRange()[Symbol.iterator]();
				iterator.next();
				await new Promise((resolve) => setImmediate(resolve));
				while (!iterator.next().done);
				// Released after the trim ran: it must not sit idle until the transaction ends.
				expect(status(dbPath).closables).toBe(baseline.closables + 1);
			} finally {
				txn.abort();
			}
		}));

	it('does not pool once the transaction has staged many deletes', () =>
		dbRunner(async ({ db }) => {
			db.putSync('a', 'a');
			db.putSync('z', 'z');
			const txn = new Transaction(db.store);
			try {
				for (let i = 0; i < 100; i++) {
					txn.removeSync(`m${1000 + i}`);
				}
				const before = iteratorPoolStats();
				for (let i = 0; i < 3; i++) {
					expect(txn.getRange({ start: 'a', end: 'b' }).asArray.map((e) => e.key)).toEqual(['a']);
				}
				expect(delta(before)).toEqual({ created: 0, reused: 0, fallbacks: 0 });
			} finally {
				txn.abort();
			}
		}));

	it('resumes a bounded fallback after a row returned on the slow path', () =>
		dbRunner(async ({ db }) => {
			const big = 'x'.repeat(200_000);
			db.putSync('a', big);
			for (let i = 0; i < 300; i++) {
				db.putSync(`m${1000 + i}`, 'x');
			}
			for (let i = 0; i < 300; i++) {
				db.removeSync(`m${1000 + i}`);
			}
			db.putSync('z', 'z');
			const txn = new Transaction(db.store);
			try {
				txn.putSync('n', 'staged');
				expect(txn.getRange({ end: 'a~' }).asArray.length).toBe(1);
				const before = iteratorPoolStats();
				const rows = txn.getRange().asArray;
				expect(rows.map((e) => e.key)).toEqual(['a', 'n', 'z']);
				expect(rows[0].value).toBe(big);
				expect(rows).toEqual(txn.getRange(FRESH).asArray);
				expect(delta(before)).toMatchObject({ reused: 1 });
				expect(delta(before).fallbacks).toBeGreaterThan(0);
			} finally {
				txn.abort();
			}
		}));

	it('closes a database whose pending transactions hold pooled iterators', () =>
		dbRunner({ skipOpen: true }, async ({ dbPath }) => {
			const db = RocksDatabase.open(dbPath);
			for (const key of ['a', 'b', 'c']) {
				db.putSync(key, key);
			}
			const busy: Iterator<unknown>[] = [];
			const transactions: Transaction[] = [];
			for (let i = 0; i < 8; i++) {
				const txn = new Transaction(db.store);
				transactions.push(txn);
				// A self-close detaches nothing during the sweep (the handle drops its
				// descriptor first); the shutdown() fixture below covers that path.
				for (let j = 0; j < 3; j++) {
					expect(txn.getRange().asArray.length).toBe(3);
					const iterator = txn.getRange()[Symbol.iterator]();
					iterator.next();
					busy.push(iterator);
				}
			}
			db.close();
			expect(registryStatus().find((entry) => entry.path === dbPath)).toBeUndefined();
			for (const iterator of busy) {
				expect(() => iterator.next()).toThrow('Iterator not initialized');
			}
		}));

	it.skipIf(!forceGC)('closes a dropped transaction that only holds idle pooled iterators', () =>
		dbRunner(async ({ db, dbPath }) => {
			db.putSync('a', 'a');
			const baseline = status(dbPath);
			(() => {
				const txn = new Transaction(db.store);
				for (let i = 0; i < 3; i++) {
					expect(txn.getRange().asArray.length).toBe(1);
				}
			})();
			expect(status(dbPath).transactions).toBe(baseline.transactions + 1);
			for (let i = 0; i < 20 && status(dbPath).transactions !== baseline.transactions; i++) {
				forceGC!();
				await new Promise((resolve) => setTimeout(resolve, 10));
			}
			expect(status(dbPath)).toMatchObject({
				closables: baseline.closables,
				transactions: baseline.transactions,
			});
		})
	);
});

describe('Transaction iterator pool teardown', () => {
	it.skipIf(process.platform === 'win32')(
		'survives shutdown() while transactions hold pooled iterators',
		async () => {
			for (let i = 0; i < 3; i++) {
				const result = spawnSync(
					process.execPath,
					[
						join(import.meta.dirname, 'fixtures', 'fork-pooled-iterator-shutdown.mts'),
						generateDBPath(),
					],
					{ encoding: 'utf8', env: { ...process.env, MALLOC_PERTURB_: '165' }, timeout: 60_000 }
				);
				expect(result.signal, result.stderr).toBeNull();
				expect(result.status, result.stderr).toBe(0);
			}
		}
	);
});
