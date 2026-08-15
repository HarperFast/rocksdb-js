import { RocksDatabase } from '../src/index.ts';
import type { Transaction } from '../src/transaction.ts';
import { dbRunner } from './lib/util.ts';
import { describe, expect, it } from 'vitest';

describe('Drop', () => {
	it('should error if database is not open', () =>
		dbRunner({ skipOpen: true }, async ({ db }) => {
			expect(() => db.dropSync()).toThrow('Database not open');
			await expect(db.drop()).rejects.toThrow('Database not open');
		}));

	it('should drop (clear) default column family', () =>
		dbRunner(({ db }) => {
			db.putSync('key', 'value');
			expect(db.getSync('key')).toBe('value');
			db.dropSync();
			expect(db.columns).toEqual(['default']);
			db.close();
			db.open();
			expect(db.getSync('key')).toBeUndefined();
		}));

	it('should drop (clear) default column family asynchronously', () =>
		dbRunner(async ({ db }) => {
			db.putSync('key', 'value');
			expect(db.getSync('key')).toBe('value');
			await db.drop();
			expect(db.columns).toEqual(['default']);
			db.close();
			db.open();
			expect(db.getSync('key')).toBeUndefined();
		}));

	it('should drop a column family', () =>
		dbRunner({ dbOptions: [{ name: 'test' }] }, ({ db }) => {
			db.putSync('key', 'value');
			expect(db.getSync('key')).toBe('value');
			expect(db.columns).toEqual(['default', 'test']);
			db.dropSync();
			expect(db.columns).toEqual(['default']);
			db.close();
			db.open();
			expect(db.getSync('key')).toBeUndefined();
		}));

	it('should drop a column family asynchronously', () =>
		dbRunner({ dbOptions: [{ name: 'test' }] }, async ({ db }) => {
			db.putSync('key', 'value');
			expect(db.getSync('key')).toBe('value');
			expect(db.columns).toEqual(['default', 'test']);
			await db.drop();
			expect(db.columns).toEqual(['default']);
			db.close();
			db.open();
			expect(db.getSync('key')).toBeUndefined();
		}));

	// Regression test for the "ghost table" bug: a drop must free the column
	// family name immediately, even while other instances still hold the
	// dropped column family open. Before the fix, the dropped entry stayed in
	// the registry's by-name map whenever any other handle referenced it, so a
	// reopen-by-name reused the dangling handle and every write failed with
	// "Invalid column family specified in write batch" until a full process
	// restart.
	it('should free the column family name immediately on drop, even with another instance holding it', () =>
		dbRunner(
			{ dbOptions: [{ name: 'test' }, { name: 'test' }] },
			async ({ db: db1, dbPath }, { db: db2 }) => {
				db1.putSync('key', 'value');
				db2.putSync('key2', 'value2');
				expect(db1.columns).toEqual(['default', 'test']);
				expect(db2.columns).toEqual(['default', 'test']);

				await db1.drop();

				// the name is freed immediately: the registry no longer lists it
				expect(db1.columns).toEqual(['default']);
				expect(db2.columns).toEqual(['default']);

				// an instance still holding the dropped column family can read it
				// until it closes
				expect(db2.getSync('key')).toBe('value');
				expect(db2.getSync('key2')).toBe('value2');

				// a write through the dropped handle is discarded rather than
				// applied, and does not throw. This used to be the LAST assertion in
				// the test, because the failed write contaminated the env's shared
				// write path and every other handle's writes failed afterward too;
				// see 'should not poison the database' below.
				db2.putSync('key4', 'value4');
				expect(db2.getSync('key4')).toBeUndefined();

				// reopening by the same name creates a fresh, WRITABLE column
				// family instead of reusing the dropped handle - and it stays
				// writable even after the discarded write above
				const db3 = RocksDatabase.open(dbPath, { name: 'test' });
				try {
					db3.putSync('key3', 'value3');
					expect(db3.getSync('key3')).toBe('value3');
					// the fresh column family does not see the dropped data
					expect(db3.getSync('key')).toBeUndefined();
					expect(db3.columns).toEqual(['default', 'test']);
				} finally {
					db3.close();
				}
			}
		));

	// Regression test for the environment-wide poison. RocksDB applies a write
	// batch to the WAL first and to the memtables second; a batch naming a
	// dropped column family fails in between, which RocksDB treats as
	// unrecoverable inconsistency and records as a background error on the whole
	// environment. Every subsequent write to EVERY column family on that path
	// then failed with the same, unattributable 'Invalid column family specified
	// in write batch' until the environment was closed and reopened - one racing
	// writer took the whole database down. Harper hit this by dropping a table
	// while background cache writes to it were still in flight, and lost 44
	// unrelated tests in one shard to the cascade.
	it('should not poison the database when a stale handle writes to a dropped column family', () =>
		dbRunner(
			{ dbOptions: [{ name: 'victim' }, { name: 'doomed' }, { name: 'doomed' }] },
			async ({ db: victim, dbPath }, { db: doomed }, { db: stale }) => {
				victim.putSync('k0', 'v0');
				doomed.dropSync();

				// the poisoning write: `stale` still holds the dropped family
				stale.putSync('k1', 'v1');

				// an unrelated column family is unaffected, synchronously...
				victim.putSync('k2', 'v2');
				expect(victim.getSync('k2')).toBe('v2');

				// ...and transactionally, which is the path Harper's writes take
				await victim.transaction(async (txn: Transaction) => {
					await victim.put('k3', 'v3', { transaction: txn });
				});
				expect(victim.getSync('k3')).toBe('v3');

				// removeSync is a separate write path with its own write options, so
				// poison it independently: a remove through the dropped handle must
				// also be discarded rather than fatal, and must leave the unrelated
				// family's own removes working
				stale.removeSync('k1');
				victim.removeSync('k0');
				expect(victim.getSync('k0')).toBeUndefined();

				// a column family created AFTER the poisoning write is writable too
				const fresh = RocksDatabase.open(dbPath, { name: 'fresh' });
				try {
					fresh.putSync('k4', 'v4');
					expect(fresh.getSync('k4')).toBe('v4');
				} finally {
					fresh.close();
				}
			}
		));

	// Transactions deliberately do NOT get `ignore_missing_column_families`. In
	// optimistic mode - the default -
	// conflict validation rejects a commit naming a dropped family early, with an
	// error that names the family, so the transaction is lost whole rather than
	// discarded in part.
	it('should reject only the dropped-family commit when transactions race a drop', () =>
		dbRunner(
			{ dbOptions: [{ name: 'victim' }, { name: 'doomed' }, { name: 'doomed' }] },
			async ({ db: victim }, { db: doomed }, { db: stale }) => {
				doomed.dropSync();

				const results = await Promise.allSettled([
					stale.transaction(async (txn: Transaction) => {
						await stale.put('a', '1', { transaction: txn });
					}),
					victim.transaction(async (txn: Transaction) => {
						await victim.put('b', '2', { transaction: txn });
					}),
					victim.transaction(async (txn: Transaction) => {
						await victim.put('c', '3', { transaction: txn });
					}),
				]);
				expect(results.map((result) => result.status)).toEqual([
					'rejected',
					'fulfilled',
					'fulfilled',
				]);

				// the rejection identifies the column family it could not reach,
				// instead of the unattributable environment-wide message
				const [staleResult] = results;
				expect(staleResult.status === 'rejected' && staleResult.reason.message).toMatch(
					/Could not access column family \d+/
				);

				expect(victim.getSync('b')).toBe('2');
				expect(victim.getSync('c')).toBe('3');

				// and the environment is still writable afterwards
				victim.putSync('d', '4');
				expect(victim.getSync('d')).toBe('4');
			}
		));

	// The atomicity guard that keeps `ignore_missing_column_families` off the
	// transactional path. A pessimistic transaction sends its batch straight
	// through RocksDB's write path under the transaction's own write options, so
	// setting the flag there would make a commit spanning a live family and a
	// dropped one apply the live half, discard the dropped half, and return OK -
	// a silent partial commit reported as success, which the transaction log
	// would then mark committed.
	//
	// Losing the whole transaction is the correct outcome. (In this mode the
	// commit also poisons the environment on its way out; that is a separate,
	// pre-existing bug tracked as
	// https://github.com/HarperFast/rocksdb-js/issues/726 (needs the drop
	// interlocked against in-flight transactions), so this test asserts only
	// atomicity and verifies that the poisoned environment is reported while
	// still completing native teardown.)
	it('should not partially apply a pessimistic transaction spanning a dropped column family', () =>
		dbRunner(
			{
				dbOptions: [
					{ name: 'victim', pessimistic: true },
					{ name: 'doomed', pessimistic: true },
					{ name: 'doomed', pessimistic: true },
				],
			},
			async ({ db: victim }, { db: doomed }, { db: stale }) => {
				await expect(
					stale.transaction(async (txn: Transaction) => {
						await victim.put('live', 'A', { transaction: txn });
						await stale.put('dead', 'B', { transaction: txn });
						// the drop lands after both writes are staged
						doomed.dropSync();
					})
				).rejects.toThrow();

				// the live half must NOT have been applied
				expect(victim.getSync('live')).toBeUndefined();

				stale.close();
				doomed.close();
				expect(() => victim.close()).toThrow('Failed to flush database during close');
			}
		));

	// Dropping an already-dropped column family must be idempotent. Harper
	// broadcasts drops to all worker threads, so multiple handles to the same
	// shared column family can each issue a drop; RocksDB rejects the second
	// with "Column family already dropped!". The family is gone either way, so
	// the redundant drop is treated as success instead of a failed operation.
	it('should treat dropping an already-dropped column family as a no-op (sync)', () =>
		dbRunner({ dbOptions: [{ name: 'test' }, { name: 'test' }] }, ({ db: db1 }, { db: db2 }) => {
			db1.dropSync();
			expect(db1.columns).toEqual(['default']);
			// db2 still holds the dropped column family; re-dropping must not throw
			expect(() => db2.dropSync()).not.toThrow();
			expect(db2.columns).toEqual(['default']);
		}));

	it('should treat dropping an already-dropped column family as a no-op (async)', () =>
		dbRunner(
			{ dbOptions: [{ name: 'test' }, { name: 'test' }] },
			async ({ db: db1 }, { db: db2 }) => {
				await db1.drop();
				expect(db1.columns).toEqual(['default']);
				// db2 still holds the dropped column family; re-dropping must resolve
				await expect(db2.drop()).resolves.toBeUndefined();
				expect(db2.columns).toEqual(['default']);
			}
		));

	// A stale handle's tolerated (already-dropped) re-drop must NOT unregister a
	// freshly-created column family that reuses the same name. db1 really drops
	// 'test' (and unregisters it); db3 recreates a fresh 'test'; db2 is a stale
	// handle to the original dropped family - its no-op drop must leave db3's
	// fresh family registered and usable, not erase it by name.
	it('should not unregister a freshly-recreated same-name column family on a stale drop', () =>
		dbRunner(
			{ dbOptions: [{ name: 'test' }, { name: 'test' }] },
			({ db: db1, dbPath }, { db: db2 }) => {
				db1.dropSync();
				const db3 = RocksDatabase.open(dbPath, { name: 'test' });
				try {
					db3.putSync('k', 'v');
					expect(db3.columns).toContain('test');
					// stale re-drop of the original family - must be a no-op for the registry
					db2.dropSync();
					// db3's fresh family is untouched: still registered and writable
					expect(db3.columns).toContain('test');
					expect(db3.getSync('k')).toBe('v');
					db3.putSync('k2', 'v2');
					expect(db3.getSync('k2')).toBe('v2');
				} finally {
					db3.close();
				}
			}
		));
});
