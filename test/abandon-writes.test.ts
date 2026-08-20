import { RETRY_NOW, Transaction } from '../src/transaction.ts';
import { dbRunner } from './lib/util.ts';
import { describe, expect, it } from 'vitest';

const settled = <T>(promise: Promise<T>) => {
	let done = false;
	promise.then(
		() => (done = true),
		() => (done = true)
	);
	// Two macrotask turns: enough for a resolved TSFN callback to have run, none
	// for a genuinely parked commit (which waits on another thread's release).
	return new Promise<boolean>((resolve) => setImmediate(() => setImmediate(() => resolve(done))));
};

describe('Transaction.abandonWrites()', () => {
	it('releases held write intents so a parked coordinated-retry commit wakes', () =>
		dbRunner({ dbOptions: [{ verificationTable: true }] }, async ({ db }) => {
			await db.put('hot', 'v0');
			db.populateVersion('hot', 1.5e12); // materialize the VT slot so write intents engage

			// Stages a write (locking the slot's write intent) and never settles — the
			// retained-for-iterators shape from HarperFast/harper#2001.
			const holder = new Transaction(db.store, { coordinatedRetry: true });
			await holder.put('hot', 'held');

			// Snapshots before the conflicting commit below so its own commit is IsBusy.
			const parked = new Transaction(db.store, { coordinatedRetry: true });
			await parked.put('hot', 'parked');

			const conflicting = new Transaction(db.store, { coordinatedRetry: true });
			await conflicting.put('hot', 'conflicting');
			await conflicting.commit();

			const parkedCommit = parked.commit();
			expect(await settled(parkedCommit)).toBe(false);

			holder.abandonWrites();

			expect(await parkedCommit).toBe(RETRY_NOW);
			parked.abort();
			holder.abort();
		}));

	it('keeps an outstanding read iterator working after abandoning', () =>
		dbRunner({ dbOptions: [{ verificationTable: true }] }, async ({ db }) => {
			await db.put('k1', 'v1');
			await db.put('k2', 'v2');
			await db.put('k3', 'v3');

			const txn = new Transaction(db.store);
			await txn.put('k2', 'staged');

			const iterator = txn.getRange()[Symbol.iterator]();
			expect(iterator.next().done).toBe(false);

			txn.abandonWrites();

			let remaining = 0;
			for (let entry = iterator.next(); !entry.done; entry = iterator.next()) {
				remaining++;
			}
			expect(remaining).toBeGreaterThanOrEqual(1);
			txn.abort();
		}));

	it('bars every write and commit entry point but keeps reads working', () =>
		dbRunner(async ({ db }) => {
			await db.put('key', 'committed');
			const txn = new Transaction(db.store);
			await txn.put('key', 'staged');

			txn.abandonWrites();

			expect(await txn.get('key')).toBe('staged'); // read-your-own-writes survives
			await expect(txn.commit()).rejects.toThrow(/abandoned/);
			expect(() => txn.commitSync()).toThrow(/abandoned/);
			await expect(txn.put('key', 'more')).rejects.toThrow(/abandoned/);
			await expect(txn.remove('key')).rejects.toThrow(/abandoned/);
			// Database-context writes reach the same handle through Database::PutSync's
			// txnId branch rather than Transaction::PutSync — an unguarded path there would
			// re-lock the VT slot that was just released.
			expect(() => db.putSync('key', 'via-db', { transaction: txn })).toThrow(/abandoned/);
			expect(() => db.removeSync('key', { transaction: txn })).toThrow(/abandoned/);
			txn.abort();
		}));

	it('is idempotent and a no-op after abort', () =>
		dbRunner(async ({ db }) => {
			const txn = new Transaction(db.store);
			await txn.put('k', 'v');
			txn.abandonWrites();
			txn.abandonWrites();
			txn.abort();
			txn.abandonWrites();
		}));

	it('reports the abort, not the abandonment, when committed after abort — matching async and sync', () =>
		dbRunner(async ({ db }) => {
			const txn = new Transaction(db.store);
			await txn.put('key', 'staged');
			txn.abandonWrites();
			txn.abort();
			// Both must report ERR_ALREADY_ABORTED (the more specific state), not
			// ERR_WRITES_ABANDONED — and must agree with each other, since the
			// db.transaction()/transactionSync() wrappers only special-case
			// TransactionAlreadyAbortedError as an expected user abort.
			await expect(txn.commit()).rejects.toThrow(/already been aborted/);

			const txnSync = new Transaction(db.store);
			txnSync.putSync('key', 'staged');
			txnSync.abandonWrites();
			txnSync.abort();
			expect(() => txnSync.commitSync()).toThrow(/already been aborted/);
		}));
});
