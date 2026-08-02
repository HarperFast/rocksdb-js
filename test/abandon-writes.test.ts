import { RETRY_NOW, Transaction } from '../src/transaction.js';
import { dbRunner } from './lib/util.js';
import { describe, expect, it } from 'vitest';

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe('Transaction.abandonWrites()', () => {
	it('releases held write intents so a parked coordinated-retry commit wakes', () =>
		dbRunner({ dbOptions: [{ verificationTable: true }] }, async ({ db }) => {
			await db.put('hot', 'v0');
			db.populateVersion('hot', 1.5e12); // materialize the VT slot so write intents engage

			// Stages a write (locking the slot's write intent) and never settles —
			// the retained-for-iterators shape from HarperFast/harper#2001.
			const holder = new Transaction(db.store, { coordinatedRetry: true });
			await holder.put('hot', 'held');

			// Snapshots now so the conflicting commit below makes its commit IsBusy.
			const parked = new Transaction(db.store, { coordinatedRetry: true });
			await parked.put('hot', 'parked');

			const conflicting = new Transaction(db.store, { coordinatedRetry: true });
			await conflicting.put('hot', 'conflicting');
			await conflicting.commit();

			// IsBusy under coordinatedRetry: parks on the slot's tracker, which
			// `holder` keeps locked — the commit must not settle on its own.
			const parkedCommit = parked.commit();
			expect(
				await Promise.race([
					parkedCommit.then(() => 'settled'),
					delay(400).then(() => 'still-parked'),
				])
			).toBe('still-parked');

			holder.abandonWrites();

			expect(await parkedCommit).toBe(RETRY_NOW);
			parked.abort();
			holder.abort();
		}));

	it('bars commit and further writes but keeps reads working', () =>
		dbRunner(async ({ db }) => {
			await db.put('key', 'committed');
			const txn = new Transaction(db.store);
			await txn.put('key', 'staged');

			txn.abandonWrites();

			expect(await txn.get('key')).toBe('staged'); // read-your-own-writes survives
			await expect(txn.commit()).rejects.toThrow(/abandoned/);
			await expect(txn.put('key', 'more')).rejects.toThrow(/abandoned/);
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
});
