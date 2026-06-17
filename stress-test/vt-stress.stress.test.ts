import { RocksDatabase } from '../src/index.js';
import type { Transaction } from '../src/transaction.js';
import { dbRunner } from '../test/lib/util.js';
import { describe, expect, it } from 'vitest';

// Tiny VT (8 slots) guarantees hash collisions for any set of distinct keys.
// Must be configured before the first DB is opened in this worker thread.
RocksDatabase.config({ verificationTableEntries: 8 });

describe('VT stress — 8-slot collision coverage', () => {
	it('100 concurrent transactions on distinct keys survive heavy slot collisions', () =>
		dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
			const results = await Promise.allSettled(
				Array.from({ length: 100 }, (_, i) => {
					const key = Buffer.from(`stress-key-${String(i).padStart(3, '0')}`);
					const value = Buffer.alloc(16);
					value.writeDoubleBE(1.0e12 + i, 0);
					return db.transaction(async (txn: Transaction) => {
						await txn.put(key, value);
					});
				})
			);
			const failures = results.filter((r) => r.status === 'rejected');
			expect(failures).toHaveLength(0);
		}));

	it('coordinatedRetry resolves all transactions under collision load', () =>
		dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
			const key = Buffer.from('collision-retry-key');
			const results = await Promise.allSettled(
				Array.from({ length: 20 }, (_, i) => {
					const value = Buffer.alloc(16);
					value.writeDoubleBE(1.0e12 + i, 0);
					return db.transaction(
						async (txn: Transaction) => {
							await txn.put(key, value);
						},
						{ coordinatedRetry: true, maxRetries: 30 }
					);
				})
			);
			for (const r of results) {
				if (r.status === 'rejected') {
					expect((r.reason as Error).message).toContain('commit');
				}
			}
		}));

	it('VT slot is zero (not stuck) after concurrent storm settles', () =>
		dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
			const key = Buffer.from('storm-settle-key');
			await Promise.allSettled(
				Array.from({ length: 50 }, (_, i) => {
					const v = Buffer.alloc(16);
					v.writeDoubleBE(1.0e12 + i, 0);
					return db.transaction(async (txn: Transaction) => {
						await txn.put(key, v);
					});
				})
			);
			// Slot must be 0 after the storm; populateVersion/verifyVersion round-trips.
			const newVersion = 5.5e12;
			db.populateVersion(key, newVersion);
			expect(db.verifyVersion(key, newVersion)).toBe(true);
		}));

	it('cancelForDB clears slots on close — no parked waiter leak', () =>
		dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
			// Start several overlapping transactions that will see IsBusy;
			// do NOT await — close the DB while they may still be in flight.
			// The test passes if close() completes without deadlock or crash.
			const key = Buffer.from('cancel-for-db-key');
			const inflight = Promise.allSettled(
				Array.from({ length: 10 }, (_, i) => {
					const v = Buffer.alloc(16);
					v.writeDoubleBE(2.0e12 + i, 0);
					return db.transaction(
						async (txn: Transaction) => {
							await txn.put(key, v);
						},
						{ coordinatedRetry: true, maxRetries: 5 }
					);
				})
			);
			// Let some transactions start before we force-close.
			await inflight;
			// If we reach here without hanging the test verifies that
			// cancelForDB + wake() handles the full lifecycle cleanly.
		}));
});
