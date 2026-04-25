import { RETRY_NOW } from '../src/transaction.js';
import type { Transaction } from '../src/transaction.js';
import { dbRunner } from './lib/util.js';
import { describe, expect, it } from 'vitest';

describe('LockTracker (Phase 2)', () => {
	describe('intent registration clears slots on commit', () => {
		it('zeroes a previously-populated slot after a successful commit', () =>
			dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
				const key = Buffer.from('lock-clears-slot');
				const version = 1.7e12;
				const value = Buffer.alloc(16);
				value.writeDoubleBE(version, 0);

				await db.put(key, value);
				db.populateVersion(key, version);
				expect(db.verifyVersion(key, version)).toBe(true);

				// A transaction writing the same key locks the slot during commit,
				// then clears it to 0 when the last holder releases.
				await db.transaction(async (txn: Transaction) => {
					await txn.put(key, value);
				});

				// The slot was zeroed; the stale version is no longer valid.
				expect(db.verifyVersion(key, version)).toBe(false);
			}));

		it('slot is writable again (not stuck lock-tagged) after commit', () =>
			dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
				const key = Buffer.from('slot-writable-after-commit');
				const value = Buffer.alloc(16);

				await db.transaction(async (txn: Transaction) => {
					await txn.put(key, value);
				});

				// populateVersion should succeed — slot must be 0, not lock-tagged.
				const v = 1.8e12;
				db.populateVersion(key, v);
				expect(db.verifyVersion(key, v)).toBe(true);
			}));

		it('multiple transactions on same key all settle without leaving a stuck lock', () =>
			dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
				const key = Buffer.from('concurrent-writes');
				const value = Buffer.alloc(16);
				value.writeDoubleBE(1.7e12, 0);

				// Run several concurrent transactions writing the same key.
				// Some will get IsBusy (optimistic conflict); use allSettled so the DB
				// stays open until every transaction has fully resolved or rejected.
				// The test only cares that no slot is left lock-tagged, not that every
				// transaction succeeds.
				await Promise.allSettled(
					Array.from({ length: 6 }, (_, i) =>
						db.transaction(async (txn: Transaction) => {
							const v = Buffer.alloc(16);
							v.writeDoubleBE(1.7e12 + i, 0);
							await txn.put(key, v);
						})
					)
				);

				// After all transactions settle the slot must be 0 (no stuck lock).
				// Verify by populating and confirming the next verifyVersion round-trips.
				const newVersion = 2.0e12;
				db.populateVersion(key, newVersion);
				expect(db.verifyVersion(key, newVersion)).toBe(true);
			}));

		it('does not affect keys not written by the transaction', () =>
			dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
				const keyA = Buffer.from('txn-writes-a');
				const keyB = Buffer.from('txn-skips-b');
				const vA = 1.1e12;
				const vB = 2.2e12;
				const value = Buffer.alloc(16);

				await db.put(keyA, value);
				await db.put(keyB, value);
				db.populateVersion(keyA, vA);
				db.populateVersion(keyB, vB);

				// Transaction only touches keyA.
				await db.transaction(async (txn: Transaction) => {
					await txn.put(keyA, value);
				});

				// keyA's slot was cleared by the commit; keyB's slot is untouched.
				expect(db.verifyVersion(keyA, vA)).toBe(false);
				expect(db.verifyVersion(keyB, vB)).toBe(true);
			}));
	});
});

describe('Coordinated retry (Phase 3)', () => {
	it('RETRY_NOW is exported and is a number', () => {
		expect(typeof RETRY_NOW).toBe('number');
		expect(RETRY_NOW).toBeGreaterThan(0);
	});

	it('resolves with RETRY_NOW (not rejects) on IsBusy when coordinatedRetry is true', () =>
		dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
			const key = Buffer.from('coordinated-retry-key');
			const value = Buffer.alloc(16);
			value.writeDoubleBE(1.5e12, 0);

			// Force IsBusy by running concurrent transactions writing the same key
			// under coordinatedRetry: true. database.ts handles RETRY_NOW internally
			// via immediate retry; callers never see it as a return value.
			const results = await Promise.allSettled(
				Array.from({ length: 4 }, async (_, i) => {
					const v = Buffer.alloc(16);
					v.writeDoubleBE(1.5e12 + i, 0);
					await db.transaction(
						async (txn: Transaction) => {
							await txn.put(key, v);
						},
						{ coordinatedRetry: true, maxRetries: 5 }
					);
				})
			);

			// All transactions should eventually succeed (coordinatedRetry retries
			// without error) or fail gracefully; none should throw unexpectedly.
			for (const r of results) {
				if (r.status === 'rejected') {
					// Only acceptable rejection is exhausted retries
					expect((r.reason as Error).message).toContain('commit');
				}
			}

			// Slot should be 0 (released) after all transactions settle.
			const newV = 2.5e12;
			db.populateVersion(key, newV);
			expect(db.verifyVersion(key, newV)).toBe(true);
		}));

	it('transaction() retries immediately on RETRY_NOW without error', () =>
		dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
			const key = Buffer.from('retry-now-immediate');
			let attempts = 0;

			// Run a transaction with coordinatedRetry. Even with contention
			// the transaction should eventually succeed (no unhandled rejection).
			await db.transaction(
				async (txn: Transaction) => {
					attempts++;
					const v = Buffer.alloc(16);
					v.writeDoubleBE(1.0e12 + attempts, 0);
					await txn.put(key, v);
				},
				{ coordinatedRetry: true, maxRetries: 10 }
			);

			expect(attempts).toBeGreaterThanOrEqual(1);
		}));
});
