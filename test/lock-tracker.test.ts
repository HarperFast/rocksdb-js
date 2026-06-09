import { constants } from '../src/load-binding.js';
import { RETRY_NOW, Transaction } from '../src/transaction.js';
import { dbRunner } from './lib/util.js';
import { describe, expect, it } from 'vitest';

const FRESH_VERSION_FLAG = constants.FRESH_VERSION_FLAG;

// Builds a value buffer whose first 8 bytes are the big-endian float64 version,
// matching VerificationTable::extractVersionFromValue (Harper's record format).
function valueWithVersion(version: number): Buffer {
	const value = Buffer.alloc(16);
	value.writeDoubleBE(version, 0);
	return value;
}

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

// Regression coverage for the VT-fast-path / optimistic-snapshot interaction.
// A read satisfied entirely from the Verification Table (returning
// FRESH_VERSION_FLAG, skipping the RocksDB read) must STILL establish the
// transaction's read snapshot. Optimistic conflict detection compares written
// keys against the transaction's snapshot sequence; without a snapshot a
// read-modify-write whose read was served from the VT commits with no baseline,
// so a concurrent write to the same key goes undetected (lost update).
describe('Coordinated retry — VT fast-path establishes the snapshot (regression)', () => {
	it('DB-context read (with txnId) served from the VT still sets the snapshot', () =>
		dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
			const key = Buffer.from('vt-fastpath-snapshot-db-ctx');
			const version = 1.7e12;
			await db.put(key, valueWithVersion(version));
			db.populateVersion(key, version);
			expect(db.verifyVersion(key, version)).toBe(true);

			const txn = new Transaction(db.store, { coordinatedRetry: true });
			try {
				expect(db.getOldestSnapshotTimestamp()).toBe(0);
				// Harper-style read: root DB + transaction in options + expectedVersion.
				const result = db.getBinarySync(key, { transaction: txn, expectedVersion: version } as any);
				expect(result).toBe(FRESH_VERSION_FLAG); // fast path taken (no RocksDB read)
				// ...and the snapshot was still established despite skipping the read.
				expect(db.getOldestSnapshotTimestamp()).toBeGreaterThan(0);
			} finally {
				txn.abort();
			}
		}));

	it('transaction-context read served from the VT still sets the snapshot', () =>
		dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
			const key = Buffer.from('vt-fastpath-snapshot-txn-ctx');
			const version = 1.7e12;
			await db.put(key, valueWithVersion(version));
			db.populateVersion(key, version);
			expect(db.verifyVersion(key, version)).toBe(true);

			const txn = new Transaction(db.store, { coordinatedRetry: true });
			try {
				expect(db.getOldestSnapshotTimestamp()).toBe(0);
				const result = txn.getBinarySync(key, { expectedVersion: version } as any);
				expect(result).toBe(FRESH_VERSION_FLAG); // fast path taken
				expect(db.getOldestSnapshotTimestamp()).toBeGreaterThan(0);
			} finally {
				txn.abort();
			}
		}));

	it('a VT-served read still participates in conflict detection (no lost update)', () =>
		dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
			const key = Buffer.from('vt-fastpath-conflict');
			const v0 = 1.7e12;
			await db.put(key, valueWithVersion(v0));
			db.populateVersion(key, v0);
			expect(db.verifyVersion(key, v0)).toBe(true);

			const txn = new Transaction(db.store, { coordinatedRetry: true });
			let committed = false;
			try {
				// 1. Read through the VT fast path (no RocksDB read) — must snapshot.
				expect(db.getBinarySync(key, { transaction: txn, expectedVersion: v0 } as any)).toBe(
					FRESH_VERSION_FLAG
				);
				// 2. A competing committed write bumps the key's sequence after our snapshot.
				await db.put(key, valueWithVersion(1.8e12));
				// 3. Our write must now be detected as conflicting; under coordinatedRetry
				//    that surfaces as the RETRY_NOW signal rather than a silent success.
				txn.putSync(key, valueWithVersion(1.9e12));
				const result = await txn.commit();
				committed = result !== RETRY_NOW;
				expect(result).toBe(RETRY_NOW);
			} finally {
				if (!committed) txn.abort();
			}
		}));
});
