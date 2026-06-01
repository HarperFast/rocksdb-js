import { RocksDatabase } from '../src/index.js';
import { constants } from '../src/load-binding.js';
import { Transaction } from '../src/transaction.js';
import { dbRunner, generateDBPath } from './lib/util.js';
import { describe, expect, it } from 'vitest';

const { POPULATE_VERSION_FLAG, FRESH_VERSION_FLAG } = constants;

describe('Verification Table', () => {
	describe('verifyVersion() / populateVersion()', () => {
		it('returns false on an unseeded slot', () =>
			dbRunner(async ({ db }) => {
				expect(db.verifyVersion('never-written', 1.5e12)).toBe(false);
			}));

		it('returns true after populateVersion with the same version', () =>
			dbRunner(async ({ db }) => {
				const version = 1700000000000;
				db.populateVersion('foo', version);
				expect(db.verifyVersion('foo', version)).toBe(true);
			}));

		it('returns false after populateVersion with a different version', () =>
			dbRunner(async ({ db }) => {
				db.populateVersion('foo', 1700000000000);
				expect(db.verifyVersion('foo', 1700000000001)).toBe(false);
			}));

		it('overwrites a prior version', () =>
			dbRunner(async ({ db }) => {
				db.populateVersion('foo', 1.0e12);
				db.populateVersion('foo', 2.0e12);
				expect(db.verifyVersion('foo', 1.0e12)).toBe(false);
				expect(db.verifyVersion('foo', 2.0e12)).toBe(true);
			}));

		it('isolates entries by key', () =>
			dbRunner(async ({ db }) => {
				db.populateVersion('alpha', 1.1e12);
				db.populateVersion('beta', 2.2e12);
				expect(db.verifyVersion('alpha', 1.1e12)).toBe(true);
				expect(db.verifyVersion('alpha', 2.2e12)).toBe(false);
				expect(db.verifyVersion('beta', 2.2e12)).toBe(true);
				expect(db.verifyVersion('beta', 1.1e12)).toBe(false);
			}));

		it('treats version 0 as not-fresh', () =>
			dbRunner(async ({ db }) => {
				// 0 means "empty/unknown" in the slot encoding. Populating with 0
				// is a no-op; verifying against 0 always returns false.
				db.populateVersion('foo', 0);
				expect(db.verifyVersion('foo', 0)).toBe(false);
				db.populateVersion('foo', 1.5e12);
				expect(db.verifyVersion('foo', 0)).toBe(false);
				expect(db.verifyVersion('foo', 1.5e12)).toBe(true);
			}));

		it('isolates entries between different databases', async () => {
			await dbRunner(
				{
					dbOptions: [{}, { path: generateDBPath() }],
				},
				async ({ db: db1 }, { db: db2 }) => {
					db1.populateVersion('foo', 1.5e12);
					// Same key in a different DB hashes to a different slot (via the
					// db pointer mixed into the hash). With overwhelmingly high
					// probability the slot is still empty.
					expect(db2.verifyVersion('foo', 1.5e12)).toBe(false);
				}
			);
		});
	});

	describe('getSync() with expectedVersion fast path', () => {
		it('returns FRESH_VERSION_FLAG when slot matches', () =>
			dbRunner(async ({ db }) => {
				const key = Buffer.from('hot-key');
				const version = 1.7e12;
				db.populateVersion(key, version);

				// Bypass the Store wrapper to access the native getSync directly
				// with the optional expectedVersion 4th arg. This is the path Harper
				// will use after a JS-side WeakMap cache hit.
				const native = (db as any).store.db;
				const result = native.getSync(key, 0, undefined, version);
				expect(result).toBe(FRESH_VERSION_FLAG);
			}));

		it('falls through to a normal read when slot does not match', () =>
			dbRunner({ dbOptions: [{ encoding: false }] }, async ({ db }) => {
				const key = 'cold-key';
				const value = Buffer.alloc(16);
				value.writeDoubleBE(1.7e12, 0);
				value.writeUInt32BE(0xdeadbeef, 8);
				await db.put(key, value);

				const native = (db as any).store.db;
				// expectedVersion does NOT match what's in the slot (slot is empty);
				// getSync falls through to a normal read and returns the value.
				const result = native.getSync(Buffer.from(key), 0, undefined, 9.9e12);
				expect(result).not.toBe(FRESH_VERSION_FLAG);
				expect(result).toBeDefined();
			}));

		it('seeds the slot when POPULATE_VERSION_FLAG is set on a successful read', () =>
			dbRunner({ dbOptions: [{ encoding: false }] }, async ({ db }) => {
				const key = 'populate-key';
				const valueVersion = 1.7e12;
				const value = Buffer.alloc(16);
				value.writeDoubleBE(valueVersion, 0);
				value.writeUInt32BE(0xdeadbeef, 8);
				await db.put(key, value);

				expect(db.verifyVersion(key, valueVersion)).toBe(false);

				const native = (db as any).store.db;
				native.getSync(Buffer.from(key), POPULATE_VERSION_FLAG, undefined, undefined);

				expect(db.verifyVersion(key, valueVersion)).toBe(true);
			}));
	});

	describe('config({ verificationTableEntries })', () => {
		it('throws when set to a negative value', () => {
			expect(() => RocksDatabase.config({ verificationTableEntries: -1 })).toThrowError(
				'Verification table entries must be a positive integer or 0 to disable verification'
			);
		});

		it('throws if changed after the table is materialized', () =>
			dbRunner(async ({ db }) => {
				// Touching populateVersion materializes the verification table.
				db.populateVersion('any-key', 1e12);
				expect(() => RocksDatabase.config({ verificationTableEntries: 64 })).toThrowError(
					'Verification table size cannot be changed after the first database is opened'
				);
			}));
	});

	// The single-accessible-version invariant: a slot may only hold a fresh
	// (cacheable) version when no read snapshot older than that version is still
	// open. Otherwise an older snapshot sees a prior value while the latest is
	// different — two accessible versions — and a fresh cache hit would violate
	// that reader's snapshot. Gated in vtPopulateIfSettled via the oldest active
	// snapshot's wall-clock time. (Versions here are ms timestamps; we use ±10s
	// to stay well outside the second-granularity comparison.)
	describe('snapshot-gated populate (single-version invariant)', () => {
		it('suppresses publishing a version newer than an open snapshot, allows one older', () =>
			dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
				// Open a read transaction to establish a live snapshot at ~now.
				const txn = new Transaction(db.store);
				txn.getBinarySync(Buffer.from('open-snapshot-anchor')); // forces SetSnapshot
				try {
					expect(db.getOldestSnapshotTimestamp()).toBeGreaterThan(0);
					const nowSec = Math.floor(Date.now() / 1000);

					// Older than the open snapshot → every snapshot already sees it
					// (single accessible version) → publish allowed.
					const settled = (nowSec - 10) * 1000;
					db.populateVersion(Buffer.from('settled'), settled);
					expect(db.verifyVersion(Buffer.from('settled'), settled)).toBe(true);

					// Newer than the open snapshot → that snapshot still sees a prior
					// value (two accessible versions) → publish suppressed.
					const unsettled = (nowSec + 10) * 1000;
					db.populateVersion(Buffer.from('unsettled'), unsettled);
					expect(db.verifyVersion(Buffer.from('unsettled'), unsettled)).toBe(false);
				} finally {
					txn.abort();
				}
			}));

		it('publishes freely when no snapshots are open', () =>
			dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
				expect(db.getOldestSnapshotTimestamp()).toBe(0);
				// With no open snapshots there is a single accessible version, so even
				// a just-written version is immediately cacheable.
				const v = (Math.floor(Date.now() / 1000) + 10) * 1000;
				db.populateVersion(Buffer.from('no-snap'), v);
				expect(db.verifyVersion(Buffer.from('no-snap'), v)).toBe(true);
			}));
	});
});
