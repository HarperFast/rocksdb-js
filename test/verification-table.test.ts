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
					// The db pointer is mixed into the slot hash, so the same key in two
					// different databases maps to different slots. Populate many keys in
					// db1 and confirm db2 sees (almost) none of them as fresh.
					//
					// Asserted statistically rather than on a single key: the VT is
					// process-global with a fixed slot count, so a stray hash collision —
					// or a freed database's address being reused for db2 with leftover slot
					// state — can make an individual key spuriously verify. That is by
					// design (collisions cause false invalidations, never incorrect
					// results). Per-key distinct versions avoid cross-test contamination,
					// and the threshold still fails loudly if the db pointer were NOT mixed
					// into the hash — then every key would leak.
					const N = 100;
					let leaked = 0;
					for (let i = 0; i < N; i++) {
						const key = `cross-db-iso-${i}`;
						const version = 1.5e12 + i;
						db1.populateVersion(key, version);
						if (db2.verifyVersion(key, version)) leaked++;
					}
					expect(leaked).toBeLessThan(N / 4);
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

	// The single-accessible-version invariant: a getSync populate may only mark a
	// slot fresh when the key's LATEST version is the single accessible value —
	// no read snapshot older than that latest write is still open. This is gated
	// in vtPopulateIfSettled, which reads the latest version itself (so a
	// transactional/snapshot read cannot publish a stale value) and compares the
	// oldest active snapshot's wall-clock time against the version's ms timestamp.
	// (db.populateVersion is the separate low-level primitive and is NOT gated.)
	const makeValue = (versionMs: number): Buffer => {
		const value = Buffer.alloc(16);
		value.writeDoubleBE(versionMs, 0);
		return value;
	};

	describe('snapshot-gated populate (single-version invariant)', () => {
		it('a transactional read seeds the VT when the key is settled', () =>
			dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
				const key = Buffer.from('txn-seeds');
				const version = 1.7e12; // a settled (well in the past) version
				await db.put(key, makeValue(version));
				expect(db.verifyVersion(key, version)).toBe(false); // not seeded yet

				// A read INSIDE a transaction must still seed the cache when settled —
				// this is the whole point: Harper's reads are transactional.
				await db.transaction(async (txn: Transaction) => {
					txn.getBinarySync(key, { populateVersion: true } as any);
				});

				expect(db.verifyVersion(key, version)).toBe(true);
			}));

		it('suppresses seeding while a snapshot older than the latest version is open', () =>
			dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
				const key = Buffer.from('gated');
				// Craft a version timestamp in the future so it is unambiguously newer
				// than the snapshot we open now (second-granularity safe).
				const futureVersion = (Math.floor(Date.now() / 1000) + 30) * 1000;
				await db.put(key, makeValue(futureVersion));

				// Open a read snapshot now — older than the (future-dated) latest write.
				const snap = new Transaction(db.store);
				snap.getBinarySync(Buffer.from('anchor')); // forces SetSnapshot
				try {
					expect(db.getOldestSnapshotTimestamp()).toBeGreaterThan(0);
					// A non-transactional populate read: vtPopulateIfSettled sees an
					// open snapshot older than the latest version → suppresses.
					const native = (db as any).store.db;
					native.getSync(key, POPULATE_VERSION_FLAG, undefined, undefined);
					expect(db.verifyVersion(key, futureVersion)).toBe(false);
				} finally {
					snap.abort();
				}

				// Once the snapshot closes, a populate read settles the slot.
				const native = (db as any).store.db;
				native.getSync(key, POPULATE_VERSION_FLAG, undefined, undefined);
				expect(db.verifyVersion(key, futureVersion)).toBe(true);
			}));

		// FIX D: Sequence-number gate for backdated replicated versions.
		// A version whose wall-clock timestamp predates the oldest open snapshot's
		// creation time passes Gate 1 (the original wall-clock check), but if that
		// version was written locally AFTER the snapshot was taken (i.e. its write
		// sequence number > oldest_snapshot_seq), the snapshot cannot see it.
		// Gate 2 (sequence-number check) blocks publication in that case.
		//
		// This case uses a sentinel write so the snapshot sits at sequence >= 1;
		// the following test covers a snapshot taken at sequence 0 (fresh DB).
		it('suppresses seeding for backdated version written after the open snapshot (FIX D)', () =>
			dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
				const key = Buffer.from('backdated-key');
				// A very old timestamp (2001-09-09) that pre-dates any plausible local
				// snapshot creation time → Gate 1 passes for this version, even with
				// a snapshot open right now.
				const backdatedVersion = 1.0e12;

				// Sentinel write so the snapshot is taken at a non-zero sequence.
				await db.put(Buffer.from('sentinel'), makeValue(1.5e12));

				// Open a snapshot to pin the current DB sequence (S1 >= 1).
				const snap = new Transaction(db.store);
				snap.getBinarySync(Buffer.from('sentinel')); // forces SetSnapshot

				try {
					// Write the backdated version AFTER the snapshot is open (seq S2 > S1).
					// This models a replicated write: origin timestamp is old but the local
					// apply sequence is newer than the open snapshot.
					await db.put(key, makeValue(backdatedVersion));

					// Attempt to seed the VT. Gate 1 would pass (backdatedVersion << now).
					// Gate 2 must block because oldest_snapshot_seq (S1) < latest_seq (S2).
					const native = (db as any).store.db;
					native.getSync(key, POPULATE_VERSION_FLAG, undefined, undefined);
					expect(db.verifyVersion(key, backdatedVersion)).toBe(false);
				} finally {
					snap.abort();
				}

				// After snapshot drains, the sequence gate passes and the slot settles.
				const native = (db as any).store.db;
				native.getSync(key, POPULATE_VERSION_FLAG, undefined, undefined);
				expect(db.verifyVersion(key, backdatedVersion)).toBe(true);
			}));

		// A snapshot taken on a fresh DB legitimately has sequence 0; Gate 2 must
		// still protect it (no `oldestSnapshotSeq != 0` exemption — a backdated
		// write at seq > 0 would otherwise publish past the seq-0 snapshot).
		it('suppresses seeding for a backdated write past a snapshot taken at sequence 0 (FIX D)', () =>
			dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
				const key = Buffer.from('backdated-seq0');
				const backdatedVersion = 1.0e12;

				// No sentinel write: the snapshot is taken on the fresh DB at seq 0.
				const snap = new Transaction(db.store);
				snap.getBinarySync(Buffer.from('missing')); // forces SetSnapshot at seq 0

				try {
					// Backdated write after the snapshot (seq > 0).
					await db.put(key, makeValue(backdatedVersion));

					const native = (db as any).store.db;
					native.getSync(key, POPULATE_VERSION_FLAG, undefined, undefined);
					expect(db.verifyVersion(key, backdatedVersion)).toBe(false);
				} finally {
					snap.abort();
				}

				// Snapshot drained → publication resumes.
				const native = (db as any).store.db;
				native.getSync(key, POPULATE_VERSION_FLAG, undefined, undefined);
				expect(db.verifyVersion(key, backdatedVersion)).toBe(true);
			}));
	});

	// FIX A: clear() must invalidate the VT.
	describe('VT invalidation on clear()', () => {
		it('verifyVersion returns false after clearSync() on a VT-enabled store', () =>
			dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
				const key = Buffer.from('clear-me');
				const version = 1.7e12;
				await db.put(key, makeValue(version));

				// Seed the VT slot with the version.
				const native = (db as any).store.db;
				native.getSync(key, POPULATE_VERSION_FLAG, undefined, undefined);
				expect(db.verifyVersion(key, version)).toBe(true);

				// Synchronous clear: VT sweep must follow the data delete.
				db.clearSync();

				// Slot should be advanced to settled-empty; stale version no longer fresh.
				expect(db.verifyVersion(key, version)).toBe(false);
			}));

		it('verifyVersion returns false after async clear() on a VT-enabled store', () =>
			dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
				const key = Buffer.from('clear-me-async');
				const version = 1.71e12;
				await db.put(key, makeValue(version));

				const native = (db as any).store.db;
				native.getSync(key, POPULATE_VERSION_FLAG, undefined, undefined);
				expect(db.verifyVersion(key, version)).toBe(true);

				await db.clear();

				expect(db.verifyVersion(key, version)).toBe(false);
			}));

		it('a stale expectedVersion read after clearSync does not return FRESH', () =>
			dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
				const key = Buffer.from('stale-after-clear');
				const version = 1.72e12;
				await db.put(key, makeValue(version));

				const native = (db as any).store.db;
				// Seed the slot.
				native.getSync(key, POPULATE_VERSION_FLAG, undefined, undefined);
				expect(db.verifyVersion(key, version)).toBe(true);

				db.clearSync();

				// A getSync with expectedVersion (the harper "check cache freshness" path)
				// must fall through to a DB read (key not found), not return FRESH.
				const result = native.getSync(key, 0, undefined, version);
				expect(result).not.toBe(FRESH_VERSION_FLAG);
				expect(result).toBeUndefined();
			}));
	});

	// Drop of a non-default column family bulk-deletes like clear() and must
	// sweep the VT the same way (default-CF drop routes to clear(), covered above).
	describe('VT invalidation on drop of a non-default column family', () => {
		it('verifyVersion returns false after dropSync()', () =>
			dbRunner(
				{ dbOptions: [{ name: 'droppable', encoding: false, verificationTable: true }] },
				async ({ db }) => {
					const key = Buffer.from('drop-me');
					const version = 1.73e12;
					await db.put(key, makeValue(version));

					const native = (db as any).store.db;
					native.getSync(key, POPULATE_VERSION_FLAG, undefined, undefined);
					expect(db.verifyVersion(key, version)).toBe(true);

					db.dropSync();

					expect(db.verifyVersion(key, version)).toBe(false);
				}
			));

		it('verifyVersion returns false after async drop()', () =>
			dbRunner(
				{ dbOptions: [{ name: 'droppable-async', encoding: false, verificationTable: true }] },
				async ({ db }) => {
					const key = Buffer.from('drop-me-async');
					const version = 1.74e12;
					await db.put(key, makeValue(version));

					const native = (db as any).store.db;
					native.getSync(key, POPULATE_VERSION_FLAG, undefined, undefined);
					expect(db.verifyVersion(key, version)).toBe(true);

					await db.drop();

					expect(db.verifyVersion(key, version)).toBe(false);
				}
			));
	});

	// FIX B: non-transactional putSync/removeSync must invalidate the VT.
	describe('VT invalidation on non-transactional putSync/removeSync', () => {
		it('putSync advances the slot so a stale expectedVersion is no longer FRESH', () =>
			dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
				const key = Buffer.from('non-txn-put');
				const v1 = 1.7e12;
				const v2 = 1.8e12;
				await db.put(key, makeValue(v1));

				const native = (db as any).store.db;
				// Seed the slot with v1.
				native.getSync(key, POPULATE_VERSION_FLAG, undefined, undefined);
				expect(db.verifyVersion(key, v1)).toBe(true);

				// Non-transactional putSync with a new version.
				native.putSync(key, makeValue(v2));

				// v1 must no longer verify as FRESH (slot advanced by the write).
				expect(db.verifyVersion(key, v1)).toBe(false);

				// A getSync with stale expectedVersion must NOT return FRESH_VERSION_FLAG.
				const result = native.getSync(key, 0, undefined, v1);
				expect(result).not.toBe(FRESH_VERSION_FLAG);
			}));

		it('removeSync advances the slot so a stale expectedVersion is no longer FRESH', () =>
			dbRunner({ dbOptions: [{ encoding: false, verificationTable: true }] }, async ({ db }) => {
				const key = Buffer.from('non-txn-remove');
				const v1 = 1.7e12;
				await db.put(key, makeValue(v1));

				const native = (db as any).store.db;
				native.getSync(key, POPULATE_VERSION_FLAG, undefined, undefined);
				expect(db.verifyVersion(key, v1)).toBe(true);

				// Non-transactional removeSync invalidates the slot.
				native.removeSync(key);

				expect(db.verifyVersion(key, v1)).toBe(false);

				// Key is gone and slot is settled-empty; FRESH_VERSION_FLAG must NOT fire.
				const result = native.getSync(key, 0, undefined, v1);
				expect(result).not.toBe(FRESH_VERSION_FLAG);
				expect(result).toBeUndefined();
			}));
	});
});
