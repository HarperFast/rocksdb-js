import {
	FRESH_VERSION_FLAG,
	HAS_DISTINCT_VERSION_FLAG,
	RocksDatabase,
	type Transaction,
} from '../src/index.ts';
import { generateDBPath } from './lib/util.ts';
import { rmSync } from 'node:fs';
import { afterEach, describe, expect, it } from 'vitest';

/**
 * Commit-time local mutation stamping (dual-clock stage 1, #811).
 *
 * On an enabled column family, rocksdb-js owns the first 8 bytes of every
 * value: at commit a receiver-local monotonic stamp is claimed
 * (keep-if-greater against the database watermark) and lands identically in
 * every stamped record's first word and as the transaction-log batch key.
 * DORMANT by default — the dormant assertions here pin that no bytes change
 * without the option.
 */

const paths: string[] = [];

function dbPath(): string {
	const path = generateDBPath();
	paths.push(path);
	return path;
}

function openStamped(path: string, extra?: Record<string, unknown>): RocksDatabase {
	return RocksDatabase.open(path, {
		commitStamping: true,
		encoding: 'binary',
		...extra,
	});
}

function stampedValue(word: number, payload: string): Buffer {
	const value = Buffer.alloc(8 + payload.length);
	value.writeDoubleBE(word, 0);
	value.write(payload, 8);
	return value;
}

function firstWord(value: Buffer): number {
	return value.readDoubleBE(0);
}

afterEach(() => {
	for (const path of paths.splice(0)) {
		try {
			rmSync(path, { recursive: true, force: true });
		} catch {
			// best effort
		}
	}
});

describe('commit stamping', () => {
	it('is dormant without the option: bytes round-trip unchanged', () => {
		const db = RocksDatabase.open(dbPath(), { encoding: 'binary' });
		try {
			const value = stampedValue(123.456, 'dormant');
			db.transactionSync((txn: Transaction) => {
				txn.putSync('k', value);
			});
			const raw = db.getSync('k') as Buffer;
			expect(Buffer.compare(raw, value)).toBe(0);
			expect(firstWord(raw)).toBe(123.456);
		} finally {
			db.close();
		}
	});

	it('stamps record first words with the commit stamp on the keep path', () => {
		const db = openStamped(dbPath());
		try {
			let txnTimestamp = 0;
			let committed: number | undefined;
			db.transactionSync((txn: Transaction) => {
				txnTimestamp = txn.getTimestamp();
				txn.putSync('a', stampedValue(0, 'alpha'));
				txn.putSync('b', stampedValue(0, 'beta'));
			});

			const a = db.getSync('a') as Buffer;
			const b = db.getSync('b') as Buffer;
			// Keep path: the txn's own monotonic timestamp is above the watermark
			// and becomes the stamp of every record in the commit.
			expect(firstWord(a)).toBe(txnTimestamp);
			expect(firstWord(b)).toBe(txnTimestamp);
			expect(a.subarray(8).toString()).toBe('alpha');
		} finally {
			db.close();
		}
	});

	it('exposes the finalized stamp via getCommittedLocalTime', () => {
		const db = openStamped(dbPath());
		try {
			const txn = db.transactionSync((t: Transaction): Transaction => {
				t.putSync('k', stampedValue(0, 'x'));
				expect(t.getCommittedLocalTime()).toBeUndefined();
				return t;
			}) as Transaction;
			const stamp = txn.getCommittedLocalTime();
			expect(stamp).toBeTypeOf('number');
			expect(stamp).toBe(firstWord(db.getSync('k') as Buffer));
		} finally {
			db.close();
		}
	});

	it('re-stamps a caller timestamp at/below the watermark (replicated-apply shape)', () => {
		const db = openStamped(dbPath());
		try {
			// Advance the watermark with a normal commit.
			db.transactionSync((txn: Transaction) => {
				txn.putSync('w', stampedValue(0, 'wm'));
			});
			const watermark = firstWord(db.getSync('w') as Buffer);

			// An origin-timestamped apply below the watermark must NOT keep it:
			// the record first word becomes a fresh receiver-local stamp.
			const originTime = 1000000.5;
			const txn = db.transactionSync((t: Transaction): Transaction => {
				t.setTimestamp(originTime);
				t.putSync('r', stampedValue(0, 'replicated'));
				return t;
			}) as Transaction;
			const stamped = firstWord(db.getSync('r') as Buffer);
			expect(stamped).not.toBe(originTime);
			expect(stamped).toBeGreaterThan(watermark);
			expect(txn.getCommittedLocalTime()).toBe(stamped);
		} finally {
			db.close();
		}
	});

	it('keys the transaction-log batch with the stamp (stamp-as-key)', () => {
		const db = openStamped(dbPath(), { transactionLogsPath: `${paths[paths.length - 1]}-logs` });
		try {
			const log = db.useLog('audit');
			// Keep-path commit: batch key == txn timestamp == stamp.
			let keepTimestamp = 0;
			db.transactionSync((txn: Transaction) => {
				keepTimestamp = txn.getTimestamp();
				txn.putSync('k1', stampedValue(0, 'one'));
				log.addEntry(Buffer.from('entry-one'), txn.id);
			});
			// Re-stamp commit: origin time below watermark; the batch key must be
			// the fresh receiver stamp, not the origin time (the key domain flips
			// at activation), and the record word must match it exactly.
			const originTime = 1000.25;
			const txn = db.transactionSync((t: Transaction): Transaction => {
				t.setTimestamp(originTime);
				t.putSync('k2', stampedValue(0, 'two'));
				log.addEntry(Buffer.from('entry-two'), t.id);
				return t;
			}) as Transaction;

			const entries = [...log.query({ start: 1 })];
			expect(entries.length).toBe(2);
			expect(entries[0].timestamp).toBe(keepTimestamp);
			expect(entries[1].timestamp).toBe(txn.getCommittedLocalTime());
			expect(entries[1].timestamp).toBe(firstWord(db.getSync('k2') as Buffer));
			expect(entries[1].timestamp).toBeGreaterThan(entries[0].timestamp);
			expect(entries[1].timestamp).not.toBe(originTime);
		} finally {
			db.close();
		}
	});

	it('rejects values shorter than 8 bytes on a stamped column family', () => {
		const db = openStamped(dbPath());
		try {
			expect(() =>
				db.transactionSync((txn: Transaction) => {
					txn.putSync('short', Buffer.from('tiny'));
				})
			).toThrow(/at least 8 bytes/);
			expect(() => db.putSync('short2', Buffer.from('tiny'))).toThrow(/at least 8 bytes/);
		} finally {
			db.close();
		}
	});

	it('does not mutate the caller buffer when staging the stamp', () => {
		const db = openStamped(dbPath());
		try {
			const value = stampedValue(42.5, 'shared');
			const snapshot = Buffer.from(value);
			db.transactionSync((txn: Transaction) => {
				txn.putSync('k', value);
				expect(Buffer.compare(value, snapshot)).toBe(0);
			});
			expect(Buffer.compare(value, snapshot)).toBe(0);
			expect(firstWord(db.getSync('k') as Buffer)).not.toBe(42.5);
		} finally {
			db.close();
		}
	});

	it('stamps direct (non-transactional) putSync writes', () => {
		const db = openStamped(dbPath());
		try {
			const before = Date.now() - 1000;
			db.putSync('direct', stampedValue(0, 'direct'));
			const stamp = firstWord(db.getSync('direct') as Buffer);
			expect(stamp).toBeGreaterThan(before);
			expect(stamp).toBeLessThan(8.64e15);
		} finally {
			db.close();
		}
	});

	it('defines committedLocalTime for delete-only transactions', () => {
		const db = openStamped(dbPath());
		try {
			db.putSync('gone', stampedValue(0, 'x'));
			const txn = db.transactionSync((t: Transaction): Transaction => {
				t.removeSync('gone');
				return t;
			}) as Transaction;
			expect(txn.getCommittedLocalTime()).toBeTypeOf('number');
			expect(db.getSync('gone')).toBeUndefined();
		} finally {
			db.close();
		}
	});

	it('getEntry exposes both words per the value-header contract', () => {
		const db = openStamped(dbPath());
		try {
			db.transactionSync((txn: Transaction) => {
				txn.putSync('plain', stampedValue(0, 'no-metadata'));
			});
			const plain = db.getEntrySync('plain');
			expect(plain?.localTime).toBeTypeOf('number');
			expect(plain?.version).toBe(plain?.localTime);

			// A value carrying the metadata word with HAS_DISTINCT_VERSION_FLAG and
			// the distinct version word at offset 12 (what harper stage 2 writes).
			const distinctVersion = 1234567890.75;
			const value = Buffer.alloc(20 + 4);
			value.writeUInt32BE((0x0e << 24) | HAS_DISTINCT_VERSION_FLAG, 8);
			value.writeDoubleBE(distinctVersion, 12);
			value.write('meta', 20);
			db.transactionSync((txn: Transaction) => {
				txn.putSync('flagged', value);
			});
			const flagged = db.getEntrySync('flagged');
			expect(flagged?.version).toBe(distinctVersion);
			expect(flagged?.localTime).not.toBe(distinctVersion);
			expect(flagged?.localTime).toBeTypeOf('number');

			// A 12-byte flagged corpse must not read out of range: version falls
			// back to localTime because the distinct word cannot exist.
			const corpse = Buffer.alloc(12);
			corpse.writeUInt32BE((0x0e << 24) | HAS_DISTINCT_VERSION_FLAG, 8);
			db.transactionSync((txn: Transaction) => {
				txn.putSync('corpse', corpse);
			});
			const parsed = db.getEntrySync('corpse');
			expect(parsed?.version).toBe(parsed?.localTime);
		} finally {
			db.close();
		}
	});

	it('inherits the durable marker on reopen and fails closed on explicit false', () => {
		const path = dbPath();
		let db = openStamped(path);
		let firstStamp = 0;
		try {
			db.transactionSync((txn: Transaction) => {
				txn.putSync('k', stampedValue(0, 'one'));
			});
			firstStamp = firstWord(db.getSync('k') as Buffer);
		} finally {
			db.close();
		}

		// Reopen WITHOUT the option: the marker enables stamping (the data
		// format demands it — an option-less reopen must not write unstamped
		// first words into a stamped CF).
		db = RocksDatabase.open(path, { encoding: 'binary' });
		try {
			db.transactionSync((txn: Transaction) => {
				txn.putSync('k2', stampedValue(0, 'two'));
			});
			const second = firstWord(db.getSync('k2') as Buffer);
			expect(second).toBeGreaterThan(firstStamp);
		} finally {
			db.close();
		}

		// Explicit false conflicts with the marker.
		expect(() => RocksDatabase.open(path, { encoding: 'binary', commitStamping: false })).toThrow(
			/durably marked/
		);
	});

	it('never re-mints a stamp across restart (clean-close floor)', () => {
		const path = dbPath();
		let db = openStamped(path);
		let futureStamp = 0;
		try {
			// A kept caller timestamp slightly in the future pushes the watermark
			// ahead of the wall clock.
			const future = Date.now() + 30000;
			const txn = db.transactionSync((t: Transaction): Transaction => {
				t.setTimestamp(future);
				t.putSync('f', stampedValue(0, 'future'));
				return t;
			}) as Transaction;
			futureStamp = txn.getCommittedLocalTime()!;
			expect(futureStamp).toBe(future);
		} finally {
			db.close();
		}

		db = openStamped(path);
		try {
			db.transactionSync((txn: Transaction) => {
				txn.putSync('g', stampedValue(0, 'after-restart'));
			});
			// The restart seeds from the persisted floor: the new stamp must be
			// above the pre-restart future stamp even though the wall clock is
			// behind it.
			expect(firstWord(db.getSync('g') as Buffer)).toBeGreaterThan(futureStamp);
		} finally {
			db.close();
		}
	});

	it('rejects enabling an existing column family on an already-open database', () => {
		const path = dbPath();
		const db = RocksDatabase.open(path, { encoding: 'binary' });
		try {
			expect(() => openStamped(path)).toThrow(/exclusive first open/);
		} finally {
			db.close();
		}
	});

	it('rejects the reserved metadata column family name', () => {
		expect(() =>
			RocksDatabase.open(dbPath(), { encoding: 'binary', name: '__rocksdbjs.meta' })
		).toThrow(/reserved/);
	});

	it('VT freshness keys on the stamp (superseding #766 without breaking it)', () => {
		const db = openStamped(dbPath(), { verificationTable: true });
		try {
			const txn = db.transactionSync((t: Transaction): Transaction => {
				t.putSync('k', stampedValue(0, 'cached'));
				return t;
			}) as Transaction;
			const stamp = txn.getCommittedLocalTime()!;

			// Populate the slot with a real read, then verify by the stamp: the
			// first word IS the freshness token, unique per write by construction.
			expect((db.getSync('k') as Buffer).readDoubleBE(0)).toBe(stamp);
			expect(db.getSync('k', { expectedVersion: stamp })).toBe(FRESH_VERSION_FLAG);

			// A new write to the key invalidates the old stamp: never FRESH again.
			db.transactionSync((t: Transaction) => {
				t.putSync('k', stampedValue(0, 'newer'));
			});
			const result = db.getSync('k', { expectedVersion: stamp });
			expect(result).not.toBe(FRESH_VERSION_FLAG);
		} finally {
			db.close();
		}
	});

	it('keeps mixed stamped/unstamped column families correct in one transaction', () => {
		const path = dbPath();
		const stamped = openStamped(path, { name: 'stamped' });
		const plain = RocksDatabase.open(path, { encoding: 'binary', name: 'plain' });
		try {
			const rawWord = 55.25;
			// Force the re-stamp path so the batch rebuild runs across both CFs.
			const txn = stamped.transactionSync((t: Transaction): Transaction => {
				t.setTimestamp(2000.5); // below watermark after activation floor
				t.putSync('s', stampedValue(0, 'stamped-cf'));
				plain.putSync('p', stampedValue(rawWord, 'plain-cf'), { transaction: t });
				return t;
			}) as Transaction;
			expect(txn.getCommittedLocalTime()).toBeTypeOf('number');
			// Stamped CF: first word replaced with the commit stamp.
			expect(firstWord(stamped.getSync('s') as Buffer)).not.toBe(0);
			// Unstamped CF: bytes untouched.
			const plainValue = plain.getSync('p') as Buffer;
			expect(firstWord(plainValue)).toBe(rawWord);
			expect(plainValue.subarray(8).toString()).toBe('plain-cf');
		} finally {
			plain.close();
			stamped.close();
		}
	});
});
