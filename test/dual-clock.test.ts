import { RocksDatabase, constants } from '../src/index.ts';
import { dbRunner, generateDBPath } from './lib/util.ts';
import { describe, expect, it } from 'vitest';

const {
	HAS_DISTINCT_VERSION_FLAG,
	VERSION_HEADER_TAG,
	VERSION_NOT_UNIQUE_FLAG,
	FRESH_VERSION_FLAG,
} = constants;

/**
 * A value carrying the header `getEntry()` reads: an 8-byte big-endian first word, the 4-byte
 * big-endian metadata word, an optional 8-byte big-endian second word, then payload bytes.
 */
function headerValue(
	localTime: number,
	{
		flags = 0,
		version,
		tag = VERSION_HEADER_TAG,
		payload = Buffer.alloc(0),
		omitMetadata = false,
	}: {
		flags?: number;
		version?: number;
		tag?: number;
		payload?: Buffer;
		omitMetadata?: boolean;
	} = {}
): Buffer {
	const head = Buffer.alloc(omitMetadata ? 8 : version === undefined ? 12 : 20);
	head.writeDoubleBE(localTime, 0);
	if (!omitMetadata) {
		head.writeUInt32BE((((tag << 24) >>> 0) | flags) >>> 0, 8);
	}
	if (version !== undefined) {
		head.writeDoubleBE(version, 12);
	}
	return Buffer.concat([head, payload]);
}

const rawDB = { dbOptions: [{ encoding: false as const }] };

describe('getEntry() / getEntrySync()', () => {
	it('exports the distinct-version flag and the header tag', () => {
		expect(constants.HAS_DISTINCT_VERSION_FLAG).toBe(0x20000);
		expect(VERSION_HEADER_TAG).toBe(0x0e);
		// The three producer flags live in the same 24-bit field and must not collide.
		expect(HAS_DISTINCT_VERSION_FLAG & VERSION_NOT_UNIQUE_FLAG).toBe(0);
	});

	it('returns distinct words when the flag is set', () =>
		dbRunner(rawDB, async ({ db }) => {
			const localTime = 1.7e12;
			const version = 1.5e12;
			await db.put('k', headerValue(localTime, { flags: HAS_DISTINCT_VERSION_FLAG, version }));

			const entry = db.getEntrySync('k') as any;
			expect(entry.localTime).toBe(localTime);
			expect(entry.version).toBe(version);
			expect(entry.version).not.toBe(entry.localTime);

			expect(await db.getEntry('k')).toMatchObject({ localTime, version });
		}));

	it('reports version === localTime when the flag is absent', () =>
		dbRunner(rawDB, async ({ db }) => {
			const localTime = 1.7e12;
			await db.put('k', headerValue(localTime));

			const entry = db.getEntrySync('k') as any;
			expect(entry.localTime).toBe(localTime);
			expect(entry.version).toBe(localTime);
		}));

	it('reports version === localTime when another producer flag is set', () =>
		dbRunner(rawDB, async ({ db }) => {
			const localTime = 1.7e12;
			await db.put('k', headerValue(localTime, { flags: VERSION_NOT_UNIQUE_FLAG }));

			expect(db.getEntrySync('k')).toMatchObject({ localTime, version: localTime });
		}));

	it('ignores the flag bit in a word that is not a metadata word', () =>
		dbRunner(rawDB, async ({ db }) => {
			const localTime = 1.7e12;
			await db.put(
				'k',
				headerValue(localTime, { flags: HAS_DISTINCT_VERSION_FLAG, version: 1.5e12, tag: 0x0d })
			);

			expect(db.getEntrySync('k')).toMatchObject({ localTime, version: localTime });
		}));

	it('reports no version when the flag is set without a second word', () =>
		dbRunner(rawDB, async ({ db }) => {
			const localTime = 1.7e12;
			const value = headerValue(localTime, { flags: HAS_DISTINCT_VERSION_FLAG });
			await db.put('k', value);

			const entry = db.getEntrySync('k') as any;
			expect(entry.localTime).toBe(localTime);
			expect(entry.version).toBeUndefined();
		}));

	it('reports no words for a value with no header', () =>
		dbRunner(rawDB, async ({ db }) => {
			await db.put('k', Buffer.from([1, 2, 3]));

			const entry = db.getEntrySync('k') as any;
			expect(entry.localTime).toBeUndefined();
			expect(entry.version).toBeUndefined();
			expect(Buffer.from(entry.value)).toEqual(Buffer.from([1, 2, 3]));
		}));

	it('reports no words for a non-finite or non-positive first word', () =>
		dbRunner(rawDB, async ({ db }) => {
			await db.put('nan', headerValue(Number.NaN));
			await db.put('zero', headerValue(0));

			expect(db.getEntrySync('nan')).toMatchObject({ localTime: undefined, version: undefined });
			expect(db.getEntrySync('zero')).toMatchObject({ localTime: undefined, version: undefined });
		}));

	it('returns undefined for a missing key', () =>
		dbRunner(rawDB, async ({ db }) => {
			expect(db.getEntrySync('nope')).toBeUndefined();
			expect(await db.getEntry('nope')).toBeUndefined();
		}));

	it('returns the value get() returns, decoded the same way', () =>
		dbRunner(async ({ db }) => {
			await db.put('k', { hello: 'world' });

			const entry = db.getEntrySync('k') as any;
			expect(entry.value).toEqual(db.getSync('k'));
			expect(entry.value).toEqual({ hello: 'world' });
			// A msgpack value carries no clock words unless a producer wrote them.
			expect(entry.localTime).toBeUndefined();
		}));

	it('honors skipDecode, returning the same bytes the binary read returns', () =>
		dbRunner(async ({ db }) => {
			await db.put('k', { hello: 'world' });

			const decoded = db.getEntrySync('k') as any;
			expect(decoded.value).toEqual({ hello: 'world' });

			const raw = db.getEntrySync('k', { skipDecode: true }) as any;
			expect(raw.value).toBeInstanceOf(Uint8Array);

			// Same contract as `get(key, { skipDecode: true })`: the undecoded bytes,
			// which on a copying-decoder store are the reusable read buffer, so the
			// value's own extent is `end` and not `length`.
			const expected = db.getBinarySync('k') as Buffer;
			const end = raw.value.end ?? raw.value.length;
			expect(Buffer.from(raw.value.subarray(0, end))).toEqual(
				Buffer.from(expected.subarray(0, (expected as any).end ?? expected.length))
			);
		}));

	it('reads through a transaction', () =>
		dbRunner(rawDB, async ({ db }) => {
			const localTime = 1.7e12;
			const version = 1.4e12;
			await db.transaction(async (txn) => {
				await txn.put('k', headerValue(localTime, { flags: HAS_DISTINCT_VERSION_FLAG, version }));
				expect(txn.getEntrySync('k')).toMatchObject({ localTime, version });
			});

			expect(db.getEntrySync('k')).toMatchObject({ localTime, version });
		}));

	it("returns a localTime that finds the record's own transaction-log entry", () =>
		dbRunner(rawDB, async ({ db }) => {
			// The claim the first word exists for: it is the key of the batch that
			// wrote the record, so a consumer can seek the log at exactly that value.
			// Every other case here hand-assembles a header; this one round-trips a
			// real transaction's timestamp through both the record and the log.
			let claimed = 0;
			await db.transaction(async (txn) => {
				claimed = txn.getTimestamp();
				await txn.put('k', headerValue(claimed));
				db.useLog('audit').addEntry(Buffer.from('entry-for-k'), txn.id);
			});

			const entry = db.getEntrySync('k') as any;
			expect(entry.localTime).toBe(claimed);

			const found = Array.from(
				db.useLog('audit').query({ start: entry.localTime, exactStart: true })
			);
			expect(found.length).toBeGreaterThan(0);
			expect(found[0].timestamp).toBe(entry.localTime);
			expect(Buffer.from(found[0].data).toString()).toBe('entry-for-k');
		}));

	it('passes the fresh-version sentinel through unchanged', () => {
		const dbPath = generateDBPath();
		const db = new RocksDatabase(dbPath, { encoding: 'binary', verificationTable: true });
		try {
			db.open();
			const localTime = 1.7e12;
			db.putSync('k', headerValue(localTime));
			db.populateVersion('k', localTime);

			expect(db.getEntrySync('k', { expectedVersion: localTime })).toBe(FRESH_VERSION_FLAG);
		} finally {
			db.close();
		}
	});
});
