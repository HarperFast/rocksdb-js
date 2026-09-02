import { constants, HAS_DISTINCT_VERSION_FLAG, RocksDatabase } from '../src/index.ts';
import { dbRunner, generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { rmSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const __dirname = dirname(fileURLToPath(import.meta.url));
const VERSION_HEADER_TAG = 0x0e;

/**
 * A value in the value-header contract: first word at 0, metadata word at 8
 * (tag byte + 24 flag bits), and — when `distinctVersion` is given — the
 * distinct version word at 12, followed by `tail` payload bytes.
 */
function headerValue(
	localTime: number,
	{
		flags = 0,
		tag = VERSION_HEADER_TAG,
		distinctVersion,
		tail = 4,
	}: {
		flags?: number;
		tag?: number;
		distinctVersion?: number;
		tail?: number;
	} = {}
): Buffer {
	const value = Buffer.alloc(12 + (distinctVersion === undefined ? 0 : 8) + tail);
	value.writeDoubleBE(localTime, 0);
	value.writeUInt32BE((((tag << 24) >>> 0) | flags) >>> 0, 8);
	if (distinctVersion !== undefined) {
		value.writeDoubleBE(distinctVersion, 12);
	}
	return value;
}

function runFixture(args: string[]): Promise<Record<string, any>> {
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, [
			join(__dirname, 'fixtures', 'fork-clock-floor.mts'),
			...args,
		]);
		let stdout = '';
		let stderr = '';
		child.stdout.on('data', (chunk) => (stdout += chunk));
		child.stderr.on('data', (chunk) => (stderr += chunk));
		child.on('error', reject);
		child.on('close', (code) => {
			if (code !== 0) {
				reject(new Error(`fixture ${args[0]} exited ${code}: ${stdout}${stderr}`));
				return;
			}
			resolve(JSON.parse(stdout.trim().split('\n').pop()!));
		});
	});
}

describe('Dual clock', () => {
	describe('HAS_DISTINCT_VERSION_FLAG', () => {
		it('is exported from the package and the constants, as 0x20000', () => {
			expect(HAS_DISTINCT_VERSION_FLAG).toBe(0x20000);
			expect(constants.HAS_DISTINCT_VERSION_FLAG).toBe(0x20000);
			expect(HAS_DISTINCT_VERSION_FLAG & constants.VERSION_NOT_UNIQUE_FLAG).toBe(0);
		});
	});

	for (const sync of [true, false]) {
		const name = sync ? 'getEntrySync()' : 'getEntry()';
		const getEntry = (db: RocksDatabase, key: string) =>
			sync ? db.getEntrySync(key) : db.getEntry(key);

		describe(name, () => {
			it('returns version === localTime for a value without the flag', () =>
				dbRunner({ dbOptions: [{ encoding: 'binary' }] }, async ({ db }) => {
					const localTime = 1.7e12;
					await db.put('legacy', headerValue(localTime));
					const entry = await getEntry(db, 'legacy');
					expect(entry?.localTime).toBe(localTime);
					expect(entry?.version).toBe(localTime);
					expect(entry?.value).toEqual(headerValue(localTime));
				}));

			it('returns both words for a flagged value', () =>
				dbRunner({ dbOptions: [{ encoding: 'binary' }] }, async ({ db }) => {
					const localTime = 1.7e12;
					const version = 1.6e12;
					await db.put(
						'flagged',
						headerValue(localTime, { flags: HAS_DISTINCT_VERSION_FLAG, distinctVersion: version })
					);
					const entry = await getEntry(db, 'flagged');
					expect(entry?.localTime).toBe(localTime);
					expect(entry?.version).toBe(version);
				}));

			it('keeps the other producer flags out of the way', () =>
				dbRunner({ dbOptions: [{ encoding: 'binary' }] }, async ({ db }) => {
					const localTime = 1.7e12;
					const version = 1.6e12;
					await db.put(
						'both',
						headerValue(localTime, {
							flags: HAS_DISTINCT_VERSION_FLAG | constants.VERSION_NOT_UNIQUE_FLAG,
							distinctVersion: version,
						})
					);
					const entry = await getEntry(db, 'both');
					expect(entry?.version).toBe(version);
					await db.put(
						'other-only',
						headerValue(localTime, {
							flags: constants.VERSION_NOT_UNIQUE_FLAG,
							distinctVersion: version,
						})
					);
					expect((await getEntry(db, 'other-only'))?.version).toBe(localTime);
				}));

			it('has no version for a flagged value shorter than 20 bytes', () =>
				dbRunner({ dbOptions: [{ encoding: 'binary' }] }, async ({ db }) => {
					const localTime = 1.7e12;
					await db.put(
						'short',
						headerValue(localTime, { flags: HAS_DISTINCT_VERSION_FLAG, tail: 0 })
					);
					const entry = await getEntry(db, 'short');
					expect(entry?.localTime).toBe(localTime);
					expect(entry?.version).toBeUndefined();
				}));

			it('ignores the flag bits under a foreign tag byte', () =>
				dbRunner({ dbOptions: [{ encoding: 'binary' }] }, async ({ db }) => {
					const localTime = 1.7e12;
					await db.put(
						'foreign',
						headerValue(localTime, {
							flags: HAS_DISTINCT_VERSION_FLAG,
							tag: 0x42,
							distinctVersion: 1.6e12,
						})
					);
					const entry = await getEntry(db, 'foreign');
					expect(entry?.localTime).toBe(localTime);
					expect(entry?.version).toBe(localTime);
				}));

			it('leaves a word undefined when it is not a finite positive number', () =>
				dbRunner({ dbOptions: [{ encoding: 'binary' }] }, async ({ db }) => {
					await db.put(
						'nan-first',
						headerValue(NaN, { flags: HAS_DISTINCT_VERSION_FLAG, distinctVersion: 1.6e12 })
					);
					let entry = await getEntry(db, 'nan-first');
					expect(entry?.localTime).toBeUndefined();
					expect(entry?.version).toBe(1.6e12);

					await db.put(
						'bad-second',
						headerValue(1.7e12, { flags: HAS_DISTINCT_VERSION_FLAG, distinctVersion: -1 })
					);
					entry = await getEntry(db, 'bad-second');
					expect(entry?.localTime).toBe(1.7e12);
					expect(entry?.version).toBeUndefined();

					await db.put('tiny', Buffer.from('abc'));
					entry = await getEntry(db, 'tiny');
					expect(entry?.localTime).toBeUndefined();
					expect(entry?.version).toBeUndefined();
					expect(entry?.value).toEqual(Buffer.from('abc'));
				}));

			it('returns undefined for a missing key', () =>
				dbRunner(async ({ db }) => {
					expect(await getEntry(db, 'missing')).toBeUndefined();
				}));

			it('decodes the value on an encoded store', () =>
				dbRunner(async ({ db }) => {
					await db.put('encoded', { hello: 'world' });
					const entry = await getEntry(db, 'encoded');
					expect(entry?.value).toEqual({ hello: 'world' });
				}));

			it('works inside a transaction', () =>
				dbRunner({ dbOptions: [{ encoding: 'binary' }] }, async ({ db }) => {
					const localTime = 1.7e12;
					await db.transaction(async (txn) => {
						await txn.put('in-txn', headerValue(localTime));
						const entry = await (sync ? txn.getEntrySync('in-txn') : txn.getEntry('in-txn'));
						expect(entry?.localTime).toBe(localTime);
						expect(entry?.version).toBe(localTime);
					});
				}));
		});
	}

	describe('clock floor at open', () => {
		it('seeds the clock above the largest durable log key in a fresh process', async () => {
			const dbPath = generateDBPath();
			try {
				const key = Date.now() + 3600 * 1000;
				expect(await runFixture(['write', dbPath, String(key)])).toMatchObject({ wrote: key });
				const observed = await runFixture(['read', dbPath, String(key)]);
				expect(observed.clock).toBeGreaterThan(key);
				expect(observed.txnTimestamp).toBeGreaterThan(observed.clock);
			} finally {
				rmSync(dbPath, { force: true, recursive: true });
			}
		}, 60000);

		it('seeds the clock from a rotated segment header when the largest key is not in the active segment', async () => {
			const dbPath = generateDBPath();
			try {
				const key = Date.now() + 2 * 3600 * 1000;
				const written = await runFixture(['write-rotated', dbPath, String(key)]);
				expect(written.segments).toBeGreaterThanOrEqual(2);
				const observed = await runFixture(['read', dbPath, String(key)]);
				expect(observed.clock).toBeGreaterThan(key);
			} finally {
				rmSync(dbPath, { force: true, recursive: true });
			}
		}, 60000);

		it('warns when a segment header cannot be read and still opens', async () => {
			const dbPath = generateDBPath();
			try {
				const key = Date.now() + 3600 * 1000;
				await runFixture(['write', dbPath, String(key)]);
				const observed = await runFixture(['warn', dbPath, String(key)]);
				expect(observed.warnings).toBeGreaterThanOrEqual(1);
				expect(observed.clock).toBeGreaterThan(key);
			} finally {
				rmSync(dbPath, { force: true, recursive: true });
			}
		}, 60000);

		it('surfaces clock exhaustion as a JS error once a key at the cap is durable', async () => {
			const dbPath = generateDBPath();
			try {
				await runFixture(['cap-write', dbPath]);
				const observed = await runFixture(['cap-read', dbPath]);
				expect(String(observed.clockError)).toContain('Monotonic timestamp domain exhausted');
				expect(String(observed.txnError)).toContain('Monotonic timestamp domain exhausted');
			} finally {
				rmSync(dbPath, { force: true, recursive: true });
			}
		}, 60000);

		it('leaves the clock alone for a database without transaction logs', async () => {
			const dbPath = generateDBPath();
			try {
				const observed = await runFixture(['plain', dbPath]);
				expect(observed.clock).toBeGreaterThan(observed.now - 1000);
				expect(observed.clock).toBeLessThan(observed.now + 1000);
			} finally {
				rmSync(dbPath, { force: true, recursive: true });
			}
		}, 60000);
	});
});
