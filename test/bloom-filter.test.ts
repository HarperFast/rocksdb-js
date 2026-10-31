import { RocksDatabase } from '../src/index.ts';
import { dbRunner, generateDBPath } from './lib/util.ts';
import { rmSync } from 'node:fs';
import { afterAll, describe, expect, it } from 'vitest';

const KEY_COUNT = 1000;
const presentKey = (i: number) => `key-${String(i * 2).padStart(6, '0')}`;
const absentKey = (i: number) => `key-${String(i * 2 + 1).padStart(6, '0')}`;

function filterBlockSize(db: RocksDatabase): number {
	const properties = db.getDBProperty('rocksdb.aggregated-table-properties') ?? '';
	const match = /filter block size=(\d+)/.exec(properties);
	expect(match, properties).not.toBeNull();
	return Number(match![1]);
}

async function usefulFilterProbes(db: RocksDatabase, keys: string[]): Promise<number> {
	const before = db.getStat('rocksdb.bloom.filter.useful') as number;
	for (const key of keys) {
		expect(await db.get(key)).toBeUndefined();
	}
	return (db.getStat('rocksdb.bloom.filter.useful') as number) - before;
}

const absentKeys = Array.from({ length: KEY_COUNT }, (_, i) => absentKey(i));
const paths: string[] = [];

afterAll(() => {
	if (process.env.KEEP_FILES) return;
	for (const path of paths) {
		rmSync(path, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
	}
});

describe('bloomFilterBitsPerKey', () => {
	it.each([NaN, Infinity, -1, 0.5, 100.5])('should reject %s', (bloomFilterBitsPerKey) =>
		dbRunner({ dbOptions: [{ bloomFilterBitsPerKey }], skipOpen: true }, async ({ db }) => {
			expect(() => db.open()).toThrow(
				'bloomFilterBitsPerKey must be 0 (no filter) or a number from 1 to 100'
			);
		})
	);

	it('should give an existing filterless database filters once compaction rewrites its files', async () => {
		const path = generateDBPath();
		paths.push(path);

		const unfiltered = RocksDatabase.open(path, { bloomFilterBitsPerKey: 0, enableStats: true });
		for (let i = 0; i < KEY_COUNT; i++) {
			unfiltered.putSync(presentKey(i), i);
		}
		unfiltered.flushSync();
		unfiltered.compactSync({ bottommost: true });
		expect(filterBlockSize(unfiltered)).toBe(0);
		expect(await usefulFilterProbes(unfiltered, absentKeys)).toBe(0);
		unfiltered.close();

		const db = RocksDatabase.open(path, { enableStats: true });
		try {
			// No writes after the reopen: only a rewrite of the old files can produce a filter.
			expect(filterBlockSize(db)).toBe(0);
			expect(await usefulFilterProbes(db, absentKeys)).toBe(0);

			db.compactSync({ bottommost: true });
			expect(filterBlockSize(db)).toBeGreaterThan(0);
			// About 1% false positives at 10 bits per key.
			expect(await usefulFilterProbes(db, absentKeys)).toBeGreaterThan(KEY_COUNT * 0.9);

			for (let i = 0; i < KEY_COUNT; i++) {
				expect(await db.get(presentKey(i))).toBe(i);
			}
			const keys = Array.from(db.getKeys({ start: presentKey(10), end: presentKey(20) }));
			expect(keys).toEqual(Array.from({ length: 10 }, (_, i) => presentKey(i + 10)));
			const reversed = Array.from(
				db.getKeys({ start: presentKey(20), end: presentKey(10), reverse: true })
			);
			expect(reversed).toEqual(Array.from({ length: 10 }, (_, i) => presentKey(20 - i)));
			expect(db.getKeysCount()).toBe(KEY_COUNT);
		} finally {
			db.close();
		}
	});

	it('should write no filter when set to 0', () =>
		dbRunner({ dbOptions: [{ bloomFilterBitsPerKey: 0, enableStats: true }] }, async ({ db }) => {
			for (let i = 0; i < KEY_COUNT; i++) {
				db.putSync(presentKey(i), i);
			}
			db.flushSync();
			expect(filterBlockSize(db)).toBe(0);
			expect(await usefulFilterProbes(db, absentKeys)).toBe(0);
		}));

	it.each([
		['the shared block cache', {}],
		['no block cache', { noBlockCache: true }],
		['a fractional bits per key', { bloomFilterBitsPerKey: 6.5 }],
	])('should filter absent keys with %s', (_, options) =>
		dbRunner({ dbOptions: [{ ...options, enableStats: true }] }, async ({ db }) => {
			for (let i = 0; i < KEY_COUNT; i++) {
				db.putSync(presentKey(i), i);
			}
			db.flushSync();
			expect(filterBlockSize(db)).toBeGreaterThan(0);
			expect(await usefulFilterProbes(db, absentKeys)).toBeGreaterThan(KEY_COUNT * 0.8);
			for (let i = 0; i < KEY_COUNT; i += 97) {
				expect(await db.get(presentKey(i))).toBe(i);
			}
		})
	);

	it('should filter a column family created after the database opened', () =>
		dbRunner(
			{ dbOptions: [{ enableStats: true }, { name: 'late', enableStats: true }] },
			async (_, { db }) => {
				for (let i = 0; i < KEY_COUNT; i++) {
					db.putSync(presentKey(i), i);
				}
				db.flushSync();
				expect(await usefulFilterProbes(db, absentKeys)).toBeGreaterThan(KEY_COUNT * 0.9);
			}
		));
});
