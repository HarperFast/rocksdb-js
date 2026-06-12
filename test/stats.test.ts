import { stats } from '../src/index.js';
import { dbRunner } from './lib/util.js';
import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

/**
 * Canonical stat key -> value-type catalog, derived from the public type
 * contract in `src/stats.ts`. The native binding builds the stats object by
 * hand (see `DBDescriptor::getStats` / `DBHandle::getStats`), and `stats.ts`
 * plus `docs/stats.md` are hand-maintained mirrors of that surface. These
 * shape tests assert the runtime output matches `stats.ts` exactly so that any
 * drift -- a renamed/added/removed ticker, or a ticker that should have been a
 * histogram -- fails loudly instead of silently skewing the typed API & docs.
 *
 * We parse `stats.ts` rather than re-listing ~360 keys here so the test tracks
 * the single source of truth automatically.
 */
type StatKind = 'number' | 'histogram';

const STATS_TS = readFileSync(new URL('../src/stats.ts', import.meta.url), 'utf8');

function parseStatsGroup(typeName: string): Map<string, StatKind> {
	const match = STATS_TS.match(new RegExp(`export type ${typeName} = \\{([\\s\\S]*?)\\n\\};`));
	if (!match) {
		throw new Error(`Could not locate "export type ${typeName}" in src/stats.ts`);
	}
	const group = new Map<string, StatKind>();
	const lineRe = /'([^']+)'\s*:\s*(number|StatsHistogramData)\b/g;
	let line: RegExpExecArray | null;
	while ((line = lineRe.exec(match[1])) !== null) {
		group.set(line[1], line[2] === 'StatsHistogramData' ? 'histogram' : 'number');
	}
	if (group.size === 0) {
		throw new Error(`Parsed zero keys from "${typeName}"; src/stats.ts format may have changed`);
	}
	return group;
}

function mergeGroups(...groups: Map<string, StatKind>[]): Map<string, StatKind> {
	const merged = new Map<string, StatKind>();
	for (const group of groups) {
		for (const [key, kind] of group) {
			merged.set(key, kind);
		}
	}
	return merged;
}

// `StatsBasics` (column-family properties + the always-present txnlog.* summary
// keys) is returned regardless of `enableStats`. The curated/all sets are only
// populated when statistics are enabled.
const BASIC_STATS = parseStatsGroup('StatsBasics');
const CURATED_STATS = mergeGroups(BASIC_STATS, parseStatsGroup('StatsCuratedExtras'));
const ALL_STATS = mergeGroups(CURATED_STATS, parseStatsGroup('StatsAllExtras'));

const HISTOGRAM_FIELDS = [
	'average',
	'count',
	'max',
	'median',
	'min',
	'percentile95',
	'percentile99',
	'standardDeviation',
	'sum',
] as const;

function assertHistogram(key: string, value: unknown): void {
	expect(value, `${key} should be a non-null histogram object`).toBeTypeOf('object');
	expect(value, `${key} should not be null`).not.toBeNull();
	expect(Object.keys(value as object).sort(), `${key} histogram fields`).toEqual(
		[...HISTOGRAM_FIELDS].sort()
	);
	for (const field of HISTOGRAM_FIELDS) {
		expect((value as Record<string, unknown>)[field], `${key}.${field}`).toBeTypeOf('number');
	}
}

/**
 * Asserts the returned stats object contains exactly the expected keys (no
 * missing, no extra) and that each value's runtime type matches the declared
 * kind.
 */
function expectStatsShape(actual: Record<string, unknown>, expected: Map<string, StatKind>): void {
	const actualKeys = Object.keys(actual);
	const expectedKeys = [...expected.keys()];

	const missing = expectedKeys.filter((key) => !(key in actual)).sort();
	const extra = actualKeys.filter((key) => !expected.has(key)).sort();

	expect(missing, 'stat keys declared in src/stats.ts but missing from getStats()').toEqual([]);
	expect(extra, 'stat keys returned by getStats() but not declared in src/stats.ts').toEqual([]);

	for (const [key, kind] of expected) {
		if (kind === 'histogram') {
			assertHistogram(key, actual[key]);
		} else {
			expect(actual[key], `${key} should be a number ticker`).toBeTypeOf('number');
		}
	}
}

describe('Statistics', () => {
	it('should get essential stats', () =>
		dbRunner(
			{ dbOptions: [{ enableStats: true, statsLevel: stats.StatsLevel.All }] },
			async ({ db }) => {
				let stats = db.getStats();
				expect(stats).toBeDefined();
				expect(stats['rocksdb.number.keys.written']).toBe(0);
				expect(db.getStat('rocksdb.number.keys.written')).toBe(0);

				await db.put('key1', 'value1');
				await db.put('key2', 'value2');

				stats = db.getStats();
				expect(stats).toBeDefined();
				expect(Object.keys(stats).length).toBeGreaterThan(24);
				expect(Object.keys(stats).length).toBeLessThan(100);

				// internal stats
				expect(stats['rocksdb.number.keys.written']).toBe(2);

				// column family stats
				expect(stats['rocksdb.estimate-num-keys']).toBe(2);

				// excluded from essential stats
				expect(stats['rocksdb.readahead.trimmed']).toBeUndefined();
			}
		));

	it('should get column family level essential stats when stats are disabled', () =>
		dbRunner(async ({ db }) => {
			let stats = db.getStats();
			expect(stats).toBeDefined();
			expect(stats['rocksdb.number.keys.written']).toBeUndefined();

			await db.put('key1', 'value1');
			await db.put('key2', 'value2');

			stats = db.getStats();
			expect(stats).toBeDefined();
			// the curated column-family set stays small; the always-present txnlog.*
			// summary keys are counted separately.
			const nonTxnlogKeys = Object.keys(stats).filter((key) => !key.startsWith('txnlog.'));
			expect(nonTxnlogKeys.length).toBeLessThanOrEqual(25);

			// internal stats
			expect(stats['rocksdb.number.keys.written']).toBeUndefined();

			// column family stats
			expect(stats['rocksdb.estimate-num-keys']).toBe(2);
		}));

	it('should get all stats', () =>
		dbRunner(
			{ dbOptions: [{ enableStats: true, statsLevel: stats.StatsLevel.All }] },
			async ({ db }) => {
				let stats = db.getStats(true);
				expect(stats).toBeDefined();
				expect(stats['rocksdb.number.keys.written']).toBe(0);
				expect(db.getStat('rocksdb.number.keys.written')).toBe(0);

				await db.put('key1', 'value1');
				await db.put('key2', 'value2');

				stats = db.getStats(true);
				expect(stats).toBeDefined();
				expect(Object.keys(stats).length).toBeGreaterThan(100);

				// internal stats
				expect(stats['rocksdb.number.keys.written']).toBe(2);

				// column family stats
				expect(stats['rocksdb.estimate-num-keys']).toBe(2);

				// all stats
				expect(stats['rocksdb.readahead.trimmed']).toBeDefined();
			}
		));

	it('should get ticker stat from database', () =>
		dbRunner({ dbOptions: [{ enableStats: true }] }, async ({ db }) => {
			await db.put('key1', 'value1');
			await db.put('key2', 'value2');

			const stat = db.getStat('rocksdb.number.keys.written');
			expect(stat).toBe(2);
		}));

	it('should get histogram stat from database', () =>
		dbRunner({ dbOptions: [{ enableStats: true }] }, async ({ db }) => {
			await db.put('key1', 'value1');
			await db.put('key2', 'value2');

			const stat = db.getStat('rocksdb.db.write.micros');
			expect(stat).toBeDefined();
			expect(typeof stat).toBe('object');
			expect(stat['average']).toBeGreaterThan(0);
			expect(stat['count']).toBe(2);
			expect(stat['max']).toBeGreaterThan(0);
			expect(stat['median']).toBeGreaterThan(0);
			expect(stat['min']).toBeGreaterThan(0);
			expect(stat['percentile95']).toBeGreaterThan(0);
			expect(stat['percentile99']).toBeGreaterThan(0);
		}));
});

describe('Statistics shape (property name & type skew detection)', () => {
	// With statistics disabled, both getStats() and getStats(true) return only
	// the basic column-family properties plus the always-present txnlog.*
	// summary keys -- the curated/all RocksDB ticker & histogram sets are gated
	// behind `enableStats: true`.
	it('getStats() returns only basic keys when enableStats is false', () =>
		dbRunner(async ({ db }) => {
			await db.put('key1', 'value1');
			await db.put('key2', 'value2');

			expectStatsShape(db.getStats() as Record<string, unknown>, BASIC_STATS);
		}));

	it('getStats(true) returns only basic keys when enableStats is false', () =>
		dbRunner(async ({ db }) => {
			await db.put('key1', 'value1');
			await db.put('key2', 'value2');

			expectStatsShape(db.getStats(true) as Record<string, unknown>, BASIC_STATS);
		}));

	it('getStats() returns the curated key set when enableStats is true', () =>
		dbRunner(
			{ dbOptions: [{ enableStats: true, statsLevel: stats.StatsLevel.All }] },
			async ({ db }) => {
				await db.put('key1', 'value1');
				await db.put('key2', 'value2');

				expectStatsShape(db.getStats() as Record<string, unknown>, CURATED_STATS);
			}
		));

	it('getStats(true) returns the full key set when enableStats is true', () =>
		dbRunner(
			{ dbOptions: [{ enableStats: true, statsLevel: stats.StatsLevel.All }] },
			async ({ db }) => {
				await db.put('key1', 'value1');
				await db.put('key2', 'value2');

				expectStatsShape(db.getStats(true) as Record<string, unknown>, ALL_STATS);
			}
		));
});
