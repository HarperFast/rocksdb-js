import { stats } from '../src/index.js';
import { dbRunner } from './lib/util.js';
import { describe, expect, it } from 'vitest';

describe('Statistics', () => {
	it('should get essential stats from database', () =>
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
				expect(Object.keys(stats).length).toBeLessThan(100);

				// internal stats
				expect(stats['rocksdb.number.keys.written']).toBe(2);

				// column family stats
				expect(stats['rocksdb.estimate-num-keys']).toBe(2);

				// excluded from essential stats
				expect(stats['rocksdb.readahead.trimmed']).not.toBeDefined();
			}
		));

	it('should get all stats from database', () =>
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

	it('should get all ticker names', () => {
		const tickerNames = stats.tickers;
		expect(tickerNames).toBeDefined();
		expect(tickerNames.length).toBeGreaterThan(0);
		expect(tickerNames.includes('rocksdb.number.keys.written')).toBe(true);
	});

	it('should get all histogram names', () => {
		const histogramNames = stats.histograms;
		expect(histogramNames).toBeDefined();
		expect(histogramNames.length).toBeGreaterThan(0);
		expect(histogramNames.includes('rocksdb.db.write.micros')).toBe(true);
	});

	it('should error if statistics are not enabled', () =>
		dbRunner(async ({ db }) => {
			expect(() => {
				db.getStat('rocksdb.block.cache.miss');
			}).toThrow('Statistics are not enabled');

			expect(() => {
				db.getStats();
			}).toThrow('Statistics are not enabled');
		}));
});
