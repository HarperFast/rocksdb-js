import { StatsLevel } from '../src/index.js';
import { dbRunner } from './lib/util.js';
import { describe, expect, it } from 'vitest';

describe('Statistics', () => {
	it('should get statistics from database', () =>
		dbRunner(
			{
				dbOptions: [
					{
						enableStats: true,
						statsLevel: StatsLevel.All,
					},
				],
			},
			async ({ db }) => {
				let stats = db.getStats();
				expect(stats).toBeDefined();
				expect(stats['rocksdb.number.keys.written']).toBe(0);
				expect(db.getStat('rocksdb.number.keys.written')).toBe(0);

				await db.put('key1', 'value1');
				await db.put('key2', 'value2');

				stats = db.getStats();
				expect(stats).toBeDefined();
				expect(stats['rocksdb.number.keys.written']).toBe(2);
				expect(db.getStat('rocksdb.number.keys.written')).toBe(2);
			}
		));

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
