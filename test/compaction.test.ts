import { dbRunner } from './lib/util.js';
import { describe, expect, it } from 'vitest';

describe('Compaction', () => {
	it('should compact on close', () =>
		dbRunner(async ({ db }) => {
			const sizeStart = db.getDBIntProperty('rocksdb.estimate-live-data-size') ?? 0;
			for (let i = 0; i < 1000; ++i) {
				await db.put(`foo-${i}`, `bar-${i}`);
			}
			await db.flush();
			const sizeWithData = db.getDBIntProperty('rocksdb.estimate-live-data-size') ?? 0;
			expect(sizeWithData).toBeGreaterThan(sizeStart);

			for (let i = 0; i < 1000; ++i) {
				await db.remove(`foo-${i}`);
			}
			await db.flush();
			const sizeAfterRemove = db.getDBIntProperty('rocksdb.estimate-live-data-size') ?? 0;
			db.close();

			db.open();
			const sizeAfter = db.getDBIntProperty('rocksdb.estimate-live-data-size') ?? 0;
			expect(sizeAfter).toBeLessThan(sizeAfterRemove);
		}));

	it('should compact with compact()', () =>
		dbRunner(async ({ db }) => {
			const sizeStart = db.getDBIntProperty('rocksdb.estimate-live-data-size') ?? 0;
			for (let i = 0; i < 1000; ++i) {
				await db.put(`foo-${i}`, `bar-${i}`);
			}
			await db.flush();
			const sizeWithData = db.getDBIntProperty('rocksdb.estimate-live-data-size') ?? 0;
			expect(sizeWithData).toBeGreaterThan(sizeStart);

			for (let i = 0; i < 1000; ++i) {
				await db.remove(`foo-${i}`);
			}
			await db.flush();
			const sizeAfterRemove = db.getDBIntProperty('rocksdb.estimate-live-data-size') ?? 0;

			await db.compact();
			const sizeAfterCompact = db.getDBIntProperty('rocksdb.estimate-live-data-size') ?? 0;
			expect(sizeAfterCompact).toBeLessThan(sizeAfterRemove);
		}));

	it('should compact with compactSync()', () =>
		dbRunner(async ({ db }) => {
			const sizeStart = db.getDBIntProperty('rocksdb.estimate-live-data-size') ?? 0;
			for (let i = 0; i < 1000; ++i) {
				await db.put(`foo-${i}`, `bar-${i}`);
			}
			await db.flush();
			const sizeWithData = db.getDBIntProperty('rocksdb.estimate-live-data-size') ?? 0;
			expect(sizeWithData).toBeGreaterThan(sizeStart);

			for (let i = 0; i < 1000; ++i) {
				await db.remove(`foo-${i}`);
			}
			await db.flush();
			const sizeAfterRemove = db.getDBIntProperty('rocksdb.estimate-live-data-size') ?? 0;

			db.compactSync();
			const sizeAfterCompact = db.getDBIntProperty('rocksdb.estimate-live-data-size') ?? 0;
			expect(sizeAfterCompact).toBeLessThan(sizeAfterRemove);
		}));

	it('should compact a specific key range', () =>
		dbRunner(async ({ db }) => {
			// Insert data with different prefixes
			for (let i = 0; i < 100; ++i) {
				await db.put(`aaa-${i}`, `value-${i}`);
				await db.put(`bbb-${i}`, `value-${i}`);
				await db.put(`ccc-${i}`, `value-${i}`);
			}
			await db.flush();

			const sizeBeforeCompact = db.getDBIntProperty('rocksdb.estimate-live-data-size') ?? 0;

			// Remove the 'bbb' prefix data to create tombstones
			for (let i = 0; i < 100; ++i) {
				await db.remove(`bbb-${i}`);
			}
			await db.flush();

			// Compact only the 'bbb' range to remove tombstones
			await db.compact({ start: 'bbb', end: 'bbc' });

			const sizeAfterCompact = db.getDBIntProperty('rocksdb.estimate-live-data-size') ?? 0;
			expect(sizeAfterCompact).toBeLessThan(sizeBeforeCompact);

			// Verify data outside the compacted range is still accessible
			expect(await db.get('aaa-0')).toBe('value-0');
			expect(await db.get('ccc-0')).toBe('value-0');

			// Remove 'aaa' data and test sync version
			for (let i = 0; i < 100; ++i) {
				await db.remove(`aaa-${i}`);
			}
			await db.flush();

			const sizeBeforeSyncCompact = db.getDBIntProperty('rocksdb.estimate-live-data-size') ?? 0;
			db.compactSync({ start: 'aaa', end: 'aab' });

			const sizeAfterCompactSync = db.getDBIntProperty('rocksdb.estimate-live-data-size') ?? 0;
			expect(sizeAfterCompactSync).toBeLessThan(sizeBeforeSyncCompact);
		}));
});
