import { RocksDatabase, supportedCompression } from '../src/index.js';
import { dbRunner } from './lib/util.js';
import { readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

/** Total bytes of SST files under `dir` — what a codec change actually moves. */
function sstBytes(dir: string): number {
	let total = 0;
	for (const entry of readdirSync(dir, {
		withFileTypes: true,
		recursive: true,
	})) {
		if (entry.isFile() && entry.name.endsWith('.sst')) {
			total += statSync(join(entry.parentPath ?? dir, entry.name)).size;
		}
	}
	return total;
}

describe('Compaction', () => {
	afterEach(() => {
		RocksDatabase.config({ compactOnClose: false });
	});

	it('should compact on close', () =>
		dbRunner(async ({ db }) => {
			RocksDatabase.config({ compactOnClose: true });

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

	it('should not compact on close', () =>
		dbRunner(async ({ db }) => {
			RocksDatabase.config({ compactOnClose: false });

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
			expect(sizeAfter).toBe(sizeAfterRemove);
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

			const sizeAfterInsert = db.getDBIntProperty('rocksdb.estimate-live-data-size') ?? 0;

			// Remove the 'bbb' prefix data to create tombstones
			for (let i = 0; i < 100; ++i) {
				await db.remove(`bbb-${i}`);
			}
			await db.flush();

			// Compact only the 'bbb' range to remove tombstones
			await db.compact({ start: 'bbb', end: 'bbc' });

			const sizeAfterCompact = db.getDBIntProperty('rocksdb.estimate-live-data-size') ?? 0;
			expect(sizeAfterCompact).toBeLessThan(sizeAfterInsert);

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

			const sizeAfterSyncCompact = db.getDBIntProperty('rocksdb.estimate-live-data-size') ?? 0;

			expect(sizeAfterSyncCompact).toBeLessThan(sizeBeforeSyncCompact);
		}));

	// RocksDB's CompactRangeOptions defaults bottommost_level_compaction to
	// kIfHaveCompactionFilter, so with no compaction filter installed a plain compact() leaves the
	// bottommost level alone — and that is where the bulk of the data ends up. Since a changed
	// compression algorithm only governs newly written files, that level has to be rewritten for
	// existing data to be re-encoded, which is what `bottommost: true` requests.
	it('should re-encode existing data under a new codec only when bottommost is requested', () =>
		dbRunner(async ({ db, dbPath }) => {
			if (!supportedCompression.includes('zstd')) return;

			// Compressible: a small vocabulary repeated, so the codec has something to find.
			const words = 'the quick brown fox jumps over lazy dog durable premium standard'.split(' ');
			const value = (i: number) =>
				Array.from({ length: 90 }, (_unused, w) => words[(i * 7 + w * 13) % words.length]).join(
					' '
				);

			db.close();
			const uncompressed = RocksDatabase.open(dbPath, {
				name: 'recode',
				compression: 'none',
			});
			for (let i = 0; i < 20_000; ++i)
				uncompressed.putSync(`k${String(i).padStart(8, '0')}`, value(i));
			await uncompressed.flush();
			await uncompressed.close();
			const sizeUncompressed = sstBytes(dbPath);
			expect(sizeUncompressed).toBeGreaterThan(0);

			// Reopening under zstd governs new writes; it does not touch what is already there.
			const plain = RocksDatabase.open(dbPath, {
				name: 'recode',
				compression: 'zstd',
			});
			expect(plain.compression.algorithm).toBe('zstd');
			await plain.compact();
			await plain.close();
			expect(sstBytes(dbPath)).toBe(sizeUncompressed);

			const forced = RocksDatabase.open(dbPath, {
				name: 'recode',
				compression: 'zstd',
			});
			await forced.compact({ bottommost: true });
			await forced.close();
			const sizeRecoded = sstBytes(dbPath);
			expect(sizeRecoded).toBeLessThan(sizeUncompressed / 2);

			// compactSync takes the same option, and re-running is a no-op.
			const again = RocksDatabase.open(dbPath, {
				name: 'recode',
				compression: 'zstd',
			});
			again.compactSync({ bottommost: true });
			await again.close();
			expect(sstBytes(dbPath)).toBeLessThan(sizeUncompressed / 2);
		}));

	it('should not compact more than once at a time', () =>
		dbRunner(async ({ db }) => {
			for (let i = 0; i < 1000; ++i) {
				await db.put(`foo-${i}`, `bar-${i}`);
			}
			await db.flush();

			await Promise.all([db.compact(), db.compact()]);

			expect(await db.get('foo-0')).toBe('bar-0');
			expect(await db.get('foo-999')).toBe('bar-999');
		}));
});
