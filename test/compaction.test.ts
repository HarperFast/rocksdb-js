import { RocksDatabase, supportedCompression } from '../src/index.ts';
import { dbRunner } from './lib/util.ts';
import { readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

/** Total bytes of files with `extension` under `dir` — what a codec change actually moves. */
function fileBytes(dir: string, extension: string): number {
	let total = 0;
	for (const entry of readdirSync(dir, {
		withFileTypes: true,
		recursive: true,
	})) {
		if (entry.isFile() && entry.name.endsWith(extension)) {
			// `parentPath` was added in Node 20.11/21.5; README documents Node 18+ support, where
			// this is `undefined` and the property was still named `path`.
			const parentPath = entry.parentPath ?? (entry as unknown as { path?: string }).path ?? dir;
			total += statSync(join(parentPath, entry.name)).size;
		}
	}
	return total;
}

const sstBytes = (dir: string) => fileBytes(dir, '.sst');
const blobBytes = (dir: string) => fileBytes(dir, '.blob');

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
	it.skipIf(!supportedCompression.includes('zstd'))(
		'should re-encode existing data under a new codec only when bottommost is requested',
		() =>
			dbRunner(async ({ db, dbPath }) => {
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

				// compactSync takes the same option; data is already recoded, so this just confirms
				// it doesn't undo anything.
				const again = RocksDatabase.open(dbPath, {
					name: 'recode',
					compression: 'zstd',
				});
				again.compactSync({ bottommost: true });
				await again.close();
				expect(sstBytes(dbPath)).toBeLessThan(sizeUncompressed / 2);
			})
	);

	// Values at or above the binding's 2048-byte blob threshold (`min_blob_size`) are stored in
	// blob files, not SSTs. A bottommost SST compaction alone does not touch them: blob GC's
	// default age cutoff only reclaims the oldest fraction of blob files, so `bottommost: true`
	// forces blob GC across the full age range (see DBDescriptor::compactRange) to re-encode them
	// too. Without that, this test's blob files would stay on the old codec.
	it.skipIf(!supportedCompression.includes('zstd'))(
		'should re-encode existing blob-backed data under a new codec when bottommost is requested',
		() =>
			dbRunner(async ({ db, dbPath }) => {
				// Compressible and above the 2048-byte blob threshold.
				const words = 'the quick brown fox jumps over lazy dog durable premium standard'.split(' ');
				const value = (i: number) =>
					Array.from({ length: 400 }, (_unused, w) => words[(i * 7 + w * 13) % words.length]).join(
						' '
					);
				expect(value(0).length).toBeGreaterThan(2048);
				const keyOf = (i: number) => `k${String(i).padStart(8, '0')}`;

				db.close();
				// Three separate flushes make three blob-file generations, so the regression also
				// covers the age cutoff: a default cutoff would leave the older generations behind.
				const uncompressed = RocksDatabase.open(dbPath, {
					name: 'recodeBlob',
					compression: 'none',
				});
				for (let batch = 0; batch < 3; ++batch) {
					for (let i = batch * 700; i < batch * 700 + 700; ++i)
						uncompressed.putSync(keyOf(i), value(i));
					await uncompressed.flush();
				}
				await uncompressed.close();
				const sizeUncompressed = blobBytes(dbPath);
				expect(sizeUncompressed).toBeGreaterThan(0);

				// Reopening under zstd governs new writes; it does not touch what is already there.
				const plain = RocksDatabase.open(dbPath, {
					name: 'recodeBlob',
					compression: 'zstd',
				});
				await plain.compact();
				await plain.close();
				expect(blobBytes(dbPath)).toBe(sizeUncompressed);

				const forced = RocksDatabase.open(dbPath, {
					name: 'recodeBlob',
					compression: 'zstd',
				});
				// Exercise both entry points independently: restrict the async call to the first
				// generation's range, leaving the rest on the old codec for compactSync to re-encode.
				await forced.compact({ bottommost: true, start: keyOf(0), end: keyOf(700) });
				const sizeAfterAsync = blobBytes(dbPath);
				expect(sizeAfterAsync).toBeLessThan(sizeUncompressed);

				forced.compactSync({ bottommost: true });
				expect(blobBytes(dbPath)).toBeLessThan(sizeUncompressed / 2);

				// Re-encoding must not lose or corrupt any generation's values, including the oldest.
				for (const i of [0, 699, 700, 1399, 1400, 2099])
					expect(await forced.get(keyOf(i))).toBe(value(i));
				await forced.close();
			})
	);

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
