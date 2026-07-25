import { dbRunner, generateDBPath } from './lib/util.js';
import { describe, expect, it } from 'vitest';

describe('Database write buffer options', () => {
	it('should open with default write buffer settings', () =>
		dbRunner(async ({ db }) => {
			await db.put('foo', 'bar');
			expect(await db.get('foo')).toBe('bar');
		}));

	it('should open with custom writeBufferSize and maxWriteBufferNumber', () =>
		dbRunner(
			{
				dbOptions: [
					{
						maxWriteBufferNumber: 8,
						writeBufferSize: 4 * 1024 * 1024,
					},
				],
			},
			async ({ db }) => {
				await db.put('foo', 'bar');
				expect(await db.get('foo')).toBe('bar');
			}
		));

	it('should open with dbWriteBufferSize set', () =>
		dbRunner({ dbOptions: [{ dbWriteBufferSize: 64 * 1024 * 1024 }] }, async ({ db }) => {
			await db.put('foo', 'bar');
			expect(await db.get('foo')).toBe('bar');
		}));

	it('should open with maxWriteBufferSizeToMaintain set', () =>
		dbRunner(
			{ dbOptions: [{ maxWriteBufferSizeToMaintain: 128 * 1024 * 1024 }] },
			async ({ db }) => {
				await db.put('foo', 'bar');
				expect(await db.get('foo')).toBe('bar');
			}
		));

	it('should open with an explicit maxOpenFiles cap and serve reads across evicted table handles', () =>
		dbRunner({ dbOptions: [{ maxOpenFiles: 32, writeBufferSize: 64 * 1024 }] }, async ({ db }) => {
			// A small memtable produces many small SSTs so reads must reopen
			// table files evicted from the 32-handle table cache.
			const value = 'x'.repeat(1024);
			for (let i = 0; i < 512; i++) {
				await db.put(`key-${i.toString().padStart(6, '0')}`, value);
			}
			await db.flush();
			expect(await db.get('key-000000')).toBe(value);
			expect(await db.get('key-000511')).toBe(value);
		}));

	it('should open with maxOpenFiles -1 (unlimited, the previous default)', () =>
		dbRunner({ dbOptions: [{ maxOpenFiles: -1 }] }, async ({ db }) => {
			await db.put('foo', 'bar');
			expect(await db.get('foo')).toBe('bar');
		}));

	it('should reject maxOpenFiles below -1', () =>
		dbRunner({ dbOptions: [{ maxOpenFiles: -2 }], skipOpen: true }, async ({ db }) => {
			expect(() => db.open()).toThrow(
				'maxOpenFiles must be -1 (unlimited), 0 (auto), or a positive 32-bit integer'
			);
		}));

	it('should reject non-integer maxOpenFiles instead of truncating to -1', () =>
		dbRunner({ dbOptions: [{ maxOpenFiles: -1.5 }], skipOpen: true }, async ({ db }) => {
			expect(() => db.open()).toThrow('maxOpenFiles must be');
		}));

	it('should reject maxOpenFiles beyond int32 range instead of wrapping', () =>
		dbRunner({ dbOptions: [{ maxOpenFiles: 2 ** 32 - 1 }], skipOpen: true }, async ({ db }) => {
			expect(() => db.open()).toThrow('maxOpenFiles must be');
		}));

	// A named CF is created after DB::Open, so it misses the open options.
	// Observed via flush behavior: RocksDB exposes no property for the size.
	it('should apply writeBufferSize to a named column family', () =>
		dbRunner(
			{
				dbOptions: [
					{ path: generateDBPath(), name: 'mycf', writeBufferSize: 64 * 1024 },
					{ path: generateDBPath(), name: 'mycf', writeBufferSize: 64 * 1024 * 1024 },
				],
			},
			async ({ db: smallBuffer }, { db: largeBuffer }) => {
				const value = 'x'.repeat(1024);
				for (const db of [smallBuffer, largeBuffer]) {
					for (let i = 0; i < 512; i++) {
						await db.put(`key-${i.toString().padStart(6, '0')}`, value);
					}
				}

				// Flushes run in the background.
				const deadline = Date.now() + 5000;
				let smallBufferSize = 0;
				while (Date.now() < deadline) {
					smallBufferSize = smallBuffer.getDBIntProperty('rocksdb.total-sst-files-size') ?? 0;
					if (smallBufferSize > 0) {
						break;
					}
					await new Promise((resolve) => setTimeout(resolve, 50));
				}

				expect(smallBufferSize).toBeGreaterThan(0);
				expect(largeBuffer.getDBIntProperty('rocksdb.total-sst-files-size')).toBe(0);
			}
		));

	it('should flush memtables when writeBufferSize is exceeded', () =>
		dbRunner({ dbOptions: [{ writeBufferSize: 64 * 1024 }] }, async ({ db }) => {
			// 64KB memtable; write enough data to force at least one flush.
			// `num-files-at-level0` can be racy under background compaction, so
			// check on-disk SST size instead — once any flush has happened it is
			// non-zero regardless of which level the data settled on.
			const value = 'x'.repeat(1024);
			for (let i = 0; i < 256; i++) {
				await db.put(`key-${i.toString().padStart(6, '0')}`, value);
			}
			await db.flush();
			const sstSize = db.getDBIntProperty('rocksdb.total-sst-files-size');
			expect(sstSize).toBeDefined();
			expect(sstSize!).toBeGreaterThan(0);
		}));
});
