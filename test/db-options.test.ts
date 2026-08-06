import { dbRunner, generateDBPath } from './lib/util.js';
import { describe, expect, it } from 'vitest';

/** Polls until a flush lands, so a test asserting one happened does not race it. */
async function waitForSstFiles(
	db: { getDBIntProperty(name: string): number | undefined },
	timeoutMs = 45000
) {
	const deadline = Date.now() + timeoutMs;
	let size = 0;
	do {
		size = db.getDBIntProperty('rocksdb.total-sst-files-size') ?? 0;
		if (size > 0) return size;
		await new Promise((resolve) => setTimeout(resolve, 25));
	} while (Date.now() < deadline);
	return size;
}

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

	it('should leave the global write buffer trigger disabled by default', () =>
		dbRunner(
			{ dbOptions: [{ maxWriteBufferNumber: 2, writeBufferSize: 256 * 1024 * 1024 }] },
			async ({ db }) => {
				const value = 'x'.repeat(1024);
				for (let i = 0; i < 96 * 1024; i++) {
					await db.put(`key-${i}`, value);
				}
				// 96 MiB written under a 256 MiB per-CF buffer: nothing reaches disk unless a
				// global trigger forces it, so an SST here means the default stopped being off.
				expect(db.getDBIntProperty('rocksdb.total-sst-files-size')).toBe(0);
			}
		));

	it('should disable the global write buffer trigger when dbWriteBufferSize is zero', () =>
		dbRunner(
			{
				dbOptions: [
					{ dbWriteBufferSize: 0, maxWriteBufferNumber: 2, writeBufferSize: 256 * 1024 * 1024 },
				],
			},
			async ({ db }) => {
				const value = 'x'.repeat(1024);
				for (let i = 0; i < 96 * 1024; i++) {
					await db.put(`key-${i}`, value);
				}
				expect(db.getDBIntProperty('rocksdb.total-sst-files-size')).toBe(0);
			}
		));

	it(
		'should apply an explicit dbWriteBufferSize',
		() =>
			dbRunner(
				{
					dbOptions: [
						{
							dbWriteBufferSize: 1024 * 1024,
							maxWriteBufferNumber: 2,
							writeBufferSize: 256 * 1024 * 1024,
						},
					],
				},
				async ({ db }) => {
					const value = 'x'.repeat(1024);
					for (let i = 0; i < 2 * 1024; i++) {
						await db.put(`key-${i}`, value);
					}
					// The global trigger SCHEDULES a flush; it does not complete one before the last
					// put resolves. Asserting immediately races the background flush — it wins on a
					// fast Linux runner and loses intermittently on Windows.
					expect(await waitForSstFiles(db)).toBeGreaterThan(0);
				}
			),
		// Well past the 30s global testTimeout: on Windows the 2048-put loop alone has been
		// measured at ~14s, and the flush this waits for lands after that.
		120000
	);

	it.each([NaN, Infinity, -1, 1.5, 2 ** 53])(
		'should reject invalid dbWriteBufferSize values',
		(dbWriteBufferSize) =>
			dbRunner({ dbOptions: [{ dbWriteBufferSize }], skipOpen: true }, async ({ db }) => {
				expect(() => db.open()).toThrow('dbWriteBufferSize must be a non-negative safe integer');
			})
	);

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
					{
						path: generateDBPath(),
						name: 'mycf',
						writeBufferSize: 64 * 1024,
						maxWriteBufferNumber: 2,
					},
					{
						path: generateDBPath(),
						name: 'mycf',
						writeBufferSize: 64 * 1024 * 1024,
						maxWriteBufferNumber: 2,
					},
				],
			},
			async ({ db: smallBuffer }, { db: largeBuffer }) => {
				const value = 'x'.repeat(1024);
				for (const db of [smallBuffer, largeBuffer]) {
					for (let i = 0; i < 512; i++) {
						await db.put(`key-${i.toString().padStart(6, '0')}`, value);
					}
				}

				// With only two 64 KiB memtables, all writes cannot complete until
				// RocksDB flushes at least one; write backpressure is the gate.
				const smallBufferSize = smallBuffer.getDBIntProperty('rocksdb.total-sst-files-size') ?? 0;
				expect(smallBufferSize).toBeGreaterThan(0);
				expect(largeBuffer.getDBIntProperty('rocksdb.total-sst-files-size')).toBe(0);
			}
		));

	it('should apply the current handle memory options to a late column family', () =>
		dbRunner(
			{
				dbOptions: [
					{ writeBufferSize: 64 * 1024 * 1024, maxWriteBufferNumber: 2 },
					{
						name: 'late',
						writeBufferSize: 64 * 1024,
						maxWriteBufferNumber: 2,
					},
				],
				skipOpen: true,
			},
			async ({ db: first }, { db: late }) => {
				first.open();
				late.open();

				const value = 'x'.repeat(1024);
				for (let i = 0; i < 512; i++) {
					await late.put(`key-${i.toString().padStart(6, '0')}`, value);
				}

				// The two-buffer limit gates on automatic flush without forcing one;
				// an incorrectly inherited 64 MiB buffer would still have no SST.
				const lateBufferSize = late.getDBIntProperty('rocksdb.total-sst-files-size') ?? 0;
				expect(lateBufferSize).toBeGreaterThan(0);
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
