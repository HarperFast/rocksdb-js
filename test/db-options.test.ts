import { RocksDatabase, type RocksDBCompression } from '../src/index.js';
import { dbRunner, generateDBPath } from './lib/util.js';
import { rmSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

function isCompressionSupported(compression: RocksDBCompression): boolean {
	const path = generateDBPath();
	const db = new RocksDatabase(path, { compression });
	try {
		db.open();
		return true;
	} catch (error) {
		if (error instanceof Error && error.message.includes('not supported by this RocksDB build')) {
			return false;
		}
		throw error;
	} finally {
		db.close();
		rmSync(path, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
	}
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

	it('should apply the current handle memory options to a late column family', () =>
		dbRunner(
			{
				dbOptions: [
					{ writeBufferSize: 64 * 1024 * 1024 },
					{ name: 'late', writeBufferSize: 64 * 1024 },
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

				// Do not force a flush: automatic flush behavior is what distinguishes
				// the late handle's requested 64 KiB buffer from the first opener's 64 MiB.
				const deadline = Date.now() + 5000;
				let lateBufferSize = 0;
				while (Date.now() < deadline) {
					lateBufferSize = late.getDBIntProperty('rocksdb.total-sst-files-size') ?? 0;
					if (lateBufferSize > 0) {
						break;
					}
					await new Promise((resolve) => setTimeout(resolve, 50));
				}

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

// Highly compressible so zlib wins decisively, and below min_blob_size (2048)
// so the values stay in SST blocks — blob files are not affected by this option.
const COMPRESSIBLE_VALUE = 'a'.repeat(1000);

/**
 * Writes a compressible payload and returns the resulting on-disk SST size.
 * Comparing this between two otherwise-identical databases is the only way to
 * observe that compression was really applied: an ignored `compression` option
 * still reads and writes correctly, just without shrinking anything.
 */
async function writeCompressiblePayload(db: RocksDatabase): Promise<number> {
	for (let i = 0; i < 1500; i++) {
		await db.put(`key-${i.toString().padStart(6, '0')}`, COMPRESSIBLE_VALUE);
	}
	await db.flush();
	const size = db.getDBIntProperty('rocksdb.total-sst-files-size');
	expect(size).toBeGreaterThan(0);
	return size!;
}

const zlibSupported = isCompressionSupported('zlib');
const zlibIt = zlibSupported ? it : it.skip;

describe('Database compression options', () => {
	it('should resolve true to zlib when supported and reject it otherwise', () =>
		dbRunner({ dbOptions: [{ compression: true }], skipOpen: true }, async ({ db }) => {
			if (zlibSupported) {
				db.open();
				await db.put('foo', 'bar');
				expect(await db.get('foo')).toBe('bar');
			} else {
				expect(() => db.open()).toThrow('not supported by this RocksDB build');
			}
		}));

	zlibIt('should round-trip data with an explicit zlib algorithm', () =>
		dbRunner({ dbOptions: [{ compression: 'zlib' }] }, async ({ db }) => {
			await db.put('foo', 'bar');
			expect(await db.get('foo')).toBe('bar');
		})
	);

	it('should round-trip data with compression disabled (false -> none)', () =>
		dbRunner({ dbOptions: [{ compression: false }] }, async ({ db }) => {
			await db.put('foo', 'bar');
			expect(await db.get('foo')).toBe('bar');
		}));

	it('should reject an unknown compression type on open', () =>
		dbRunner({ dbOptions: [{ compression: 'gzip' as any }], skipOpen: true }, async ({ db }) => {
			expect(() => db.open()).toThrow('Unknown compression type');
			// The message must point at what this build can actually use.
			expect(() => db.open()).toThrow('Available in this build');
		}));

	it('should reject an unknown compression type when the path is already open', () =>
		dbRunner(
			{ dbOptions: [{}, { compression: 'gzip' as any, name: 'other' }], skipOpen: true },
			async ({ db: first }, { db: second }) => {
				first.open();
				// Validation must not depend on this open being the one that
				// creates the descriptor.
				expect(() => second.open()).toThrow('Unknown compression type');
			}
		));

	it('should reject an explicitly empty compression instead of treating it as omitted', () =>
		dbRunner(
			{ dbOptions: [{}, { compression: '' as any, name: 'other' }], skipOpen: true },
			async ({ db: first }, { db: second }) => {
				first.open();
				expect(() => second.open()).toThrow('Unknown compression type');
			}
		));

	zlibIt('should write smaller table files with zlib than with no compression', () =>
		dbRunner(
			{
				dbOptions: [
					{ path: generateDBPath(), compression: 'none', writeBufferSize: 64 * 1024 },
					{ path: generateDBPath(), compression: 'zlib', writeBufferSize: 64 * 1024 },
				],
			},
			async ({ db: uncompressed }, { db: compressed }) => {
				const uncompressedSize = await writeCompressiblePayload(uncompressed);
				const compressedSize = await writeCompressiblePayload(compressed);
				expect(compressedSize).toBeLessThan(uncompressedSize);

				// Data still reads back correctly through the compressed path.
				expect(await compressed.get('key-000000')).toBe(COMPRESSIBLE_VALUE);
				expect(await compressed.get('key-001499')).toBe(COMPRESSIBLE_VALUE);
			}
		)
	);

	// A named column family does not exist on a fresh path, so it is created
	// after `DB::Open` rather than being handed the open options. It must still
	// get the requested compression.
	zlibIt('should compress a named column family created at open time', () =>
		dbRunner(
			{
				dbOptions: [
					{ path: generateDBPath(), name: 'mycf', compression: 'none' },
					{ path: generateDBPath(), name: 'mycf', compression: 'zlib' },
				],
			},
			async ({ db: uncompressed }, { db: compressed }) => {
				const uncompressedSize = await writeCompressiblePayload(uncompressed);
				const compressedSize = await writeCompressiblePayload(compressed);
				expect(compressedSize).toBeLessThan(uncompressedSize);
				expect(await compressed.get('key-000000')).toBe(COMPRESSIBLE_VALUE);
			}
		)
	);

	// Same requirement for a column family added to a database that is already
	// open, which goes through the registry rather than `DBDescriptor::open`.
	zlibIt('should compress a column family added to an already-open database', () => {
		const uncompressedPath = generateDBPath();
		const compressedPath = generateDBPath();
		return dbRunner(
			{
				dbOptions: [
					{ path: uncompressedPath, compression: 'none' },
					{ path: uncompressedPath, name: 'late' },
					{ path: compressedPath, compression: 'zlib' },
					{ path: compressedPath, name: 'late' },
				],
			},
			async (_base, { db: lateUncompressed }, _compressedBase, { db: lateCompressed }) => {
				const uncompressedSize = await writeCompressiblePayload(lateUncompressed);
				const compressedSize = await writeCompressiblePayload(lateCompressed);
				expect(compressedSize).toBeLessThan(uncompressedSize);
				expect(await lateCompressed.get('key-000000')).toBe(COMPRESSIBLE_VALUE);
			}
		);
	});

	zlibIt('should reject a conflicting compression on an already-open path', () =>
		dbRunner(
			{
				dbOptions: [{ compression: 'none' }, { compression: 'zlib', name: 'other' }],
				skipOpen: true,
			},
			async ({ db: first }, { db: second }) => {
				first.open();
				// All handles on a path share one RocksDB instance, so the second
				// request cannot be honored — reject instead of silently ignoring it.
				expect(() => second.open()).toThrow('Database already open with compression');
			}
		)
	);

	it('should not treat an omitted compression and an explicit none as a conflict', () =>
		dbRunner(
			{
				dbOptions: [
					{ compression: false },
					{ name: 'omittedSecond' },
					{ compression: false, name: 'noneSecond' },
				],
				skipOpen: true,
			},
			async ({ db: first }, { db: omittedSecond }, { db: noneSecond }) => {
				first.open();
				expect(() => omittedSecond.open()).not.toThrow();
				expect(() => noneSecond.open()).not.toThrow();
			}
		));

	it('should compare explicit none against the effective omitted compression', () =>
		dbRunner(
			{ dbOptions: [{}, { compression: false, name: 'other' }], skipOpen: true },
			async ({ db: first }, { db: second }) => {
				const snappySupported = isCompressionSupported('snappy');
				first.open();
				if (snappySupported) {
					expect(() => second.open()).toThrow('Database already open with compression');
				} else {
					expect(() => second.open()).not.toThrow();
				}
			}
		));

	zlibIt('should allow reopening an open path with a matching or omitted compression', () =>
		dbRunner(
			{
				dbOptions: [
					{ compression: 'zlib' },
					{ compression: 'zlib', name: 'matching' },
					{ name: 'omitted' },
				],
				skipOpen: true,
			},
			async ({ db: first }, { db: matching }, { db: omitted }) => {
				first.open();
				expect(() => matching.open()).not.toThrow();
				expect(() => omitted.open()).not.toThrow();
			}
		)
	);
});
