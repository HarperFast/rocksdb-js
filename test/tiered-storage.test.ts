import { RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';
import { mkdirSync, readdirSync, renameSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

const tempPaths: string[] = [];

function tempPath(): string {
	const p = generateDBPath();
	tempPaths.push(p);
	return p;
}

function tempDir(): string {
	const p = tempPath();
	mkdirSync(p, { recursive: true });
	return p;
}

function filesWithExt(dir: string, ext: string): string[] {
	try {
		return readdirSync(dir).filter((name) => name.endsWith(ext));
	} catch {
		return [];
	}
}

/** A value large enough to land in a blob file at the default 2 KB threshold. */
function largeValue(seed: number): string {
	return `${seed}:`.padEnd(8192, 'x');
}

afterEach(() => {
	while (tempPaths.length) {
		rmSync(tempPaths.pop()!, { recursive: true, force: true });
	}
});

/**
 * Whether the linked RocksDB carries the downstream blob_dir patch. Without it
 * the option is rejected at open, so those tests are skipped rather than failed.
 */
const blobDirSupported = (() => {
	const probeDb = generateDBPath();
	const probeBlobDir = `${probeDb}-blobs`;
	mkdirSync(probeBlobDir, { recursive: true });
	try {
		RocksDatabase.open(probeDb, { blobs: { dir: probeBlobDir } }).close();
		return true;
	} catch (err: any) {
		// Only the build gate means "unsupported". Swallowing every error would
		// let a real regression in open() silently skip the whole suite.
		if (/requires a RocksDB build with the blob_dir patch/.test(err?.message ?? '')) {
			return false;
		}
		throw err;
	} finally {
		rmSync(probeDb, { recursive: true, force: true });
		rmSync(probeBlobDir, { recursive: true, force: true });
	}
})();

describe('paths', () => {
	it('should place SST files on the configured volume, not the database directory', () => {
		const dbPath = tempPath();
		const fast = tempDir();
		const slow = tempDir();

		const db = RocksDatabase.open(dbPath, {
			paths: [
				{ path: fast, targetSize: 1 << 30 },
				{ path: slow, targetSize: 1 << 30 },
			],
		});

		for (let i = 0; i < 200; i++) {
			db.putSync(`key-${i}`, `value-${i}`);
		}
		db.flushSync();

		// Flush output is hardcoded to path_id 0 in RocksDB's FlushJob, so L0
		// files always land on the first path regardless of target sizes.
		expect(filesWithExt(fast, '.sst').length).toBeGreaterThan(0);
		expect(filesWithExt(dbPath, '.sst')).toHaveLength(0);

		expect(db.getSync('key-0')).toBe('value-0');
		expect(db.getSync('key-199')).toBe('value-199');
		db.close();
	});

	it('should spill compaction output onto a later path', async () => {
		const dbPath = tempPath();
		const fast = tempDir();
		const slow = tempDir();

		// targetSize 0 exhausts the first path immediately, so GetPathId() returns
		// the second path for every level. Note this only governs *compaction*
		// output: flushes go to path 0, and a manual compactSync() would too
		// (CompactRangeOptions::target_path_id defaults to 0), so this drives
		// automatic compaction by producing more L0 files than the trigger.
		const db = RocksDatabase.open(dbPath, {
			paths: [
				{ path: fast, targetSize: 0 },
				{ path: slow, targetSize: 1 << 30 },
			],
			writeBufferSize: 64 * 1024,
		});

		for (let batch = 0; batch < 8; batch++) {
			for (let i = 0; i < 200; i++) {
				db.putSync(`key-${batch}-${i}`, `value-${i}`.padEnd(512, 'x'));
			}
			db.flushSync();
		}

		const deadline = Date.now() + 30_000;
		while (filesWithExt(slow, '.sst').length === 0 && Date.now() < deadline) {
			await new Promise((resolve) => setTimeout(resolve, 100));
		}
		expect(filesWithExt(slow, '.sst').length).toBeGreaterThan(0);

		expect(db.getSync('key-0-0')).toBe(`value-0`.padEnd(512, 'x'));
		expect(db.getSync('key-7-199')).toBe(`value-199`.padEnd(512, 'x'));
		db.close();
	});

	it('should still read data written before a path was appended', () => {
		const dbPath = tempPath();
		const fast = tempDir();
		const slow = tempDir();

		let db = RocksDatabase.open(dbPath, { paths: [{ path: fast, targetSize: 1 << 30 }] });
		for (let i = 0; i < 100; i++) {
			db.putSync(`key-${i}`, `value-${i}`);
		}
		db.flushSync();
		db.close();

		// Appending a path is the supported way to grow onto a new volume:
		// existing files keep path index 0 and stay where they are.
		db = RocksDatabase.open(dbPath, {
			paths: [
				{ path: fast, targetSize: 1 },
				{ path: slow, targetSize: 1 << 30 },
			],
		});
		expect(db.getSync('key-0')).toBe('value-0');
		expect(db.getSync('key-99')).toBe('value-99');
		db.close();
	});

	it('should reject malformed path entries', () => {
		expect(() => RocksDatabase.open(tempPath(), { paths: 'nope' as any })).toThrow(
			/paths must be an array/
		);
		expect(() => RocksDatabase.open(tempPath(), { paths: [{ path: '', targetSize: 0 }] })).toThrow(
			/paths\[0\]\.path must be a non-empty string/
		);
		expect(() =>
			RocksDatabase.open(tempPath(), { paths: [{ path: tempDir(), targetSize: -1 }] })
		).toThrow(/paths\[0\]\.targetSize must be a non-negative integer/);
		expect(() =>
			RocksDatabase.open(tempPath(), { paths: [{ path: tempDir(), targetSize: 1.5 }] })
		).toThrow(/paths\[0\]\.targetSize must be a non-negative integer/);
		// Omitting targetSize would silently mean "spill immediately".
		expect(() => RocksDatabase.open(tempPath(), { paths: [{ path: tempDir() } as any] })).toThrow(
			/paths\[0\]\.targetSize must be a non-negative integer/
		);
	});
});

describe('blobs', () => {
	it('should write blob files alongside the SSTs by default', () => {
		const dbPath = tempPath();
		const db = RocksDatabase.open(dbPath);

		for (let i = 0; i < 20; i++) {
			db.putSync(`key-${i}`, largeValue(i));
		}
		db.flushSync();

		expect(filesWithExt(dbPath, '.blob').length).toBeGreaterThan(0);
		expect(db.getSync('key-0')).toBe(largeValue(0));
		db.close();
	});

	it('should keep values inline when disabled', () => {
		const dbPath = tempPath();
		const db = RocksDatabase.open(dbPath, { blobs: { enabled: false } });

		for (let i = 0; i < 20; i++) {
			db.putSync(`key-${i}`, largeValue(i));
		}
		db.flushSync();

		expect(filesWithExt(dbPath, '.blob')).toHaveLength(0);
		expect(filesWithExt(dbPath, '.sst').length).toBeGreaterThan(0);
		expect(db.getSync('key-5')).toBe(largeValue(5));
		db.close();
	});

	it('should honor minSize', () => {
		const dbPath = tempPath();
		// Well above the 8 KB values written below, so nothing is extracted.
		const db = RocksDatabase.open(dbPath, { blobs: { minSize: 1 << 20 } });

		for (let i = 0; i < 20; i++) {
			db.putSync(`key-${i}`, largeValue(i));
		}
		db.flushSync();

		expect(filesWithExt(dbPath, '.blob')).toHaveLength(0);
		db.close();
	});

	it('should reject an out-of-range garbage collection ratio', () => {
		expect(() =>
			RocksDatabase.open(tempPath(), { blobs: { garbageCollectionAgeCutoff: 1.5 } })
		).toThrow(/garbageCollectionAgeCutoff must be between 0.0 and 1.0/);
		expect(() =>
			RocksDatabase.open(tempPath(), { blobs: { garbageCollectionForceThreshold: -0.1 } })
		).toThrow(/garbageCollectionForceThreshold must be between 0.0 and 1.0/);
	});

	it('should accept a blob cache size', () => {
		const dbPath = tempPath();
		RocksDatabase.config({ blockCacheSize: 16 * 1024 * 1024, blobCacheSize: 8 * 1024 * 1024 });
		try {
			const db = RocksDatabase.open(dbPath, { blobs: { prepopulateCache: true } });
			db.putSync('key', largeValue(1));
			db.flushSync();
			expect(db.getDBIntProperty('rocksdb.blob-cache-capacity')).toBe(8 * 1024 * 1024);
			expect(db.getSync('key')).toBe(largeValue(1));
			db.close();
		} finally {
			RocksDatabase.config({ blockCacheSize: 32 * 1024 * 1024, blobCacheSize: 0 });
		}
	});

	it('should derive blob cache size from block cache size when omitted', () => {
		const dbPath = tempPath();
		let db: ReturnType<typeof RocksDatabase.open> | undefined;
		try {
			RocksDatabase.config({ blobCacheSize: 8 * 1024 * 1024 });
			RocksDatabase.config({ blockCacheSize: 10 * 1024 * 1024 });
			RocksDatabase.config({ compactOnClose: false });
			db = RocksDatabase.open(dbPath, { blobs: { prepopulateCache: true } });
			expect(db.getDBIntProperty('rocksdb.blob-cache-capacity')).toBe(1024 * 1024);
		} finally {
			db?.close();
			RocksDatabase.config({ blockCacheSize: 32 * 1024 * 1024, blobCacheSize: 0 });
		}
	});

	it('should reject a negative blob cache size', () => {
		expect(() => RocksDatabase.config({ blobCacheSize: -1 })).toThrow(/Blob cache size/);
	});
});

describe.skipIf(!blobDirSupported)('blobs.dir', () => {
	it('should place blob files on a separate volume from the SSTs', () => {
		const dbPath = tempPath();
		const blobDir = tempDir();

		const db = RocksDatabase.open(dbPath, { blobs: { dir: blobDir } });
		for (let i = 0; i < 20; i++) {
			db.putSync(`key-${i}`, largeValue(i));
		}
		db.flushSync();

		expect(filesWithExt(blobDir, '.blob').length).toBeGreaterThan(0);
		expect(filesWithExt(dbPath, '.blob')).toHaveLength(0);
		expect(filesWithExt(dbPath, '.sst').length).toBeGreaterThan(0);
		expect(filesWithExt(blobDir, '.sst')).toHaveLength(0);

		for (let i = 0; i < 20; i++) {
			expect(db.getSync(`key-${i}`)).toBe(largeValue(i));
		}
		db.close();
	});

	it('should read blob values back after reopening', () => {
		const dbPath = tempPath();
		const blobDir = tempDir();

		let db = RocksDatabase.open(dbPath, { blobs: { dir: blobDir } });
		db.putSync('key', largeValue(7));
		db.flushSync();
		db.close();

		db = RocksDatabase.open(dbPath, { blobs: { dir: blobDir } });
		expect(db.getSync('key')).toBe(largeValue(7));
		db.close();
	});

	it('should combine with paths so SSTs tier independently of blobs', () => {
		const dbPath = tempPath();
		const fast = tempDir();
		const slow = tempDir();
		const blobDir = tempDir();

		const db = RocksDatabase.open(dbPath, {
			paths: [
				{ path: fast, targetSize: 1 << 30 },
				{ path: slow, targetSize: 1 << 30 },
			],
			blobs: { dir: blobDir },
		});
		for (let i = 0; i < 100; i++) {
			db.putSync(`key-${i}`, largeValue(i));
		}
		db.flushSync();

		expect(filesWithExt(fast, '.sst').length).toBeGreaterThan(0);
		expect(filesWithExt(blobDir, '.blob').length).toBeGreaterThan(0);
		expect(filesWithExt(blobDir, '.sst')).toHaveLength(0);
		expect(filesWithExt(fast, '.blob')).toHaveLength(0);
		expect(filesWithExt(slow, '.blob')).toHaveLength(0);

		expect(db.getSync('key-42')).toBe(largeValue(42));
		db.close();
	});

	it('should refuse to reopen with a different blob directory', () => {
		const dbPath = tempPath();
		const blobDir = tempDir();
		const otherDir = tempDir();

		const db = RocksDatabase.open(dbPath, { blobs: { dir: blobDir } });
		db.putSync('key', largeValue(1));
		db.flushSync();
		db.close();

		// Reopening elsewhere would strand the blob files rather than move
		// them, silently losing every value >= minSize.
		expect(() => RocksDatabase.open(dbPath, { blobs: { dir: otherDir } })).toThrow(
			/blob files were written to/
		);
		// Dropping the option entirely is the same hazard in the other direction.
		expect(() => RocksDatabase.open(dbPath)).toThrow(/blob files were written to/);
	});

	it('should let blob files be relocated while closed', () => {
		const dbPath = tempPath();
		const blobDir = tempDir();
		const movedDir = tempDir();

		let db = RocksDatabase.open(dbPath, { blobs: { dir: blobDir } });
		db.putSync('key', largeValue(3));
		db.flushSync();
		db.close();

		for (const name of filesWithExt(blobDir, '.blob')) {
			renameSync(join(blobDir, name), join(movedDir, name));
		}

		// Moving the files is not enough on its own — the caller has to say they
		// did it, so a plain config edit can never strand them by accident.
		expect(() => RocksDatabase.open(dbPath, { blobs: { dir: movedDir } })).toThrow(
			/blob files were written to/
		);

		db = RocksDatabase.open(dbPath, { blobs: { dir: movedDir, allowDirChange: true } });
		expect(db.getSync('key')).toBe(largeValue(3));
		db.close();

		// Once opened, the new directory is what gets recorded, so the next plain
		// open needs no acknowledgement.
		db = RocksDatabase.open(dbPath, { blobs: { dir: movedDir } });
		expect(db.getSync('key')).toBe(largeValue(3));
		db.close();
	});

	// Harper maps every table to a named column family, so a named CF is the
	// normal path rather than an edge. The first version of this feature only
	// applied blob options to the families listed on disk, which left a named
	// family on the hardcoded defaults and then made the database refuse to
	// reopen with the options it was created with.
	it('should apply the blob directory to a newly created named column family', () => {
		const dbPath = tempPath();
		const blobDir = tempDir();

		const db = RocksDatabase.open(dbPath, { name: 'table1', blobs: { dir: blobDir } });
		for (let i = 0; i < 20; i++) {
			db.putSync(`key-${i}`, largeValue(i));
		}
		db.flushSync();

		expect(filesWithExt(blobDir, '.blob').length).toBeGreaterThan(0);
		expect(filesWithExt(dbPath, '.blob')).toHaveLength(0);
		expect(db.getSync('key-0')).toBe(largeValue(0));
		db.close();
	});

	it('should reopen a named column family with the options it was created with', () => {
		const dbPath = tempPath();
		const blobDir = tempDir();

		let db = RocksDatabase.open(dbPath, { name: 'table1', blobs: { dir: blobDir } });
		db.putSync('key', largeValue(9));
		db.flushSync();
		db.close();

		db = RocksDatabase.open(dbPath, { name: 'table1', blobs: { dir: blobDir } });
		expect(db.getSync('key')).toBe(largeValue(9));
		db.close();
	});

	it('should keep several named column families on the same blob directory', () => {
		const dbPath = tempPath();
		const blobDir = tempDir();

		// Column families of one database share a file-number space, so one blob
		// directory is safe for all of them (unlike two databases).
		const first = RocksDatabase.open(dbPath, { name: 'table1', blobs: { dir: blobDir } });
		const second = RocksDatabase.open(dbPath, { name: 'table2', blobs: { dir: blobDir } });
		first.putSync('a', largeValue(1));
		second.putSync('b', largeValue(2));
		first.flushSync();
		second.flushSync();

		expect(filesWithExt(dbPath, '.blob')).toHaveLength(0);
		expect(filesWithExt(blobDir, '.blob').length).toBeGreaterThan(0);
		expect(first.getSync('a')).toBe(largeValue(1));
		expect(second.getSync('b')).toBe(largeValue(2));
		second.close();
		first.close();
	});

	it('should reject a warm reopen that asks for a different blob directory', () => {
		const dbPath = tempPath();
		const blobDir = tempDir();
		const otherDir = tempDir();

		const db = RocksDatabase.open(dbPath, { name: 'table1', blobs: { dir: blobDir } });
		try {
			// The database is still open, so the request cannot take effect on the
			// reused handle — silently ignoring it would leave the caller believing
			// large values are on another volume until the next restart.
			expect(() =>
				RocksDatabase.open(dbPath, { name: 'table1', blobs: { dir: otherDir } })
			).toThrow(/already open with blobs\.dir/);
		} finally {
			db.close();
		}
	});
});
