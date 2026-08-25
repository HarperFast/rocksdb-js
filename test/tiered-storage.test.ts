import { backups, RocksDatabase } from '../src/index.js';
import { generateDBPath } from './lib/util.js';
import { randomBytes } from 'node:crypto';
import {
	copyFileSync,
	existsSync,
	mkdirSync,
	readdirSync,
	readFileSync,
	renameSync,
	rmSync,
} from 'node:fs';
import { isAbsolute, join, relative as relativePath } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

const tempPaths: string[] = [];
const openDatabases: RocksDatabase[] = [];

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

/**
 * A temp directory under the process working directory rather than `os.tmpdir()`.
 * The two can be on different drives on Windows, where `path.relative` between
 * them returns an ABSOLUTE path — so the case these callers test, "a relative
 * path is resolved against the process working directory", cannot be expressed
 * from a tmpdir at all.
 */
function cwdTempDir(): string {
	const p = join(process.cwd(), `rocksdb-js-cwd-${randomBytes(8).toString('hex')}`);
	tempPaths.push(p);
	mkdirSync(p, { recursive: true });
	return p;
}

/**
 * Opens a database the `afterEach` hook is guaranteed to close. A bare
 * `db.close()` after the assertions does not run when one of them fails, and the
 * hook would then delete the database directory, the `paths` volumes and the
 * blob directory out from under a live RocksDB with background compaction
 * running — turning one clean assertion failure into follow-on noise.
 */
function openDb(path: string, options?: Parameters<typeof RocksDatabase.open>[1]): RocksDatabase {
	const db = RocksDatabase.open(path, options);
	openDatabases.push(db);
	return db;
}

function filesWithExt(dir: string, ext: string): string[] {
	try {
		return readdirSync(dir).filter((name) => name.endsWith(ext));
	} catch {
		return [];
	}
}

/**
 * The `[CFOptions "<name>"]` section of the database's newest OPTIONS file.
 * RocksDB rewrites it on open and on every column-family creation, so this is
 * what a later open of that family will actually restore — which is what makes
 * an option leaking between families outlive the process.
 */
function persistedCFOptions(dbPath: string, name: string): Record<string, string> {
	const optionsFiles = readdirSync(dbPath)
		.filter((file) => file.startsWith('OPTIONS-'))
		.sort();
	const text = readFileSync(join(dbPath, optionsFiles.at(-1)!), 'utf8');
	const section = text.split(`[CFOptions "${name}"]`)[1]?.split('\n[')[0] ?? '';
	const result: Record<string, string> = {};
	for (const line of section.split('\n')) {
		const eq = line.indexOf('=');
		if (eq > 0) {
			result[line.slice(0, eq).trim()] = line.slice(eq + 1).trim();
		}
	}
	return result;
}

/** A value large enough to land in a blob file at the default 2 KB threshold. */
function largeValue(seed: number): string {
	return `${seed}:`.padEnd(8192, 'x');
}

afterEach(() => {
	// Close before removing anything: see openDb().
	while (openDatabases.length) {
		try {
			openDatabases.pop()!.close();
		} catch {
			// Already closed, or closed as part of the failure under test.
		}
	}
	while (tempPaths.length) {
		rmSync(tempPaths.pop()!, { recursive: true, force: true });
	}
});

/**
 * Whether the linked RocksDB carries the downstream blob_dir patch. Without it
 * the option is rejected at open, so those tests are skipped rather than failed.
 */
const requireBlobDir = process.env.ROCKSDB_JS_REQUIRE_BLOB_DIR === '1';

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
			// A prebuild that lost the patch on one platform would otherwise turn
			// the whole suite green by skipping it. The release job sets this so
			// the packaged artifact has to carry the feature it advertises.
			if (requireBlobDir) {
				throw new Error(
					'ROCKSDB_JS_REQUIRE_BLOB_DIR=1 but the linked RocksDB has no blob_dir patch'
				);
			}
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

		const db = openDb(dbPath, {
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
		const db = openDb(dbPath, {
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

		let db = openDb(dbPath, { paths: [{ path: fast, targetSize: 1 << 30 }] });
		for (let i = 0; i < 100; i++) {
			db.putSync(`key-${i}`, `value-${i}`);
		}
		db.flushSync();
		db.close();

		// Appending a path is the supported way to grow onto a new volume:
		// existing files keep path index 0 and stay where they are.
		db = openDb(dbPath, {
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

	it('should resolve a relative path against the process working directory', () => {
		const dbPath = tempPath();
		const fast = cwdTempDir();
		const relative = relativePath(process.cwd(), fast);
		expect(isAbsolute(relative)).toBe(false);

		const db = openDb(dbPath, { paths: [{ path: relative, targetSize: 1 << 30 }] });
		db.putSync('key', 'value');
		db.flushSync();

		// Resolved in the JavaScript layer, so the native side never has to turn a
		// UTF-8 string into a std::filesystem::path (which re-encodes through the
		// active code page on Windows).
		expect(filesWithExt(fast, '.sst').length).toBeGreaterThan(0);
		expect(db.getSync('key')).toBe('value');
	});

	it('should refuse to add paths to a database that already has SST files of its own', () => {
		const dbPath = tempPath();
		const fast = tempDir();

		let db = openDb(dbPath);
		for (let i = 0; i < 200; i++) {
			db.putSync(`key-${i}`, `value-${i}`);
		}
		db.flushSync();
		db.close();
		expect(filesWithExt(dbPath, '.sst').length).toBeGreaterThan(0);

		// Those files carry path index 0, which this request redefines as `fast`.
		// Left to RocksDB it fails with a MANIFEST corruption error, which sends an
		// operator to backup restore rather than to the config line they changed.
		expect(() =>
			RocksDatabase.open(dbPath, { paths: [{ path: fast, targetSize: 1 << 30 }] })
		).toThrow(/already has SST files in its own directory/);

		// The supported migration: keep the database directory as paths[0].
		db = openDb(dbPath, {
			paths: [
				{ path: dbPath, targetSize: 1 << 30 },
				{ path: fast, targetSize: 1 << 30 },
			],
		});
		expect(db.getSync('key-0')).toBe('value-0');
		expect(db.getSync('key-199')).toBe('value-199');
	});

	it('should refuse to add paths when only some SST files are reachable', () => {
		const dbPath = tempPath();
		const fast = tempDir();

		const db = openDb(dbPath);
		for (let batch = 0; batch < 2; batch++) {
			for (let i = 0; i < 100; i++) {
				db.putSync(`key-${batch}-${i}`, `value-${i}`);
			}
			db.flushSync();
		}
		db.close();

		const ssts = filesWithExt(dbPath, '.sst');
		expect(ssts.length).toBeGreaterThan(1);
		// A half-finished manual copy, or a colliding file number under a paths[0]
		// shared with another database: one reachable file must not vouch for the
		// rest.
		copyFileSync(join(dbPath, ssts[0]!), join(fast, ssts[0]!));

		expect(() =>
			RocksDatabase.open(dbPath, { paths: [{ path: fast, targetSize: 1 << 30 }] })
		).toThrow(/already has SST files in its own directory/);
	});

	it('should reject backup and checkpoint while paths is configured', async () => {
		const dbPath = tempPath();
		const fast = tempDir();

		// RocksDB's GetLiveFilesStorageInfo — which both BackupEngine and
		// Checkpoint go through — refuses outright when db_paths is set. It is not
		// that the copy comes out flat: there is no copy at all, for ANY non-empty
		// `paths`, including the one-entry `[{ path: <database directory> }]` form
		// the migration section recommends.
		const db = openDb(dbPath, { paths: [{ path: fast, targetSize: 1 << 30 }] });
		db.putSync('key', 'value');
		db.flushSync();

		await expect(db.backup(tempDir())).rejects.toThrow(
			/db_paths \/ cf_paths not supported for Checkpoint nor BackupEngine/
		);
		await expect(db.createCheckpoint(tempPath())).rejects.toThrow(
			/db_paths \/ cf_paths not supported for Checkpoint nor BackupEngine/
		);
	});

	it('should delete tiered SST files on destroy()', () => {
		const dbPath = tempPath();
		const fast = tempDir();

		const db = openDb(dbPath, { paths: [{ path: fast, targetSize: 1 << 30 }] });
		for (let i = 0; i < 200; i++) {
			db.putSync(`key-${i}`, `value-${i}`);
		}
		db.flushSync();
		expect(filesWithExt(fast, '.sst').length).toBeGreaterThan(0);

		// A default rocksdb::Options describes "everything under the database
		// directory", which stopped being true once SSTs can live on another
		// volume — destroy() used to orphan exactly the bulk of the data.
		db.destroy();
		expect(filesWithExt(fast, '.sst')).toHaveLength(0);
	});

	it('should name paths when a removed or reordered list breaks the open', () => {
		const dbPath = tempPath();
		const fast = tempDir();
		const slow = tempDir();

		let db = openDb(dbPath, {
			paths: [
				{ path: fast, targetSize: 0 },
				{ path: slow, targetSize: 1 << 30 },
			],
		});
		for (let i = 0; i < 200; i++) {
			db.putSync(`key-${i}`, `value-${i}`);
		}
		db.flushSync();
		db.close();
		expect(filesWithExt(dbPath, '.sst')).toHaveLength(0);

		// Nothing on disk records which list the MANIFEST's path indexes were
		// written against, so neither of these is detectable up front the way the
		// zero-to-one transition is. RocksDB reports the MANIFEST as corrupt,
		// which reads as data loss; both are recoverable by putting the list back.
		expect(() => RocksDatabase.open(dbPath)).toThrow(/opened with `paths`, that same list/);
		expect(() =>
			RocksDatabase.open(dbPath, {
				paths: [
					{ path: slow, targetSize: 0 },
					{ path: fast, targetSize: 1 << 30 },
				],
			})
		).toThrow(/opened with `paths`, that same list/);

		db = openDb(dbPath, {
			paths: [
				{ path: fast, targetSize: 0 },
				{ path: slow, targetSize: 1 << 30 },
			],
		});
		expect(db.getSync('key-0')).toBe('value-0');
	});

	it('should delete tiered SST files when destroy() follows close()', () => {
		const dbPath = tempPath();
		const fast = tempDir();

		const db = openDb(dbPath, { paths: [{ path: fast, targetSize: 1 << 30 }] });
		for (let i = 0; i < 200; i++) {
			db.putSync(`key-${i}`, `value-${i}`);
		}
		db.flushSync();
		expect(filesWithExt(fast, '.sst').length).toBeGreaterThan(0);

		// destroy() accepts a closed handle, and closing the last one takes the
		// descriptor and its registry entry with it. `db_paths` is never written
		// to the OPTIONS file, so nothing on disk can put it back: without the
		// handle's own layout snapshot the database directory goes and every
		// tiered SST file stays, with destroy() reporting success.
		db.close();
		db.destroy();
		expect(filesWithExt(fast, '.sst')).toHaveLength(0);
		expect(existsSync(dbPath)).toBe(false);
	});

	it('should reject more storage paths than it will hold', () => {
		const dbPath = tempPath();
		// The native parser reserves against whatever length JS reports, and a
		// sparse array makes an enormous one free — the reserve then throws a C++
		// allocation exception out of the N-API callback, which terminates the
		// process rather than rejecting the open. Bounding the count first is what
		// keeps that a JS error. Driven with a merely-too-long array: at a length
		// near 2**32 the `paths.map()` in store.ts walks every index and takes
		// minutes, so the astronomical case never reaches native to begin with.
		const paths = Array.from({ length: 65 }, () => ({ path: dbPath, targetSize: 1 << 30 }));
		expect(() => RocksDatabase.open(dbPath, { paths })).toThrow(/no more than 64 entries/);
	});

	it('should name the offending entry when it is not an object', () => {
		const dbPath = tempPath();
		// Reading a property off null leaves N-API's own "Cannot convert null to
		// object" pending, which would swallow the specific message.
		expect(() => RocksDatabase.open(dbPath, { paths: [null] as any })).toThrow(
			/paths\[0\] must be a \{ path, targetSize \} object/
		);
	});
});

describe('blobs', () => {
	it('should write blob files alongside the SSTs by default', () => {
		const dbPath = tempPath();
		const db = openDb(dbPath);

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
		const db = openDb(dbPath, { blobs: { enabled: false } });

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
		const db = openDb(dbPath, { blobs: { minSize: 1 << 20 } });

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

	// The blob CACHE tests live in test/blob-cache.test.ts: the "explicitly set"
	// flag they exercise is process-global and latched, so they need a process
	// that has never configured one.

	it('should keep each column family on its own blob settings', () => {
		const dbPath = tempPath();

		// t2 has to be IN the OPTIONS file before t1's cold open, or that open
		// creates it fresh and there is nothing for the restamping to act on —
		// the test would then pass with the restore removed.
		openDb(dbPath, { name: 't2' }).close();

		// Cold-open t1 with a threshold far above the 8 KB values written below.
		// RocksDB opens every family at once, so this open is what would carry
		// t1's threshold onto t2 and persist it.
		const first = openDb(dbPath, { name: 't1', blobs: { minSize: 1 << 20 } });

		// Asserted against the OPTIONS file rather than only through behavior:
		// the restamp is what gets PERSISTED here, and a later plain open of t2
		// asks for the 2 KB default anyway, so it writes the right value back and
		// hides the damage until something reopens t2 through t1 again.
		expect(persistedCFOptions(dbPath, 't1').min_blob_size).toBe(String(1 << 20));
		expect(persistedCFOptions(dbPath, 't2').min_blob_size).toBe('2048');
		first.close();

		// A cold open of t1 must not restamp t2 with t1's threshold: in Harper
		// every table is a named column family, so that would let whichever table
		// happened to open the database first decide blob extraction for all of
		// them, and flip it on restart.
		const second = openDb(dbPath, { name: 't2' });
		for (let i = 0; i < 20; i++) {
			second.putSync(`key-${i}`, largeValue(i));
		}
		second.flushSync();
		expect(filesWithExt(dbPath, '.blob').length).toBeGreaterThan(0);
	});

	it("should keep a warm-created family off the first opener's blob settings", () => {
		const dbPath = tempPath();

		// A family created on an already-open database builds its options on the
		// descriptor's own retained base, so any field not assigned in BOTH
		// directions rides along from whichever handle opened the database first.
		// `prepopulate_blob_cache` was the one such field, and it is persisted, so
		// the inheritance survived restarts. In the Harper shape — every table a
		// column family — a table that never opted in warmed the shared blob cache
		// on every flush.
		const first = openDb(dbPath, {
			name: 't1',
			blobs: { prepopulateCache: true, minSize: 1 << 20 },
		});
		openDb(dbPath, { name: 't2' });

		expect(persistedCFOptions(dbPath, 't1').prepopulate_blob_cache).toBe('kFlushOnly');
		expect(persistedCFOptions(dbPath, 't2').prepopulate_blob_cache).toBe('kDisable');
		// A field that was already assigned unconditionally, as the control: t2
		// gets the creation default rather than t1's request either way.
		expect(persistedCFOptions(dbPath, 't2').min_blob_size).toBe('2048');
		first.close();
	});

	it("should inherit a column family's persisted blob settings on a plain reopen", () => {
		const dbPath = tempPath();

		const db = openDb(dbPath, { name: 't1', blobs: { minSize: 1 << 20 } });
		db.close();

		// No `blobs` in the request: like compression, the family keeps what it
		// persisted rather than being restamped with the default 2 KB threshold.
		const reopened = openDb(dbPath, { name: 't1' });
		for (let i = 0; i < 20; i++) {
			reopened.putSync(`key-${i}`, largeValue(i));
		}
		reopened.flushSync();
		expect(filesWithExt(dbPath, '.blob')).toHaveLength(0);
	});

	it("should not stamp a named family's blob settings onto the default family", () => {
		const dbPath = tempPath();

		// Creating the database through a named family also creates `default` on
		// the way. It must be created with the blob defaults, not with t1's
		// request — otherwise whichever table created the database decides
		// `default`'s settings forever.
		const first = openDb(dbPath, { name: 't1', blobs: { minSize: 1 << 20 } });
		first.close();

		const db = openDb(dbPath);
		for (let i = 0; i < 20; i++) {
			db.putSync(`key-${i}`, largeValue(i));
		}
		db.flushSync();
		expect(filesWithExt(dbPath, '.blob').length).toBeGreaterThan(0);
	});

	it('should round-trip the garbage collection settings through a cold reopen', () => {
		const dbPath = tempPath();
		const settings = {
			garbageCollection: true,
			garbageCollectionAgeCutoff: 0.5,
			garbageCollectionForceThreshold: 0.75,
		};

		const first = openDb(dbPath, { name: 't1', blobs: settings });
		first.close();

		// Cold-opening a DIFFERENT family reopens t1 from its OPTIONS file. These
		// fields are hand-copied through three sites (creation defaults, persisted
		// restore, explicit apply), so a dropped or transposed one survives every
		// on-disk assertion — but not the warm conflict check below, which
		// compares the live column family against the same request.
		const second = openDb(dbPath, { name: 't2' });
		expect(second.getSync('missing')).toBeUndefined();

		expect(() => openDb(dbPath, { name: 't1', blobs: settings })).not.toThrow();
		// ...and a genuinely different value still conflicts, so the check above is
		// not passing for the wrong reason.
		expect(() =>
			RocksDatabase.open(dbPath, {
				name: 't1',
				blobs: { ...settings, garbageCollectionAgeCutoff: 0.9 },
			})
		).toThrow(/garbageCollectionAgeCutoff/);
	});

	it('should reject a warm reopen that asks for different blob settings', () => {
		const dbPath = tempPath();
		const db = openDb(dbPath, { name: 't1', blobs: { minSize: 4096 } });
		// The database is still open, so the request cannot take effect on the
		// reused handle.
		expect(() => RocksDatabase.open(dbPath, { name: 't1', blobs: { minSize: 8192 } })).toThrow(
			/already open with different blob settings/
		);
		expect(db.getSync('missing')).toBeUndefined();
	});
});

describe.skipIf(!blobDirSupported)('blobs.dir', () => {
	it('should place blob files on a separate volume from the SSTs', () => {
		const dbPath = tempPath();
		const blobDir = tempDir();

		const db = openDb(dbPath, { blobs: { dir: blobDir } });
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

		let db = openDb(dbPath, { blobs: { dir: blobDir } });
		db.putSync('key', largeValue(7));
		db.flushSync();
		db.close();

		db = openDb(dbPath, { blobs: { dir: blobDir } });
		expect(db.getSync('key')).toBe(largeValue(7));
		db.close();
	});

	it('should combine with paths so SSTs tier independently of blobs', () => {
		const dbPath = tempPath();
		const fast = tempDir();
		const slow = tempDir();
		const blobDir = tempDir();

		const db = openDb(dbPath, {
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

		const db = openDb(dbPath, { blobs: { dir: blobDir } });
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

		let db = openDb(dbPath, { blobs: { dir: blobDir } });
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

		db = openDb(dbPath, { blobs: { dir: movedDir, allowDirChange: true } });
		expect(db.getSync('key')).toBe(largeValue(3));
		db.close();

		// Once opened, the new directory is what gets recorded, so the next plain
		// open needs no acknowledgement.
		db = openDb(dbPath, { blobs: { dir: movedDir } });
		expect(db.getSync('key')).toBe(largeValue(3));
		db.close();
	});

	// Harper maps every table to a named column family, so a named CF is the
	// normal path rather than an edge.
	it('should apply the blob directory to a newly created named column family', () => {
		const dbPath = tempPath();
		const blobDir = tempDir();

		const db = openDb(dbPath, { name: 'table1', blobs: { dir: blobDir } });
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

		let db = openDb(dbPath, { name: 'table1', blobs: { dir: blobDir } });
		db.putSync('key', largeValue(9));
		db.flushSync();
		db.close();

		db = openDb(dbPath, { name: 'table1', blobs: { dir: blobDir } });
		expect(db.getSync('key')).toBe(largeValue(9));
		db.close();
	});

	it('should keep several named column families on the same blob directory', () => {
		const dbPath = tempPath();
		const blobDir = tempDir();

		// Column families of one database share a file-number space, so one blob
		// directory is safe for all of them (unlike two databases).
		const first = openDb(dbPath, { name: 'table1', blobs: { dir: blobDir } });
		const second = openDb(dbPath, { name: 'table2', blobs: { dir: blobDir } });
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

		const db = openDb(dbPath, { name: 'table1', blobs: { dir: blobDir } });
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

	it('should keep each column family on its own blob directory', () => {
		const dbPath = tempPath();
		const blobDir = tempDir();

		let db = openDb(dbPath, { name: 'table1', blobs: { dir: blobDir } });
		db.putSync('key', largeValue(1));
		db.flushSync();
		db.close();

		// table2 wants its blobs alongside the SSTs. A cold open of it must not
		// restamp table1's directory — blob placement is per column family, and
		// RocksDB restores none of it on open.
		db = openDb(dbPath, { name: 'table2' });
		db.putSync('key', largeValue(2));
		db.flushSync();
		expect(filesWithExt(dbPath, '.blob').length).toBeGreaterThan(0);
		db.close();

		// Would throw "its blob files were written to ..." if table1 had been
		// restamped to the default directory by table2's open.
		db = openDb(dbPath, { name: 'table1', blobs: { dir: blobDir } });
		expect(db.getSync('key')).toBe(largeValue(1));
	});

	it('should resolve a relative blob directory', () => {
		const dbPath = tempPath();
		const blobDir = cwdTempDir();
		const relative = relativePath(process.cwd(), blobDir);
		expect(isAbsolute(relative)).toBe(false);

		let db = openDb(dbPath, { blobs: { dir: relative } });
		db.putSync('key', largeValue(4));
		db.flushSync();
		expect(filesWithExt(blobDir, '.blob').length).toBeGreaterThan(0);
		db.close();

		// The absolute form is what gets persisted, so the relative spelling of
		// the same directory reopens cleanly rather than reading as a mismatch.
		db = openDb(dbPath, { blobs: { dir: relative } });
		expect(db.getSync('key')).toBe(largeValue(4));
	});

	it('should recover large values from a flat backup with allowDirChange', async () => {
		const dbPath = tempPath();
		const blobDir = tempDir();
		const backupDir = tempDir();
		const restoreDir = tempPath();

		const db = openDb(dbPath, { blobs: { dir: blobDir } });
		for (let i = 0; i < 20; i++) {
			db.putSync(`key-${i}`, largeValue(i));
		}
		db.flushSync();
		await db.backup(backupDir);
		db.close();

		await backups.restore(backupDir, restoreDir);
		// The copy is flat, but its OPTIONS still names the old directory, so a
		// plain open is rejected rather than reading large values that are not
		// there. This is the procedure docs/tiered-storage.md documents.
		expect(filesWithExt(restoreDir, '.blob').length).toBeGreaterThan(0);
		expect(() => RocksDatabase.open(restoreDir)).toThrow(/blob files were written to/);

		// And the acknowledgement is refused while the SOURCE's blob directory is
		// still populated: from inside the process the copy is indistinguishable
		// from the original, so this is what keeps the two databases off one
		// directory. Restore where the source's directory is not reachable.
		expect(() => RocksDatabase.open(restoreDir, { blobs: { allowDirChange: true } })).toThrow(
			/still has blob files in/
		);

		rmSync(blobDir, { recursive: true, force: true });
		const restored = openDb(restoreDir, { blobs: { allowDirChange: true } });
		for (let i = 0; i < 20; i++) {
			expect(restored.getSync(`key-${i}`)).toBe(largeValue(i));
		}
	});

	it('should create a blob directory for a column family added to an open database', () => {
		const dbPath = tempPath();
		// Deliberately NOT created up front: nothing in RocksDB creates it, and a
		// missing one does not fail anything synchronously — writes are
		// acknowledged and the FIRST FLUSH errors the whole database read-only.
		// The cold open creates it; a family added to an already-open database
		// goes through CreateColumnFamily instead and used to skip that.
		const blobDir = tempPath();

		const plain = openDb(dbPath);
		const table = openDb(dbPath, { name: 'table1', blobs: { dir: blobDir } });
		for (let i = 0; i < 20; i++) {
			table.putSync(`key-${i}`, largeValue(i));
		}
		table.flushSync();

		expect(filesWithExt(blobDir, '.blob').length).toBeGreaterThan(0);
		expect(table.getSync('key-0')).toBe(largeValue(0));
		// A background error would have flipped every family read-only, not just
		// the one that wrote the blobs.
		plain.putSync('still-writable', 'yes');
		expect(plain.getSync('still-writable')).toBe('yes');
		table.close();
		plain.close();
	});

	it("should refuse to open when a column family's blob directory is gone", () => {
		const dbPath = tempPath();
		const blobDir = tempDir();

		const db = openDb(dbPath, { name: 'table1', blobs: { dir: blobDir } });
		db.putSync('key', largeValue(6));
		db.flushSync();
		db.close();

		rmSync(blobDir, { recursive: true, force: true });

		// Opening a family this call never names still has to notice: its reads
		// would fail and its first flush would error the database read-only, long
		// after the open that could have named the volume.
		expect(() => RocksDatabase.open(dbPath)).toThrow(/which does not exist/);
	});

	it('should relocate every column family when the change is acknowledged', () => {
		const dbPath = tempPath();
		const blobDir = tempDir();
		const movedDir = tempDir();

		let first = openDb(dbPath, { name: 'table1', blobs: { dir: blobDir } });
		let second = openDb(dbPath, { name: 'table2', blobs: { dir: blobDir } });
		first.putSync('a', largeValue(1));
		second.putSync('b', largeValue(2));
		first.flushSync();
		second.flushSync();
		second.close();
		first.close();

		for (const name of filesWithExt(blobDir, '.blob')) {
			renameSync(join(blobDir, name), join(movedDir, name));
		}

		// Moving blob files is a database-wide act — a closed database's files all
		// move together — so the acknowledgement has to reach every family. Scoped
		// to the named one, table2 would still point at the now-empty old
		// directory, and it could not be repaired in this process: the second open
		// is a warm one, which cannot change a live family's directory.
		first = openDb(dbPath, {
			name: 'table1',
			blobs: { dir: movedDir, allowDirChange: true },
		});
		second = openDb(dbPath, { name: 'table2', blobs: { dir: movedDir } });
		expect(first.getSync('a')).toBe(largeValue(1));
		expect(second.getSync('b')).toBe(largeValue(2));
		second.close();
		first.close();

		// And it is persisted, so the next plain open needs no acknowledgement.
		second = openDb(dbPath, { name: 'table2', blobs: { dir: movedDir } });
		expect(second.getSync('b')).toBe(largeValue(2));
		second.close();
	});

	it('should leave a column family alone whose blob files did not move', () => {
		const dbPath = tempPath();
		const blobDir = tempDir();
		const movedDir = tempDir();

		// The normal Harper shape: `default` keeps its blobs alongside the SSTs and
		// a named table has its own volume. Only table1's files are relocated.
		let plain = openDb(dbPath);
		let table = openDb(dbPath, { name: 'table1', blobs: { dir: blobDir } });
		plain.putSync('flat', largeValue(1));
		table.putSync('tiered', largeValue(2));
		plain.flushSync();
		table.flushSync();
		table.close();
		plain.close();

		expect(filesWithExt(dbPath, '.blob').length).toBeGreaterThan(0);
		for (const name of filesWithExt(blobDir, '.blob')) {
			renameSync(join(blobDir, name), join(movedDir, name));
		}

		// The acknowledgement reaches other families, but only as far as the move
		// went: `default`'s blobs never left the database directory, so re-pointing
		// it at movedDir would strand every one of its large values.
		table = openDb(dbPath, {
			name: 'table1',
			blobs: { dir: movedDir, allowDirChange: true },
		});
		plain = openDb(dbPath);
		expect(table.getSync('tiered')).toBe(largeValue(2));
		expect(plain.getSync('flat')).toBe(largeValue(1));

		// And `default` keeps writing where its files already are.
		plain.putSync('more', largeValue(3));
		plain.flushSync();
		expect(plain.getSync('more')).toBe(largeValue(3));
		plain.close();
		table.close();
	});

	it('should refuse to relocate before the blob files have moved', () => {
		const dbPath = tempPath();
		const blobDir = tempDir();
		const movedDir = tempDir();

		const db = openDb(dbPath, { blobs: { dir: blobDir } });
		// Two flushes so there is more than one blob file to leave half-copied.
		db.putSync('key', largeValue(11));
		db.flushSync();
		db.putSync('key2', largeValue(12));
		db.flushSync();
		db.close();

		// Acknowledging a move that has not happened yet — the open runs before the
		// `mv`, or against the wrong destination. Persisting the new directory here
		// makes every large value read as missing until a compaction turns it into
		// a background error. The destination existing is not a signal: the open
		// creates it.
		expect(() =>
			RocksDatabase.open(dbPath, { blobs: { dir: movedDir, allowDirChange: true } })
		).toThrow(/still has blob files in/);

		const names = filesWithExt(blobDir, '.blob');
		expect(names.length).toBeGreaterThan(1);
		// A half-finished copy is refused too. The destination holding some files
		// is not evidence the move completed, and persisting it strands every value
		// still behind — which is the state an interrupted `rsync` leaves.
		copyFileSync(join(blobDir, names[0]), join(movedDir, names[0]));
		expect(() =>
			RocksDatabase.open(dbPath, { blobs: { dir: movedDir, allowDirChange: true } })
		).toThrow(/still has blob files in/);

		for (const name of names) {
			renameSync(join(blobDir, name), join(movedDir, name));
		}
		const moved = openDb(dbPath, { blobs: { dir: movedDir, allowDirChange: true } });
		expect(moved.getSync('key')).toBe(largeValue(11));
		expect(moved.getSync('key2')).toBe(largeValue(12));
		moved.close();
	});

	it('should refuse to flatten a database whose blob files have not moved', () => {
		const dbPath = tempPath();
		const blobDir = tempDir();

		const plain = openDb(dbPath);
		const table = openDb(dbPath, { name: 'table1', blobs: { dir: blobDir } });
		table.putSync('key', largeValue(8));
		table.flushSync();
		table.close();
		plain.close();

		// `{ blobs: { allowDirChange: true } }` is character-for-character what the
		// restore procedure tells operators to type. Aimed at the live original by
		// a wrong path argument, taking the claim on trust would clear table1's
		// directory while its blob files are still in it, so the claim is checked:
		// the recorded directory still holds blob files, so nothing was flattened.
		expect(() => RocksDatabase.open(dbPath, { blobs: { allowDirChange: true } })).toThrow(
			/still has blob files in/
		);
		// Also when the tiered family is the one named. The claim is checked
		// against what each family PERSISTED, because the target's own request has
		// already been applied to it by then.
		expect(() =>
			RocksDatabase.open(dbPath, { name: 'table1', blobs: { allowDirChange: true } })
		).toThrow(/still has blob files in/);
	});

	it('should read a restored named column family from its own flattened copy', async () => {
		const dbPath = tempPath();
		const blobDir = tempDir();
		const backupDir = tempDir();
		const restoreDir = tempPath();

		// `default` has no blob directory and `table1` does, so the restored copy's
		// target family carries no mismatch of its own — the source directory can
		// only be noticed through the families this open does not name.
		const plain = openDb(dbPath);
		const table = openDb(dbPath, { name: 'table1', blobs: { dir: blobDir } });
		for (let i = 0; i < 20; i++) {
			table.putSync(`key-${i}`, largeValue(i));
		}
		table.flushSync();
		await plain.backup(backupDir);

		await backups.restore(backupDir, restoreDir);
		expect(filesWithExt(restoreDir, '.blob').length).toBeGreaterThan(0);

		// The source is still open, and its blob directory is exactly where the
		// restored table1 would point if the acknowledgement were taken on trust —
		// the two databases would then allocate blob file numbers there
		// independently and each one's obsolete-file scan would delete the other's
		// live files. Refused, which is the whole reason the claim is checked. The
		// target here is `default`, which carries no mismatch of its own, so this
		// is caught only through the family the open does not name.
		expect(() => RocksDatabase.open(restoreDir, { blobs: { allowDirChange: true } })).toThrow(
			/still has blob files in/
		);
		table.close();
		plain.close();

		// Once the source's directory is out of reach the copy stands alone.
		rmSync(blobDir, { recursive: true, force: true });
		const restoredPlain = openDb(restoreDir, { blobs: { allowDirChange: true } });
		const restoredTable = openDb(restoreDir, { name: 'table1' });
		for (let i = 0; i < 20; i++) {
			expect(restoredTable.getSync(`key-${i}`)).toBe(largeValue(i));
		}
		restoredTable.putSync('added', largeValue(99));
		restoredTable.flushSync();
		// Everything the restored database writes stays in its own directory.
		expect(filesWithExt(restoreDir, '.blob').length).toBeGreaterThan(0);
		expect(restoredTable.getSync('added')).toBe(largeValue(99));

		restoredTable.close();
		restoredPlain.close();
	});

	it('should delete blob files on destroy()', () => {
		const dbPath = tempPath();
		const blobDir = tempDir();

		const db = openDb(dbPath, { blobs: { dir: blobDir } });
		db.putSync('key', largeValue(5));
		db.flushSync();
		expect(filesWithExt(blobDir, '.blob').length).toBeGreaterThan(0);

		db.destroy();
		expect(filesWithExt(blobDir, '.blob')).toHaveLength(0);
	});
});
