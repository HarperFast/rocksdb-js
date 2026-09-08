import { RocksDatabase } from '../src/index.ts';
import { generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { rmSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, describe, expect, it } from 'vitest';

const fixtureDir = join(dirname(fileURLToPath(import.meta.url)), 'fixtures');

const dbPaths: string[] = [];
const openDatabases: RocksDatabase[] = [];

function open(options?: Parameters<typeof RocksDatabase.open>[1]): RocksDatabase {
	const path = generateDBPath();
	dbPaths.push(path);
	const db = RocksDatabase.open(path, options);
	openDatabases.push(db);
	return db;
}

afterEach(() => {
	while (openDatabases.length) {
		try {
			openDatabases.pop()!.close();
		} catch {
			// Already closed.
		}
	}
	while (dbPaths.length) {
		rmSync(dbPaths.pop()!, { recursive: true, force: true });
	}
	// The native DBSettings singleton is process-global (shared across vitest's
	// worker threads), so leave the caches as the rest of the suite expects.
	RocksDatabase.config({ blockCacheSize: 32 * 1024 * 1024, blobCacheSize: 0 });
});

describe('Blob Cache', () => {
	it('should derive from the block cache size until set explicitly', () => {
		const basePath = generateDBPath();
		dbPaths.push(`${basePath}-a`, `${basePath}-b`);

		return new Promise<void>((resolve, reject) => {
			// A child process, because the "set explicitly" flag is latched and
			// process-global — see the fixture.
			const child = spawn(
				process.execPath,
				[join(fixtureDir, 'blob-cache-derivation.mts'), basePath],
				{ stdio: ['ignore', 'ignore', 'inherit'] }
			);
			child.on('close', (code) => {
				try {
					expect(code).toBe(0);
					resolve();
				} catch (error) {
					reject(error);
				}
			});
			child.on('error', reject);
		});
	});

	it('should honor an explicit blob cache size', () => {
		RocksDatabase.config({ blockCacheSize: 16 * 1024 * 1024, blobCacheSize: 4 * 1024 * 1024 });
		const db = open({ blobs: { prepopulateCache: true } });
		const value = 'x'.repeat(8192);
		db.putSync('key', value);
		db.flushSync();
		expect(db.getDBIntProperty('rocksdb.blob-cache-capacity')).toBe(4 * 1024 * 1024);
		expect(db.getSync('key')).toBe(value);
	});

	it('should keep an explicit blob cache size across a later block-cache-only call', () => {
		RocksDatabase.config({ blobCacheSize: 8 * 1024 * 1024 });
		// Says nothing about blobs, so it must not discard the stated budget by
		// re-deriving 10% of the new block cache.
		RocksDatabase.config({ blockCacheSize: 10 * 1024 * 1024 });
		const db = open({ blobs: { prepopulateCache: true } });
		expect(db.getDBIntProperty('rocksdb.blob-cache-capacity')).toBe(8 * 1024 * 1024);
	});

	it('should treat an undefined blob cache size as omitted', () => {
		RocksDatabase.config({ blobCacheSize: 2 * 1024 * 1024 });
		// An optional config object spells "not set" as `undefined`. Rejecting it
		// used to throw *after* the block-cache resize had already been applied.
		expect(() =>
			RocksDatabase.config({ blockCacheSize: 12 * 1024 * 1024, blobCacheSize: undefined })
		).not.toThrow();
		const db = open({ blobs: { prepopulateCache: true } });
		expect(db.getDBIntProperty('rocksdb.block-cache-capacity')).toBe(12 * 1024 * 1024);
		expect(db.getDBIntProperty('rocksdb.blob-cache-capacity')).toBe(2 * 1024 * 1024);
	});

	it('should not attach the blob cache to a noBlockCache database', () => {
		RocksDatabase.config({ blockCacheSize: 16 * 1024 * 1024, blobCacheSize: 4 * 1024 * 1024 });
		// noBlockCache means "this database does not use the process-wide caches",
		// so a scratch database cannot evict the serving database's blob values.
		const db = open({ noBlockCache: true });
		expect(db.getDBIntProperty('rocksdb.blob-cache-capacity')).toBeUndefined();
	});

	it('should reopen a column family that asked to prepopulate a cache that does not exist', () => {
		RocksDatabase.config({ blobCacheSize: 0 });
		const path = generateDBPath();
		dbPaths.push(path);

		const first = RocksDatabase.open(path, { name: 't1', blobs: { prepopulateCache: true } });
		openDatabases.push(first);
		// The request is recorded on the family even with no cache to prepopulate
		// (it is inert, not invalid), so an identical second open must not read as
		// a conflict — every table is opened repeatedly across worker envs, and a
		// mismatch here would be a startup failure on an unchanged configuration.
		expect(() => {
			openDatabases.push(
				RocksDatabase.open(path, { name: 't1', blobs: { prepopulateCache: true } })
			);
		}).not.toThrow();
	});

	it('should keep a persisted prepopulateCache across a cold open with no blob cache', () => {
		const path = generateDBPath();
		dbPaths.push(path);

		return new Promise<void>((resolve, reject) => {
			// A child process, because every other suite in this process may have
			// configured a blob cache — see the fixture.
			const child = spawn(
				process.execPath,
				[join(fixtureDir, 'blob-prepopulate-persistence.mts'), path],
				{ stdio: ['ignore', 'ignore', 'inherit'] }
			);
			child.on('close', (code) => {
				try {
					expect(code).toBe(0);
					resolve();
				} catch (error) {
					reject(error);
				}
			});
			child.on('error', reject);
		});
	});

	it('should reject a negative blob cache size', () => {
		expect(() => RocksDatabase.config({ blobCacheSize: -1 })).toThrow(/Blob cache size/);
	});

	it('should reject a non-numeric blob cache size without resizing the block cache', () => {
		RocksDatabase.config({ blockCacheSize: 24 * 1024 * 1024 });
		expect(() =>
			RocksDatabase.config({ blockCacheSize: 48 * 1024 * 1024, blobCacheSize: '512MB' as any })
		).toThrow(/Blob cache size/);
		// The whole call is validated before either cache is touched.
		const db = open();
		expect(db.getDBIntProperty('rocksdb.block-cache-capacity')).toBe(24 * 1024 * 1024);
	});
});
