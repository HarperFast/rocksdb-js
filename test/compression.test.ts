import { type CompressionAlgorithm, RocksDatabase, supportedCompression } from '../src/index.js';
import { generateDBPath } from './lib/util.js';
import { readdirSync, rmSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

const tempPaths: string[] = [];

function tempPath(): string {
	const p = generateDBPath();
	tempPaths.push(p);
	return p;
}

afterEach(() => {
	while (tempPaths.length) {
		rmSync(tempPaths.pop()!, { recursive: true, force: true });
	}
});

/** Total size in bytes of every file under a directory (recursively). */
function dirSize(dir: string): number {
	let total = 0;
	for (const entry of readdirSync(dir, { withFileTypes: true })) {
		const p = join(dir, entry.name);
		if (entry.isDirectory()) {
			total += dirSize(p);
		} else {
			try {
				total += statSync(p).size;
			} catch {
				// file may vanish during background compaction; ignore
			}
		}
	}
	return total;
}

/**
 * Writes highly compressible records, then flushes and compacts so the data is
 * materialized into SST/blob files using the column family's compression.
 */
function populate(db: RocksDatabase, count: number): void {
	const filler = 'the quick brown fox jumps over the lazy dog '.repeat(24);
	for (let i = 0; i < count; ++i) {
		db.putSync(i, {
			id: i,
			name: `record-${i}`,
			note: filler,
			tags: ['alpha', 'beta', 'gamma', 'delta'],
		});
	}
	db.flushSync();
	db.compactSync();
}

// A real (non-"none") compressor available in this build, if any. The local
// dev prebuild may link only a subset (e.g. none + zlib), so size-comparison
// tests are conditional on one being present.
const algorithms = supportedCompression as readonly CompressionAlgorithm[];
const realCompressor = algorithms.find((name) => name !== 'none');

describe('Compression', () => {
	describe('supportedCompression', () => {
		it('is a non-empty, frozen list that always includes "none"', () => {
			expect(Array.isArray(supportedCompression)).toBe(true);
			expect(supportedCompression.length).toBeGreaterThan(0);
			expect(supportedCompression).toContain('none');
			expect(Object.isFrozen(supportedCompression)).toBe(true);
		});
	});

	describe('compression getter / defaults', () => {
		it('reports a supported algorithm by default', () => {
			const db = RocksDatabase.open(tempPath());
			try {
				expect(supportedCompression).toContain(db.compression);
			} finally {
				db.close();
			}
		});

		it('defaults to LZ4 when the build supports it', () => {
			const db = RocksDatabase.open(tempPath());
			try {
				if (supportedCompression.includes('lz4')) {
					expect(db.compression).toBe('lz4');
				} else {
					// No LZ4: falls back to RocksDB's own default (snappy if linked,
					// otherwise none). Either way it must be a supported value.
					expect(supportedCompression).toContain(db.compression);
				}
			} finally {
				db.close();
			}
		});

		it('round-trips every supported algorithm through the getter', () => {
			for (const algorithm of algorithms) {
				const db = RocksDatabase.open(tempPath(), { compression: algorithm });
				try {
					expect(db.compression).toBe(algorithm);
				} finally {
					db.close();
				}
			}
		});

		it('accepts the object form with an explicit level', () => {
			const algorithm = realCompressor ?? 'none';
			const db = RocksDatabase.open(tempPath(), { compression: { algorithm, level: 6 } });
			try {
				expect(db.compression).toBe(algorithm);
			} finally {
				db.close();
			}
		});

		it('can explicitly disable compression', () => {
			const db = RocksDatabase.open(tempPath(), { compression: 'none' });
			try {
				expect(db.compression).toBe('none');
			} finally {
				db.close();
			}
		});
	});

	describe('validation', () => {
		it('throws for an unsupported algorithm name', () => {
			expect(() => RocksDatabase.open(tempPath(), { compression: 'gzip' as never })).toThrow(
				/Unsupported compression algorithm "gzip"/
			);
		});

		it('throws for an unsupported algorithm in the object form', () => {
			expect(() =>
				RocksDatabase.open(tempPath(), { compression: { algorithm: 'brotli' as never } })
			).toThrow(/Unsupported compression algorithm "brotli"/);
		});
	});

	describe('on-disk size', () => {
		it.skipIf(!realCompressor)(
			`compresses data smaller than "none" (using ${realCompressor})`,
			() => {
				const nonePath = tempPath();
				const compressedPath = tempPath();

				const noneDb = RocksDatabase.open(nonePath, { compression: 'none' });
				populate(noneDb, 10_000);
				noneDb.close();

				const compressedDb = RocksDatabase.open(compressedPath, {
					compression: realCompressor,
				});
				populate(compressedDb, 10_000);
				expect(compressedDb.compression).toBe(realCompressor);
				compressedDb.close();

				const noneSize = dirSize(nonePath);
				const compressedSize = dirSize(compressedPath);
				expect(compressedSize).toBeLessThan(noneSize);
			}
		);

		it.skipIf(!realCompressor)(
			'keeps multiple databases with different compression settings independent',
			() => {
				// The issue explicitly asks to verify multiple databases with
				// different settings populated and compared side by side.
				const paths = {
					none: tempPath(),
					compressed: tempPath(),
				};

				const dbNone = RocksDatabase.open(paths.none, { compression: 'none' });
				const dbCompressed = RocksDatabase.open(paths.compressed, {
					compression: realCompressor,
				});

				// Populate both while they are open simultaneously to prove the
				// per-database setting is not shared global state.
				populate(dbNone, 10_000);
				populate(dbCompressed, 10_000);

				expect(dbNone.compression).toBe('none');
				expect(dbCompressed.compression).toBe(realCompressor);

				dbNone.close();
				dbCompressed.close();

				expect(dirSize(paths.compressed)).toBeLessThan(dirSize(paths.none));
			}
		);
	});
});
