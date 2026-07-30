import { type CompressionAlgorithm, RocksDatabase, supportedCompression } from '../src/index.js';
import { normalizeCompression } from '../src/store.js';
import { generateDBPath } from './lib/util.js';
import { execFileSync } from 'node:child_process';
import { readdirSync, rmSync, statSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, describe, expect, it } from 'vitest';

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');

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
// A compressor that honors a compression level, if the build has one.
const levelCompressor = algorithms.find((name) =>
	(['zstd', 'zlib', 'lz4', 'lz4hc', 'bzip2'] as const).includes(name as never)
);

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
				expect(supportedCompression).toContain(db.compression.algorithm);
			} finally {
				db.close();
			}
		});

		it('defaults to LZ4 when the build supports it', () => {
			const db = RocksDatabase.open(tempPath());
			try {
				if (supportedCompression.includes('lz4')) {
					expect(db.compression.algorithm).toBe('lz4');
				} else {
					// No LZ4: falls back to RocksDB's own default (snappy if linked,
					// otherwise none). Either way it must be a supported value.
					expect(supportedCompression).toContain(db.compression.algorithm);
				}
			} finally {
				db.close();
			}
		});

		it('round-trips every supported algorithm through the getter', () => {
			for (const algorithm of algorithms) {
				const db = RocksDatabase.open(tempPath(), { compression: algorithm });
				try {
					expect(db.compression.algorithm).toBe(algorithm);
				} finally {
					db.close();
				}
			}
		});

		it('accepts the object form with an explicit level', () => {
			const algorithm = realCompressor ?? 'none';
			const db = RocksDatabase.open(tempPath(), { compression: { algorithm, level: 6 } });
			try {
				expect(db.compression.algorithm).toBe(algorithm);
			} finally {
				db.close();
			}
		});

		it('can explicitly disable compression', () => {
			const db = RocksDatabase.open(tempPath(), { compression: 'none' });
			try {
				expect(db.compression.algorithm).toBe('none');
			} finally {
				db.close();
			}
		});

		it('should error if database is not open', () => {
			const db = new RocksDatabase(tempPath());
			expect(() => db.compression).toThrow('Database not open');
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
				expect(compressedDb.compression.algorithm).toBe(realCompressor);
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

				expect(dbNone.compression.algorithm).toBe('none');
				expect(dbCompressed.compression.algorithm).toBe(realCompressor);

				dbNone.close();
				dbCompressed.close();

				expect(dirSize(paths.compressed)).toBeLessThan(dirSize(paths.none));
			}
		);
	});

	describe('already-open column family', () => {
		it.skipIf(!realCompressor)(
			'throws when a second open explicitly requests a different compression',
			() => {
				const path = tempPath();
				const dbA = RocksDatabase.open(path, { compression: 'none' });
				try {
					expect(() => RocksDatabase.open(path, { compression: realCompressor })).toThrow(
						/already open with compression/
					);
				} finally {
					dbA.close();
				}
			}
		);

		it.skipIf(!realCompressor)('allows a second open requesting the same compression', () => {
			const path = tempPath();
			const dbA = RocksDatabase.open(path, { compression: realCompressor });
			let dbB: RocksDatabase | undefined;
			try {
				dbB = RocksDatabase.open(path, { compression: realCompressor });
				expect(dbB.compression.algorithm).toBe(realCompressor);
			} finally {
				dbA.close();
				dbB?.close();
			}
		});

		it.skipIf(!realCompressor)(
			'allows a plain reopen without specifying compression (inherits the live setting)',
			() => {
				// The default compression is not "explicit", so a plain reopen must
				// not conflict with the already-open column family's algorithm.
				const path = tempPath();
				const dbA = RocksDatabase.open(path, { compression: realCompressor });
				let dbB: RocksDatabase | undefined;
				try {
					dbB = RocksDatabase.open(path);
					expect(dbB.compression.algorithm).toBe(realCompressor);
				} finally {
					dbA.close();
					dbB?.close();
				}
			}
		);

		it.skipIf(!levelCompressor)(
			'throws when a second open requests a different level for the same algorithm',
			() => {
				const path = tempPath();
				const dbA = RocksDatabase.open(path, {
					compression: { algorithm: levelCompressor!, level: 4 },
				});
				let dbSame: RocksDatabase | undefined;
				try {
					// Same algorithm + level: allowed.
					dbSame = RocksDatabase.open(path, {
						compression: { algorithm: levelCompressor!, level: 4 },
					});
					expect(dbSame.compression).toEqual({ algorithm: levelCompressor, level: 4 });
					// Different level: rejected.
					expect(() =>
						RocksDatabase.open(path, { compression: { algorithm: levelCompressor!, level: 6 } })
					).toThrow(/already open with compression/);
				} finally {
					dbA.close();
					dbSame?.close();
				}
			}
		);
	});

	describe('per-column-family', () => {
		it.skipIf(!realCompressor)(
			'a cold reopen of one column family preserves another CF’s compression',
			() => {
				// Regression: the old shared-cfOptions open stamped the opener's
				// algorithm onto every column family, so first-open order dictated all
				// CFs. Each CF must keep its own algorithm across a close/reopen.
				const path = tempPath();
				const dbPlain = RocksDatabase.open(path, { name: 'plain', compression: 'none' });
				const dbComp = RocksDatabase.open(path, {
					name: 'compressed',
					compression: realCompressor,
				});
				dbPlain.putSync(1, 'x');
				dbComp.putSync(1, 'y');
				dbPlain.flushSync();
				dbComp.flushSync();
				dbPlain.close();
				dbComp.close();

				// Cold-reopen the 'plain' CF FIRST (explicitly 'none'). The old code
				// would have opened every CF with 'none'; per-CF must leave
				// 'compressed' at its own algorithm.
				const dbPlain2 = RocksDatabase.open(path, { name: 'plain', compression: 'none' });
				let dbComp2: RocksDatabase | undefined;
				try {
					expect(dbPlain2.compression.algorithm).toBe('none');
					dbComp2 = RocksDatabase.open(path, { name: 'compressed' }); // plain reopen inherits persisted
					expect(dbComp2.compression.algorithm).toBe(realCompressor);
				} finally {
					dbPlain2.close();
					dbComp2?.close();
				}
			}
		);
	});

	describe('normalizeCompression', () => {
		it('returns empty (native default) for an unset option', () => {
			expect(normalizeCompression(undefined)).toEqual({});
			expect(normalizeCompression(null as never)).toEqual({});
		});

		it('throws for an object without an algorithm', () => {
			expect(() => normalizeCompression({ level: 3 } as never)).toThrow(TypeError);
			expect(() => normalizeCompression({} as never)).toThrow(TypeError);
			expect(() => normalizeCompression([] as never)).toThrow(TypeError);
		});

		it('throws for an unsupported algorithm as string or object', () => {
			expect(() => normalizeCompression('gzip' as never)).toThrow(/Unsupported/);
			expect(() => normalizeCompression({ algorithm: 'gzip' as never })).toThrow(/Unsupported/);
		});

		it('passes through a supported algorithm and level', () => {
			expect(normalizeCompression('none')).toEqual({
				compression: 'none',
				compressionLevel: undefined,
			});
			const algorithm = realCompressor ?? 'none';
			expect(normalizeCompression({ algorithm, level: 6 })).toEqual({
				compression: algorithm,
				compressionLevel: 6,
			});
		});

		it('coerces a numeric-string level and treats null/undefined as omitted', () => {
			expect(normalizeCompression({ algorithm: 'none', level: '5' as never })).toEqual({
				compression: 'none',
				compressionLevel: 5,
			});
			expect(normalizeCompression({ algorithm: 'none', level: null as never })).toEqual({
				compression: 'none',
				compressionLevel: undefined,
			});
		});

		it('throws for a non-integer, out-of-range, or non-numeric level', () => {
			expect(() => normalizeCompression({ algorithm: 'none', level: '9.5' as never })).toThrow(
				TypeError
			);
			expect(() => normalizeCompression({ algorithm: 'none', level: 'abc' as never })).toThrow(
				TypeError
			);
			expect(() => normalizeCompression({ algorithm: 'none', level: 1e10 })).toThrow(TypeError);
		});
	});

	describe('configure-rocksdb.mjs (build script)', () => {
		// node-gyp always runs this script with `node`; under Bun/Deno the test's
		// `process.execPath` would be the wrong runtime (and tsx's CLI isn't
		// Bun-compatible), so only exercise it on the runtime the build uses.
		const nonNodeRuntime = !!process.versions.bun || !!process.versions.deno;
		it.skipIf(nonNodeRuntime)(
			'emits only whitespace-free link tokens (safe for gyp <!@() under spaced paths)',
			() => {
				// binding.gyp splices the script's stdout via <!@(), which splits on
				// whitespace. Every emitted token must therefore be whitespace-free — no
				// absolute path that could carry a space from a spaced checkout dir.
				const out = execFileSync(
					process.execPath,
					[join(repoRoot, 'scripts', 'configure-rocksdb.mjs')],
					{ cwd: repoRoot, encoding: 'utf8' }
				).trim();
				const tokens = out ? out.split('\n') : [];
				for (const token of tokens) {
					expect(token).not.toMatch(/\s/);
					// POSIX: `-l:libX.a` / `-lX`; Windows: `X.lib`.
					expect(token.startsWith('-l') || token.endsWith('.lib')).toBe(true);
				}
			}
		);
	});

	describe('compression getter shape', () => {
		it('omits level when none is configured', () => {
			const db = RocksDatabase.open(tempPath(), { compression: 'none' });
			try {
				expect(db.compression).toEqual({ algorithm: 'none' });
			} finally {
				db.close();
			}
		});

		it.skipIf(!levelCompressor)('returns the configured level', () => {
			const db = RocksDatabase.open(tempPath(), {
				compression: { algorithm: levelCompressor!, level: 5 },
			});
			try {
				expect(db.compression).toEqual({ algorithm: levelCompressor, level: 5 });
			} finally {
				db.close();
			}
		});
	});
});
