import { type CompressionAlgorithm, RocksDatabase, supportedCompression } from '../src/index.ts';
import { normalizeCompression } from '../src/store.ts';
import { generateDBPath } from './lib/util.ts';
import { execFileSync, spawnSync } from 'node:child_process';
import {
	cpSync,
	existsSync,
	mkdirSync,
	readFileSync,
	readdirSync,
	rmSync,
	statSync,
	symlinkSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
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

		it.skipIf(!levelCompressor)(
			'throws when a second open omits the level but the CF is live at a non-default level',
			() => {
				// Omitting the level means "the algorithm's default level", which differs
				// from the live level 4 — so an explicit reopen must conflict rather than
				// silently inherit 4.
				const path = tempPath();
				const dbA = RocksDatabase.open(path, {
					compression: { algorithm: levelCompressor!, level: 4 },
				});
				try {
					expect(() => RocksDatabase.open(path, { compression: levelCompressor! })).toThrow(
						/already open with compression/
					);
				} finally {
					dbA.close();
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

		it.skipIf(!levelCompressor)(
			'a cold reopen with an explicit algorithm and no level resets the persisted level',
			() => {
				// A CF persisted at level 4, cold-reopened as the same algorithm without
				// a level, must fall back to the algorithm's default level (getter omits
				// it) — not silently inherit the persisted 4.
				const path = tempPath();
				const dbA = RocksDatabase.open(path, {
					compression: { algorithm: levelCompressor!, level: 4 },
				});
				dbA.putSync(1, 'x');
				dbA.flushSync();
				dbA.close();

				const dbB = RocksDatabase.open(path, { compression: levelCompressor! });
				try {
					expect(dbB.compression).toEqual({ algorithm: levelCompressor });
				} finally {
					dbB.close();
				}
			}
		);

		it('fails the open when an existing DB’s OPTIONS file cannot be read', () => {
			// The persisted OPTIONS file is the only authoritative source for each
			// CF's compression. If it is missing/corrupt for a DB that already exists,
			// opening every CF with the base defaults would silently restamp the
			// non-target CFs — so the open must fail loudly instead.
			const path = tempPath();
			const db = RocksDatabase.open(path, { name: 'plain', compression: 'none' });
			db.putSync(1, 'x');
			db.close();

			// Remove every OPTIONS-* file. RocksDB still opens the DB (it does not
			// require OPTIONS) and ListColumnFamilies still succeeds from the MANIFEST,
			// but LoadLatestOptions can no longer recover the persisted compression.
			let removed = 0;
			for (const entry of readdirSync(path)) {
				if (entry.startsWith('OPTIONS-')) {
					rmSync(join(path, entry));
					removed++;
				}
			}
			expect(removed).toBeGreaterThan(0);

			expect(() => RocksDatabase.open(path, { name: 'plain', compression: 'none' })).toThrow(
				/persisted column family options/
			);
		});
	});

	describe('normalizeCompression', () => {
		it('returns empty (native default) for an unset option', () => {
			expect(normalizeCompression(undefined)).toEqual({});
			expect(normalizeCompression(null as never)).toEqual({});
		});

		it('treats an object without an algorithm as unset', () => {
			expect(normalizeCompression({} as never)).toEqual({});
			expect(normalizeCompression({ algorithm: undefined } as never)).toEqual({});
			expect(normalizeCompression({ algorithm: null } as never)).toEqual({});
		});

		it('throws for an object with a level but no algorithm', () => {
			expect(() => normalizeCompression({ level: 3 } as never)).toThrow(TypeError);
		});

		it('throws for a non-object, non-string option', () => {
			expect(() => normalizeCompression([] as never)).toThrow(TypeError);
			expect(() => normalizeCompression(6 as never)).toThrow(TypeError);
			expect(() => normalizeCompression(true as never)).toThrow(TypeError);
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

		it('rejects loose non-number levels instead of silently coercing them', () => {
			// `Number()` would turn all of these into a number (true→1, ''→0, []→0,
			// [6]→6) and quietly mis-tune or drop compression. Only a number or a
			// non-blank numeric string is accepted.
			for (const bad of [true, false, '', '   ', [], [6], {}]) {
				expect(() => normalizeCompression({ algorithm: 'none', level: bad as never })).toThrow(
					TypeError
				);
			}
			// A whitespace-padded numeric string is still coerced.
			expect(normalizeCompression({ algorithm: 'none', level: '  6  ' as never })).toEqual({
				compression: 'none',
				compressionLevel: 6,
			});
		});
	});

	describe('configure-rocksdb.mjs (build script)', () => {
		// node-gyp always runs this script with `node`; under Bun/Deno the test's
		// `process.execPath` would be the wrong runtime for the .ts it spawns, so
		// only exercise it on the runtime the build uses.
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

		// A real `node-gyp configure` from a checkout whose path contains a space.
		// The whitespace-free-token check above only exercises the helper; this
		// drives gyp end-to-end so it also covers the gtest.gyp command quoting
		// and binding.gyp's module-relative link inputs — both of which break a
		// spaced checkout differently (a shell-split `<!()` command vs. an
		// unquoted absolute path emitted into LDFLAGS/LIBS).
		const gypBin = join(repoRoot, 'node_modules', 'node-gyp', 'bin', 'node-gyp.js');
		const spacedConfigureRunnable =
			!nonNodeRuntime &&
			process.platform !== 'win32' && // spaces break the make generator; MSVS quotes
			existsSync(join(repoRoot, 'deps', 'rocksdb', 'lib', 'librocksdb.a')) &&
			existsSync(gypBin);

		it.skipIf(!spacedConfigureRunnable)(
			'configures cleanly from a checkout path containing a space',
			() => {
				// A spaced "checkout" that symlinks the heavy trees back to the real
				// repo (nothing copied or downloaded), but keeps the two .gyp files as
				// REAL files: a symlinked .gyp resolves to its real, unspaced path and
				// would defeat the test. module_root_dir becomes the spaced path.
				const spaced = join(tmpdir(), `rocksdb js gyp ${process.pid}`);
				rmSync(spaced, { recursive: true, force: true });
				mkdirSync(join(spaced, 'deps'), { recursive: true });
				try {
					for (const entry of readdirSync(repoRoot)) {
						if (entry === 'deps' || entry === 'build' || entry === '.git') {
							continue;
						}
						if (entry === 'binding.gyp') {
							cpSync(join(repoRoot, entry), join(spaced, entry));
						} else {
							symlinkSync(join(repoRoot, entry), join(spaced, entry));
						}
					}
					for (const entry of readdirSync(join(repoRoot, 'deps'))) {
						const dst = join(spaced, 'deps', entry);
						if (entry === 'gtest.gyp') {
							cpSync(join(repoRoot, 'deps', entry), dst);
						} else {
							symlinkSync(join(repoRoot, 'deps', entry), dst);
						}
					}

					const res = spawnSync(process.execPath, [gypBin, 'configure'], {
						cwd: spaced,
						encoding: 'utf8',
					});
					// A shell-split gtest.gyp command aborts configure here (exit != 0).
					expect(res.stderr + res.stdout).not.toMatch(/No such file or directory/);
					expect(res.status).toBe(0);

					// The generated link settings must carry no absolute path bearing the
					// spaced root (which the link shell would split), and must link
					// rocksdb via a search-dir-resolved `-l` token rather than a path.
					for (const target of ['rocksdb-js.target.mk', 'rocksdb-js-native-tests.target.mk']) {
						const mk = readFileSync(join(spaced, 'build', target), 'utf8');
						expect(mk).not.toContain(spaced);
						expect(mk).toMatch(/-l:?(lib)?rocksdb(\.a)?\b/);
					}
				} finally {
					rmSync(spaced, { recursive: true, force: true });
				}
			},
			120_000
		);
	});

	describe('compressionForAllColumnFamilies', () => {
		// RocksDB opens every column family of a database in one native call, and by default each
		// family the caller did not name keeps its persisted algorithm. A caller whose first open
		// targets some other family — a catalog, say — therefore cannot adopt a codec for the
		// families it did not name: they are already open at the old one, and a family's
		// compression cannot change while it is open.
		const seed = (algorithm: CompressionAlgorithm, names: string[]) => {
			const dbPath = tempPath();
			for (const name of names) {
				const db = RocksDatabase.open(dbPath, { name, compression: algorithm });
				db.putSync('k', 'v');
				db.close();
			}
			return dbPath;
		};

		it.skipIf(!supportedCompression.includes('lz4'))(
			'leaves families the caller did not name at their persisted algorithm by default',
			() => {
				const dbPath = seed('none', ['__catalog__', 'records']);

				const catalog = RocksDatabase.open(dbPath, { name: '__catalog__', compression: 'lz4' });
				try {
					expect(catalog.compression.algorithm).toBe('lz4');
					// `records` was opened transitively alongside it, still at 'none'.
					expect(() => RocksDatabase.open(dbPath, { name: 'records', compression: 'lz4' })).toThrow(
						/already open with compression/
					);
				} finally {
					catalog.close();
				}
			}
		);

		it.skipIf(!supportedCompression.includes('lz4'))(
			'applies the requested algorithm to every family when set',
			() => {
				const dbPath = seed('none', ['__catalog__', 'records', 'orders']);

				const catalog = RocksDatabase.open(dbPath, {
					name: '__catalog__',
					compression: 'lz4',
					compressionForAllColumnFamilies: true,
				});
				try {
					expect(catalog.compression.algorithm).toBe('lz4');
					// Both siblings now reconcile to the same codec instead of conflicting.
					for (const name of ['records', 'orders']) {
						const sibling = RocksDatabase.open(dbPath, { name, compression: 'lz4' });
						try {
							expect(sibling.compression.algorithm).toBe('lz4');
						} finally {
							sibling.close();
						}
					}
				} finally {
					catalog.close();
				}
			}
		);

		it('requires an explicit algorithm', () => {
			const dbPath = seed('none', ['__catalog__']);
			expect(() =>
				RocksDatabase.open(dbPath, { name: '__catalog__', compressionForAllColumnFamilies: true })
			).toThrow(/requires an explicit compression option/);
		});
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
