/**
 * Configure-time RocksDB step for the native build. `binding.gyp` runs this via
 * `<!@()` and does two things with it:
 *
 *   1. Provisions the pinned RocksDB prebuild (delegates to scripts/init-rocksdb),
 *      failing the build hard if that cannot be done — configure runs before the
 *      `prepare-rocksdb` build action, so the prebuild must be present here for
 *      the enumeration below to be correct.
 *   2. Emits the RocksDB link libraries — the core `librocksdb` archive plus the
 *      compression static libs — one per line, for the current platform; gyp
 *      splices them into the link settings.
 *
 * RocksDB prebuilds vary in which compression libraries they were compiled
 * with: an older prebuild may ship only zlib, while a compression-enabled one
 * ships snappy/lz4/zstd/bzip2 as well. `librocksdb` references whichever it was
 * built with but (being a static archive) does not bundle them, so the consumer
 * must link exactly the ones the prebuild provides — no more, no less. Linking
 * a lib the prebuild doesn't ship fails with "no such file"; omitting one it
 * needs fails with "undefined symbols". So we link precisely the files present.
 *
 * The core `librocksdb` is emitted first (static link order: it depends on the
 * compression libs, so it must precede them) as a `-l` flag / `.lib` name rather
 * than an absolute path — a repo checked out under a path with spaces (e.g.
 * `/tmp/rocks db`) would otherwise emit a whitespace-bearing library entry that
 * gyp's `<!@()` split and the link shell both break on. All tokens resolve
 * against the (single, gyp-supplied, space-safe) library search dir.
 */

import { spawnSync } from 'node:child_process';
import { readdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const isWin = process.platform === 'win32';
const isLinux = process.platform === 'linux';
const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const libDir = join(root, 'deps', 'rocksdb', 'lib');

// The core RocksDB archive. Always present after provisioning; linked first.
const coreLib = isWin ? 'rocksdb.lib' : 'librocksdb.a';

const candidates = isWin
	? ['snappy.lib', 'lz4.lib', 'zstd.lib', 'bz2.lib', 'zs.lib']
	: ['libsnappy.a', 'liblz4.a', 'libzstd.a', 'libbz2.a', 'libz.a'];

// Run the .ts script directly with the current Node executable — its native
// type stripping needs no tsx. Passing the absolute script path as a single
// argv element (no shell) is space-safe and identical across platforms.
const result = spawnSync(process.execPath, [join(root, 'scripts', 'init-rocksdb', 'main.ts')], {
	cwd: root,
	stdio: ['ignore', 'ignore', 'inherit'],
});

// Provisioning is a hard prerequisite: if it failed, enumerating below would
// emit a stale or empty lib set and defer the failure to a confusing link
// error. A non-zero exit here aborts gyp configure. init-rocksdb prints its own
// diagnostics to inherited stderr.
if (result.error) {
	// Node itself could not be launched (e.g. ENOENT).
	console.error(`Failed to run init-rocksdb: ${result.error.message}`);
	process.exit(1);
}
if (result.status !== 0) {
	process.exit(result.status ?? 1);
}

// init-rocksdb succeeded, so deps/rocksdb/lib exists (both the download and
// build-from-source paths create it). An empty enumeration is legitimate — a
// build-from-source tree ships only librocksdb.a, and a none-only prebuild has
// no compression archives — but a missing dir despite a successful prep is an
// invariant violation, so fail loudly rather than emit a misleading empty list.
let files;
try {
	files = new Set(readdirSync(libDir));
} catch (error) {
	console.error(`init-rocksdb succeeded but ${libDir} is missing: ${error.message}`);
	process.exit(1);
}
// The core archive is a hard invariant post-provision — its absence means a
// broken prebuild, so fail loudly rather than emit a link set that omits it.
if (!files.has(coreLib)) {
	console.error(`init-rocksdb succeeded but ${join(libDir, coreLib)} is missing`);
	process.exit(1);
}
const present = candidates.filter((name) => files.has(name));

// One token per line; gyp's `<!@()` splits the output on whitespace into a list,
// so every token MUST be whitespace-free — an absolute path under a repo checked
// out to `/tmp/rocks db` would otherwise be split into broken entries. We emit
// only names/flags and let `binding.gyp` supply the (single, gyp-quoted, so
// space-safe) library search directory:
//   - Windows: bare `rocksdb.lib`/`snappy.lib` names, resolved via `AdditionalLibraryDirectories`.
//   - Linux:   `-l:librocksdb.a` — force the exact static archive from the search dir.
//   - macOS:   `-lrocksdb` — ld64 has no `-l:`, but the search dir holds only the
//              `.a` (no dylib), so the static archive is selected.
const toToken = (name) => {
	if (isWin) {
		return name;
	}
	if (isLinux) {
		return `-l:${name}`;
	}
	// libsnappy.a -> -lsnappy
	return '-l' + name.replace(/^lib/, '').replace(/\.a$/, '');
};
// Core first (it depends on the compression libs), then the present compression libs.
const tokens = [coreLib, ...present].map(toToken);
process.stdout.write(tokens.join('\n'));
