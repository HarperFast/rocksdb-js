/**
 * Emits the compression static libraries to link against the RocksDB prebuild,
 * one per line, for the current platform. `binding.gyp` runs this at configure
 * time via `<!@()` and splices the result into the link settings.
 *
 * RocksDB prebuilds vary in which compression libraries they were compiled
 * with: an older prebuild may ship only zlib, while a compression-enabled one
 * ships snappy/lz4/zstd/bzip2 as well. `librocksdb` references whichever it was
 * built with but (being a static archive) does not bundle them, so the consumer
 * must link exactly the ones the prebuild provides — no more, no less. Linking
 * a lib the prebuild doesn't ship fails with "no such file"; omitting one it
 * needs fails with "undefined symbols". So we link precisely the files present.
 *
 * `librocksdb` itself is linked unconditionally by `binding.gyp`; only the
 * optional compression libs are enumerated here.
 */

import { spawnSync } from 'node:child_process';
import { readdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const isWin = process.platform === 'win32';
const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const libDir = join(root, 'deps', 'rocksdb', 'lib');

// Compression archives a prebuild MAY ship. `zs.lib` is zlib on the Windows
// builds (the POSIX counterpart is `libz.a`).
const candidates = isWin
	? ['snappy.lib', 'lz4.lib', 'zstd.lib', 'bz2.lib', 'zs.lib']
	: ['libsnappy.a', 'liblz4.a', 'libzstd.a', 'libbz2.a', 'libz.a'];

// Reconcile deps/rocksdb to the pinned version BEFORE enumerating. This runs at
// gyp configure time, which is before the prepare-rocksdb build action, so the
// installed prebuild may be absent OR a different version than package.json
// pins — either way the lib set on disk would be stale. init-rocksdb downloads
// (or builds) the pinned version on a mismatch and no-ops when already correct,
// so the enumeration below reflects the version that will actually be linked.
// We must always run it, not just when librocksdb is missing: a wrong-version
// librocksdb is present but has the wrong compression lib set. Its stdout is
// discarded — only the library list may reach gyp on our stdout. `shell` on
// Windows so the `.cmd` shim resolves (mirrors scripts/native-test/run.mjs).
spawnSync(
	join(root, 'node_modules', '.bin', 'tsx'),
	[join(root, 'scripts', 'init-rocksdb', 'main.ts')],
	{
		cwd: root,
		stdio: ['ignore', 'ignore', 'inherit'],
		shell: isWin,
	}
);

let present = [];
try {
	const files = new Set(readdirSync(libDir));
	present = candidates.filter((name) => files.has(name));
} catch {
	// deps/rocksdb/lib still missing (prep failed); emit nothing. The static
	// librocksdb path in binding.gyp will surface a clear link error.
}

// One path per line; gyp's `<!@()` splits on whitespace into a list. POSIX
// needs full paths (used directly in `libraries`); Windows uses bare names
// resolved via `AdditionalLibraryDirectories`.
const tokens = present.map((name) => (isWin ? name : join(libDir, name)));
process.stdout.write(tokens.join('\n'));
