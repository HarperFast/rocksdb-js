#!/usr/bin/env node
// Compiles and runs the standalone partial-writev unit test
// (test/binding/writev_partial_test.cpp). Skipped on Windows; the Win32
// writeBatchToFile uses WriteFile directly, not writev.

import { spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const repoRoot = dirname(dirname(fileURLToPath(import.meta.url)));

if (process.platform === 'win32') {
	console.log('SKIP (Windows): writev_partial_test is POSIX-only');
	process.exit(0);
}

const cxx = process.env.CXX || 'c++';
const tmp = mkdtempSync(join(tmpdir(), 'rocksdb-js-writev-test-'));
const bin = join(tmp, 'writev_partial_test');

const sources = [
	join(repoRoot, 'test', 'binding', 'writev_partial_test.cpp'),
	join(repoRoot, 'src', 'binding', 'writev_all.cpp'),
];
for (const s of sources) {
	if (!existsSync(s)) {
		console.error(`missing source: ${s}`);
		process.exit(2);
	}
}

const compile = spawnSync(
	cxx,
	[
		'-std=c++20',
		'-O2',
		'-Wall',
		'-Wextra',
		'-pthread',
		'-DROCKSDB_JS_WRITEV_ALL_STANDALONE',
		'-o',
		bin,
		...sources,
	],
	{ stdio: 'inherit' }
);
if (compile.status !== 0) {
	rmSync(tmp, { recursive: true, force: true });
	process.exit(compile.status ?? 1);
}

const run = spawnSync(bin, [], { stdio: 'inherit' });
rmSync(tmp, { recursive: true, force: true });
process.exit(run.status ?? 1);
