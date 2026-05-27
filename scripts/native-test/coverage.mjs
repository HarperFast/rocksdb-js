/**
 * Build native tests with coverage, run them, and generate an lcov HTML report.
 */

import { spawnSync } from 'node:child_process';
import { mkdirSync, rmSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');

function run(command, args, options = {}) {
	const result = spawnSync(command, args, {
		cwd: root,
		stdio: 'inherit',
		shell: process.platform === 'win32',
		...options,
	});
	if (result.status !== 0) {
		process.exit(result.status ?? 1);
	}
}

if (process.platform === 'win32') {
	console.warn('Native coverage collection is not supported on Windows yet; running tests only.');
	process.env.NATIVE_TEST_DEBUG = '1';
	run(process.execPath, [
		resolve(dirname(fileURLToPath(import.meta.url)), 'run.mjs'),
		...process.argv.slice(2),
	]);
	process.exit(0);
}

process.env.NATIVE_TEST_DEBUG = '1';
run(process.execPath, [
	resolve(dirname(fileURLToPath(import.meta.url)), 'run.mjs'),
	...process.argv.slice(2),
]);

const coverageDir = join(root, 'coverage/native');
const infoFile = join(coverageDir, 'lcov.info');
const htmlDir = join(coverageDir, 'html');
const objDir = join(root, 'build/Debug/obj.target/rocksdb-js-native-tests');

rmSync(coverageDir, { recursive: true, force: true });
mkdirSync(coverageDir, { recursive: true });

const lcovIgnore =
	'unsupported,unsupported,inconsistent,inconsistent,format,format,mismatch,mismatch,gcov,gcov';

// Capture: walk the test binary's object directory, read every .gcda/.gcno
// pair gcov produced during the test run, and write a single lcov tracefile
// (lcov.info) describing line/branch coverage across every translation unit
// the binary was built from - including system headers and gtest sources.
run('lcov', [
	'--ignore-errors',
	lcovIgnore,
	'--capture',
	'--directory',
	objDir,
	'--output-file',
	infoFile,
]);

// Extract: filter the tracefile in place, keeping only entries whose source
// path matches the binding code we actually care about. Drops gtest internals,
// libc++ headers, and anything else outside src/binding/ so the HTML report
// reflects our coverage, not the test harness's.
run('lcov', [
	'--ignore-errors',
	lcovIgnore,
	'--extract',
	infoFile,
	'*/src/binding/*',
	'--output-file',
	infoFile,
]);

// Render: turn the filtered tracefile into a browsable HTML report under
// coverage/native/html/. --dark-mode picks the dark CSS theme.
run('genhtml', [
	'--ignore-errors',
	'category,category',
	'--dark-mode',
	infoFile,
	'--output-directory',
	htmlDir,
]);
console.log(`Coverage report: ${htmlDir}/index.html`);
