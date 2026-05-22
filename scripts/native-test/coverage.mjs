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

const lcovIgnore = 'unsupported,unsupported,inconsistent,inconsistent,format,format';

run('lcov', [
	'--ignore-errors',
	lcovIgnore,
	'--capture',
	'--directory',
	objDir,
	'--output-file',
	infoFile,
]);

run('lcov', [
	'--ignore-errors',
	lcovIgnore,
	'--extract',
	infoFile,
	'*/src/binding/*',
	'--output-file',
	infoFile,
]);

run('genhtml', [
	'--ignore-errors',
	'category,category',
	'--dark-mode',
	infoFile,
	'--output-directory',
	htmlDir,
]);
console.log(`Coverage report: ${htmlDir}/index.html`);
