/**
 * Build and run native GoogleTest binaries.
 */

import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const gtestMarker = join(root, 'deps/googletest/googletest/include/gtest/gtest.h');

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

if (!existsSync(gtestMarker)) {
	run('pnpm', ['exec', 'tsx', 'scripts/init-gtest/main.ts']);
}

const pkg = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
const expectedVersion = pkg.rocksdb?.version ?? '';

const config = process.env.NATIVE_TEST_DEBUG === '1' ? 'Debug' : 'Release';
const binaryName =
	process.platform === 'win32' ? 'rocksdb-js-native-tests.exe' : 'rocksdb-js-native-tests';
const binary = join(root, 'build', config, binaryName);

if (!existsSync(binary) || process.env.NATIVE_TEST_REBUILD === '1') {
	const gypArgs = ['rebuild'];
	if (process.env.NATIVE_TEST_DEBUG === '1') {
		gypArgs.push('--debug');
	}
	run('pnpm', ['exec', 'node-gyp', ...gypArgs]);
}

if (!existsSync(binary)) {
	console.error(`Native test binary not found: ${binary}`);
	process.exit(1);
}

const env = {
	...process.env,
	...(expectedVersion ? { ROCKSDB_EXPECTED_VERSION: expectedVersion } : {}),
	GTEST_COLOR: '1',
};

const testArgs = process.argv.slice(2);
const result = spawnSync(binary, testArgs, { cwd: root, stdio: 'inherit', env });
process.exit(result.status ?? 1);
