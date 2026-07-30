/**
 * Build and run native GoogleTest binaries.
 */

import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
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

// The compiled binary is only valid for the RocksDB version it was built
// against. `RocksDBVersion.MatchesPackagePin` checks the base MAJOR.MINOR.PATCH
// (a `-N` revision suffix isn't in RocksDB's version.h), so a stale binary from
// an earlier revision (e.g. 11.1.2-1 when the pin is now 11.1.2-2) would still
// pass. Record the pinned version alongside the binary and force a rebuild when
// it changes, so the runner never reuses a binary built for a different pin.
const versionMarker = join(root, 'build', config, '.rocksdb-test-version');
const builtVersion = existsSync(versionMarker) ? readFileSync(versionMarker, 'utf8').trim() : '';

if (
	!existsSync(binary) ||
	process.env.NATIVE_TEST_REBUILD === '1' ||
	builtVersion !== expectedVersion
) {
	const gypArgs = ['rebuild'];
	if (process.env.NATIVE_TEST_DEBUG === '1') {
		gypArgs.push('--debug');
	}
	run('pnpm', ['exec', 'node-gyp', ...gypArgs]);
	// Record the pin this binary was built against (node-gyp's prepare-rocksdb
	// action has reconciled deps/rocksdb to it by now).
	writeFileSync(versionMarker, expectedVersion);
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
