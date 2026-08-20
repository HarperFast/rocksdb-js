/**
 * Build and run native GoogleTest binaries.
 */

import { resolveBuildSelection } from './build-identity.mjs';
import { config as loadEnv } from 'dotenv';
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
	run(process.execPath, ['scripts/init-gtest/main.ts']);
}

const pkg = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));

// Resolve the effective RocksDB build selection the SAME way init-rocksdb does
// (.env override, then ROCKSDB_VERSION, then the package pin; or a source build
// via ROCKSDB_PATH). The compiled binary is only valid for the artifact it was
// actually linked against, so both the rebuild marker and ROCKSDB_EXPECTED_VERSION
// must derive from this selection — not the package pin alone. Otherwise a build
// made under `ROCKSDB_VERSION=11.1.2-1` (env wins over the pin) would be recorded
// as the pin `11.1.2-2`; a later run that drops the override would then treat the
// stale `-1` binary as current (`MatchesPackagePin` also ignores the `-N`
// revision, so it can't catch it either).
loadEnv({ path: ['.env'], override: true });
// `identity` is the rebuild marker; `expectedVersion` feeds ROCKSDB_EXPECTED_VERSION.
// A `latest`/source selection is compared verbatim (not re-resolved) — the same
// pre-existing limitation a `latest` package pin already had.
const { expectedVersion, identity: buildIdentity } = resolveBuildSelection(
	process.env,
	pkg,
	process.platform
);

const buildType = process.env.NATIVE_TEST_DEBUG === '1' ? 'Debug' : 'Release';
const binaryName =
	process.platform === 'win32' ? 'rocksdb-js-native-tests.exe' : 'rocksdb-js-native-tests';
const binary = join(root, 'build', buildType, binaryName);

const versionMarker = join(root, 'build', buildType, '.rocksdb-test-version');
const builtIdentity = existsSync(versionMarker) ? readFileSync(versionMarker, 'utf8').trim() : '';

if (
	!existsSync(binary) ||
	process.env.NATIVE_TEST_REBUILD === '1' ||
	builtIdentity !== buildIdentity
) {
	const gypArgs = ['rebuild'];
	if (process.env.NATIVE_TEST_DEBUG === '1') {
		gypArgs.push('--debug');
	}
	run('pnpm', ['exec', 'node-gyp', ...gypArgs]);
	// Record the selection this binary was built against (node-gyp's configure
	// step has reconciled deps/rocksdb to it by now).
	writeFileSync(versionMarker, buildIdentity);
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
