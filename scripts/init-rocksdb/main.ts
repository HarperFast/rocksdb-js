/**
 * This script prepares the RocksDB library. It is called by the
 * binding.gyp before compiling the rocksdb-js target.
 *
 * It will build RocksDB from source if the `ROCKSDB_PATH` environment
 * variable is set. Otherwise, it will download the latest RocksDB prebuild
 * from https://github.com/HarperFast/rocksdb-prebuilds/releases.
 *
 * To manually run this script: pnpm tsx scripts/init-rocksdb/main.ts
 */

import { config } from 'dotenv';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import semver from 'semver';
import { buildRocksDBFromSource } from './build-rocksdb-from-source';
import { downloadRocksDB } from './download-rocksdb';
import { getCurrentVersion } from './get-current-version';
import { getPrebuild } from './get-prebuild';

const __dirname = fileURLToPath(dirname(import.meta.url));

try {
	console.log('Initializing RocksDB library...');

	config({ path: ['.env'], override: true });

	const dest = resolve(__dirname, '../../deps/rocksdb');

	const rocksdbPath = process.env.ROCKSDB_PATH;
	if (rocksdbPath) {
		await buildRocksDBFromSource(rocksdbPath, dest);
		process.exit(0);
	}

	const currentVersion = getCurrentVersion();
	const runtime = process.platform === 'linux'
		? `-${process.env.ROCKSDB_LIBC || 'glibc'}`
		: undefined;
	const pkgJson = JSON.parse(readFileSync(resolve(__dirname, '../../package.json'), 'utf8'));
	const desiredVersion = process.env.ROCKSDB_VERSION || pkgJson.rocksdb?.version || undefined;

	if (
		currentVersion && desiredVersion && semver.eq(currentVersion.version, desiredVersion) &&
		(!currentVersion.runtime || currentVersion.runtime === runtime)
	) {
		console.log(`No update needed, RocksDB ${currentVersion} is already installed.`);
		process.exit(0);
	}

	const prebuild = await getPrebuild(desiredVersion);

	if (
		currentVersion && semver.lte(prebuild.version, currentVersion.version) &&
		(!currentVersion.runtime || currentVersion.runtime === runtime)
	) {
		console.log(`No update needed, latest version ${prebuild.version} is active.`);
		process.exit(0);
	}

	await downloadRocksDB(prebuild, dest, runtime);
} catch (error) {
	console.error(error);
	process.exit(1);
}
