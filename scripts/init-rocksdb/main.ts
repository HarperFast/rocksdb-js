/**
 * This script prepares the RocksDB library. It is called by the
 * binding.gyp before compiling the rocksdb-js target.
 *
 * It will build RocksDB from source if the `ROCKSDB_PATH` environment
 * variable is set. Otherwise, it will download the latest RocksDB prebuild
 * from https://github.com/HarperDB/rocksdb-prebuilds/releases.
 *
 * To manually run this script: pnpm tsx scripts/init-rocksdb/main.ts
 */

import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { config } from 'dotenv';
import semver from 'semver';
import { buildRocksDBFromSource } from './build-rocksdb-from-source';
import { getActiveVersion } from './get-active-version';
import { getPrebuild } from './get-prebuild';
import { downloadRocksDB } from './download-rocksdb';

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

	const activeVersion = getActiveVersion();
	const prebuild = await getPrebuild();

	if (activeVersion && semver.lte(prebuild.version, activeVersion)) {
		console.log(`No update needed, latest version ${prebuild.version} is active.`);
		process.exit(0);
	}

	await downloadRocksDB(prebuild, dest);
} catch (error) {
	console.error(error);
	process.exit(1);
}
