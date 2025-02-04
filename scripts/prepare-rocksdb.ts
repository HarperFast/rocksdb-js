/**
 * This script prepares the RocksDB prebuild binary. It is called by the
 * binding.gyp before compiling the rocksdb-js target.
 *
 * It will build RocksDB from source if the `ROCKSDB_PATH` environment
 * variable is set. Otherwise, it will download the latest RocksDB prebuild
 * from https://github.com/HarperDB/rocksdb-prebuilds/releases.
 *
 * To manually run this script: pnpm tsx scripts/prepare-rocksdb.ts
 */

import { execFileSync } from 'node:child_process';
import { resolve } from 'node:path';
import { cp, mkdir, rm } from 'node:fs/promises';
import { config } from 'dotenv';
import semver from 'semver';
import { readFileSync, existsSync } from 'node:fs';

config({ path: ['.env'], override: true });

let rocksdbPath = process.env.ROCKSDB_PATH;
const dest = resolve(import.meta.dirname, '../deps/rocksdb');

if (rocksdbPath) {
	// build rocksdb from source
	rocksdbPath = resolve(rocksdbPath);

	execFileSync(
		'make',
		['-C', rocksdbPath, 'static_lib'],
		{ stdio: 'inherit' }
	);

	// delete existing deps/rocksdb
	await rm(dest, { recursive: true, force: true });
	await mkdir(dest, { recursive: true });

	// copy include and librocksdb.a to deps/rocksdb
	await cp(
		resolve(rocksdbPath, 'include'),
		resolve(dest, 'include'),
		{ force: true, recursive: true }
	);

	await cp(
		resolve(rocksdbPath, 'librocksdb.a'),
		resolve(dest, 'librocksdb.a'),
		{ force: true }
	);

	process.exit(0);
}

let activeVersion: string | null = null;
const versionFile = resolve(import.meta.dirname, '../deps/rocksdb/include/rocksdb/version.h');
if (existsSync(versionFile)) {
	try {
		const contents = readFileSync(versionFile, 'utf8').trim();
		const majorVersionMatch = contents.match(/#define ROCKSDB_MAJOR (\d+)/);
		const minorVersionMatch = contents.match(/#define ROCKSDB_MINOR (\d+)/);
		const patchVersionMatch = contents.match(/#define ROCKSDB_PATCH (\d+)/);
		if (majorVersionMatch && minorVersionMatch && patchVersionMatch) {
			activeVersion = `${majorVersionMatch[1]}.${minorVersionMatch[1]}.${patchVersionMatch[1]}`;
		}
	} catch (error) {
		console.error('Failed to read deps/rocksdb/include/rocksdb/version.h:', error);
		process.exit(1);
	}
}

// download the latest RocksDB prebuild
const headers: Record<string, string> = {
	Accept: 'application/vnd.github.v3.raw',
};
if (process.env.GITHUB_TOKEN) {
	headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;
}

// get the latest RocksDB release
const response = await fetch('https://api.github.com/repos/harperdb/rocksdb-prebuilds/releases', {
	headers,
});

interface GithubRelease {
	assets: {
		name: string;
		browser_download_url: string;
	}[];
	body: string;
	name: string;
	tag_name: string;
}

const releases = await response.json() as GithubRelease[];
releases.sort((a, b) => {
	const aVersion = a.tag_name.replace(/^v/, '');
	const bVersion = b.tag_name.replace(/^v/, '');
	return semver.rcompare(aVersion, bVersion);
});

const latestRelease = releases[0];
if (!latestRelease) {
	console.error('No prebuild releases found!');
	process.exit(1);
}

const latestVersion = latestRelease.name.replace(/^v/, '');
if (semver.gt(latestVersion, activeVersion)) {
	console.log(`Updating RocksDB from v${activeVersion} to v${latestVersion}`);
} else {
	console.log(`No update needed, v${activeVersion} is up to date.`);
	process.exit(0);
}

// get the latest release artifacts
// console.log(latestRelease.assets);

// const platforms = { 'darwin': 'macos', 'linux': 'linux', 'win32': 'windows'};
// const file = `rocksdb-${latestVersion}-${process.arch}-${platforms[process.platform] || process.platform}.tar.xz`;
// const activeVersion = null; // TODO: get the version from the file

process.exit(1);

// TODO:
// - get the latest RocksDB release
// - check if newer than the one in the deps folder
// - if newer, download and replace the one in the deps folder
