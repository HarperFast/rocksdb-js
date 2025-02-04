import { execFileSync } from 'node:child_process';
import { resolve } from 'node:path';
import { cp, mkdir, rm } from 'node:fs/promises';
import { config } from 'dotenv';

config({ path: ['.env'], override: true });

let rocksdbPath = process.env.ROCKSDB_PATH;
const dest = resolve(import.meta.dirname, '../deps/rocksdb');

if (rocksdbPath) {
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

const headers: Record<string, string> = {
	Accept: 'application/vnd.github.v3.raw',
};
if (process.env.GITHUB_TOKEN) {
	headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;
}

// get the latest RocksDB release
const response = await fetch('https://api.github.com/repos/harperdb/rocksdb-js/releases', {
	headers,
});

interface GithubRelease {
	name: string;
	body: string;
}

const releases = await response.json() as GithubRelease[];
const latest = releases.find(release => /^RocksDB nightly/.test(release.name));

console.log(releases);

process.exit(1);

// TODO:
// - get the latest RocksDB release
// - check if newer than the one in the deps folder
// - if newer, download and replace the one in the deps folder

