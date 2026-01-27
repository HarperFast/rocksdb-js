import { existsSync, lstatSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = fileURLToPath(dirname(import.meta.url));

type CurrentVersion = { version?: string; runtime?: string };

export function getCurrentVersion(): CurrentVersion | undefined {
	// see if we have a rocksdb.json file
	try {
		const rocksdbJson = JSON.parse(
			readFileSync(resolve(__dirname, '../../deps/rocksdb/rocksdb.json'), 'utf8')
		);
		return { version: rocksdbJson.version, runtime: rocksdbJson.runtime };
	} catch {
		// ignore
	}

	// we are either using an old prebuild or building from source

	const includeDir = resolve(__dirname, '../../deps/rocksdb/include');
	if (existsSync(includeDir) && lstatSync(includeDir).isSymbolicLink()) {
		// if include dir is a symlink, then the last build was from source,
		// but now we're downloading a prebuild
		return;
	}

	const versionFile = join(includeDir, 'rocksdb', 'version.h');
	if (!existsSync(versionFile)) {
		return;
	}

	try {
		const contents = readFileSync(versionFile, 'utf8').trim();
		const majorVersionMatch = contents.match(/#define ROCKSDB_MAJOR (\d+)/);
		const minorVersionMatch = contents.match(/#define ROCKSDB_MINOR (\d+)/);
		const patchVersionMatch = contents.match(/#define ROCKSDB_PATCH (\d+)/);
		if (majorVersionMatch && minorVersionMatch && patchVersionMatch) {
			return { version: `${majorVersionMatch[1]}.${minorVersionMatch[1]}.${patchVersionMatch[1]}` };
		}
	} catch (error) {
		console.error('Failed to read deps/rocksdb/include/rocksdb/version.h:', error);
		process.exit(1);
	}
}
