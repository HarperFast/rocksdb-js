import { existsSync, lstatSync, readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';

export function getActiveVersion(): string | undefined {
	const includeDir = resolve(import.meta.dirname, '../../deps/rocksdb/include');

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
			return `${majorVersionMatch[1]}.${minorVersionMatch[1]}.${patchVersionMatch[1]}`;
		}
	} catch (error) {
		console.error('Failed to read deps/rocksdb/include/rocksdb/version.h:', error);
		process.exit(1);
	}
}
