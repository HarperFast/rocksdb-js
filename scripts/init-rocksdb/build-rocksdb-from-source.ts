import { resolve } from 'node:path';
import { execFileSync } from 'node:child_process';
import { mkdir, rm, symlink } from 'node:fs/promises';

export async function buildRocksDBFromSource(rocksdbPath: string, dest: string) {
	rocksdbPath = resolve(rocksdbPath);

	execFileSync(
		'make',
		['-C', rocksdbPath, 'static_lib'],
		{ stdio: 'inherit' }
	);

	// delete existing deps/rocksdb
	await rm(dest, { recursive: true, force: true });
	await mkdir(resolve(dest, 'lib'), { recursive: true });

	// copy include and librocksdb.a to deps/rocksdb
	await symlink(
		resolve(rocksdbPath, 'include'),
		resolve(dest, 'include'),
	);

	await symlink(
		resolve(rocksdbPath, 'librocksdb.a'),
		resolve(dest, 'lib/librocksdb.a'),
	);
}
