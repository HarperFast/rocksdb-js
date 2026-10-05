import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

// The ESM and CJS bundles each patch the same native prototypes, so loading both in one process
// throws. ESM is checked here; CJS is checked in a child.
function check(mod, format) {
	if (typeof mod.getRegistryStatus !== 'function') {
		throw new Error(`${format}: getRegistryStatus is not a function`);
	}
	if (!Array.isArray(mod.getRegistryStatus())) {
		throw new Error(`${format}: getRegistryStatus() did not return an array`);
	}
	if ('registryStatus' in mod) {
		throw new Error(`${format}: the removed registryStatus alias is still exported`);
	}
	console.log(`${format}: getRegistryStatus() ok, registryStatus alias absent`);
}

if (process.argv[2] === 'cjs') {
	const { createRequire } = await import('node:module');
	check(createRequire(import.meta.url)('../dist/index.cjs'), 'cjs');
} else {
	const esm = await import('../dist/index.mjs');
	console.log(
		`rocksdb-js v${esm.versions['rocksdb-js']} (RocksDB v${esm.versions.rocksdb}) loaded successfully!`
	);
	check(esm, 'esm');

	const child = spawnSync(process.execPath, [fileURLToPath(import.meta.url), 'cjs'], {
		stdio: 'inherit',
	});
	if (child.status !== 0) process.exit(child.status ?? 1);
}
