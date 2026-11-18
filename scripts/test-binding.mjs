import { versions } from '../dist/index.mjs';
import { createRequire } from 'node:module';

console.log(
	`rocksdb-js v${versions['rocksdb-js']} (RocksDB v${versions.rocksdb}) loaded successfully!`
);

// A published prebuild must report a failed internal check and continue, never abort the host.
const binding = createRequire(import.meta.url)('../build/Release/rocksdb-js.node');
if (binding.fatalAsserts !== false) {
	console.error(
		`build/Release/rocksdb-js.node was built with fatal internal checks (fatalAsserts=${binding.fatalAsserts}); unset ROCKSDB_JS_FATAL_ASSERTS before building a release`
	);
	process.exit(1);
}
