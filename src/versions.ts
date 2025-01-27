import nodeGypBuild from 'node-gyp-build';

const binding = nodeGypBuild();

export const versions = {
	'rocksdb': binding.version,
	'rocksdb-js': 'ROCKSDB_JS_VERSION',
};