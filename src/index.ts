import { version } from './load-binding.js';

export { RocksDatabase } from './database.js';
export { Store } from './store.js';
export { DBIterator } from './dbi-iterator.js';

export const versions = {
	'rocksdb': version,
	'rocksdb-js': 'ROCKSDB_JS_VERSION',
};
