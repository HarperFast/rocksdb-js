import { version } from './load-binding.js';

export { RocksDatabase, type RocksDatabaseOptions } from './database.js';
export { Store, type Context } from './store.js';
export { DBIterator } from './dbi-iterator.js';
export { Transaction } from './transaction.js';
export type { Key } from './encoding.js';
export { TransactionLog } from './load-binding.js';
export * from './transaction-log.js';

export const versions: {
	rocksdb: string;
	'rocksdb-js': string;
} = {
	rocksdb: version,
	'rocksdb-js': 'ROCKSDB_JS_VERSION',
};
