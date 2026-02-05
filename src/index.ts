import { version } from './load-binding.js';

export { RocksDatabase, type RocksDatabaseOptions } from './database.js';
export { DBIterator } from './dbi-iterator.js';
export { DBI, type IteratorOptions } from './dbi.js';
export type { Key } from './encoding.js';
export {
	constants,
	registryStatus,
	shutdown,
	type TransactionEntry,
	TransactionLog,
} from './load-binding.js';
export * from './parse-transaction-log.js';
export {
	Store,
	type StoreContext,
	type StoreGetOptions,
	type StoreIteratorOptions,
	type StorePutOptions,
	type StoreRangeOptions,
	type StoreRemoveOptions,
} from './store.js';
export { Transaction } from './transaction.js';

import './transaction-log-reader.js';

export const versions: { rocksdb: string; 'rocksdb-js': string } = {
	rocksdb: version,
	'rocksdb-js': 'ROCKSDB_JS_VERSION',
};
