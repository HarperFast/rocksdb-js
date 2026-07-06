import { version } from './load-binding.js';

export {
	backups,
	type BackupInfo,
	type BackupOptions,
	type RestoreMode,
	type RestoreOptions,
} from './backup.js';
export type { BackupStreamOptions } from './backup-stream.js';
export {
	RocksDatabase,
	type RocksDatabaseOptions,
	type RocksDBStat,
	type RocksDBStats,
} from './database.js';
export { DBIterator } from './dbi-iterator.js';
export { DBI, type IteratorOptions } from './dbi.js';
export type { Key } from './encoding.js';
export type * from './stats.js';
export {
	constants,
	coolTransactionLogs,
	currentThreadId,
	fileLockRelease,
	tryFileLock,
	registryStatus,
	stats,
	shutdown,
	TransactionLog,
	type TransactionEntry,
	type TransactionLogPosition,
	type TransactionLogStats,
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
