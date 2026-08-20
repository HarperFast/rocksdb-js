import { version } from './load-binding.ts';

export {
	backups,
	type BackupInfo,
	type BackupOptions,
	type RestoreMode,
	type RestoreOptions,
} from './backup.ts';
export type { BackupStreamOptions } from './backup-stream.ts';
export {
	RocksDatabase,
	type RocksDatabaseOptions,
	type RocksDBStat,
	type RocksDBStats,
} from './database.ts';
export { CountEstimator, type CountEstimatorOptions } from './count-estimator.ts';
export { DBIterator } from './dbi-iterator.ts';
export { DBI, type CountEstimate, type CountEstimateOptions, type IteratorOptions } from './dbi.ts';
export type { Key } from './encoding.ts';
export type * from './stats.ts';
export {
	constants,
	coolTransactionLogs,
	currentThreadId,
	fileLockRelease,
	tryFileLock,
	registryStatus,
	stats,
	shutdown,
	supportedCompression,
	TransactionLog,
	type TransactionEntry,
	type TransactionLogPosition,
	type TransactionLogStats,
} from './load-binding.ts';
export * from './parse-transaction-log.ts';
export {
	validateTransactionLogStore,
	type TransactionLogFileValidation,
	type TransactionLogStoreValidation,
	type ValidateTransactionLogStoreOptions,
} from './validate-transaction-log.ts';
export {
	type CompressionAlgorithm,
	type CompressionInfo,
	type CompressionOption,
	type LogOptions,
	Store,
	type StoreContext,
	type StoreGetOptions,
	type StoreIteratorOptions,
	type StorePutOptions,
	type StoreRangeOptions,
	type StoreRemoveOptions,
} from './store.ts';
export { Transaction } from './transaction.ts';

import './transaction-log-reader.ts'; // installs TransactionLog.prototype.query
export { CorruptFrameError } from './transaction-log-reader.ts';

export const versions: { rocksdb: string; 'rocksdb-js': string } = {
	rocksdb: version,
	'rocksdb-js': 'ROCKSDB_JS_VERSION',
};
