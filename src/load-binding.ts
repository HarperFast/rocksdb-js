import type { BackupInfo, BackupOptions, RestoreOptions } from './backup.js';
import type { RangeOptions } from './dbi.js';
import type { BufferWithDataView, Key } from './encoding.js';
import type { StatsAll, StatsDefault, StatsHistogramData } from './stats.js';
import type { StoreContext } from './store.js';
import type { TransactionLogStoreValidation } from './validate-transaction-log.js';
export type {
	GetStatsMethod,
	StatsAll,
	StatsAllExtras,
	StatsBasics,
	StatsCurated,
	StatsCuratedExtras,
	StatsDefault,
	StatsHistogramData,
	StatsValue,
} from './stats.js';
import { execSync } from 'node:child_process';
import { readdirSync, readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export type NativeTransactionOptions = {
	/**
	 * Whether to disable snapshots.
	 *
	 * @default false
	 */
	disableSnapshot?: boolean;

	/**
	 * When `true`, an `IsBusy` conflict at commit time is resolved with the
	 * `RETRY_NOW` sentinel value instead of being rejected. The native layer
	 * may park on a VT slot before resolving, so the JS retry fires only after
	 * the conflicting transaction has committed and released its write intent.
	 *
	 * Use together with `verificationTable: true` on the database.
	 *
	 * @default false
	 */
	coordinatedRetry?: boolean;
};

export type NativeTransaction = {
	id: number;
	new (context: NativeDatabase, options?: NativeTransactionOptions): NativeTransaction;
	abort(): void;
	commit(resolve: (retrySignal?: number) => void, reject: (err: Error) => void): void;
	commitSync(): void;
	// Note that keyLengthOrKeyBuffer can be the length of the key if it was written into the shared buffer, or a direct buffer
	get(
		keyLengthOrKeyBuffer: number | Buffer,
		resolve: (value: Buffer | number) => void,
		reject: (err: Error) => void,
		txnIdIgnored?: number,
		expectedVersion?: number
	): number;
	getCount(options?: RangeOptions): number;
	getSync(keyLengthOrKeyBuffer: number | Buffer): Buffer | number | undefined;
	getTimestamp(): number;
	putSync(key: Key, value: Buffer | Uint8Array, txnId?: number): void;
	removeSync(key: Key): void;
	setTimestamp(timestamp?: number): void;
	useLog(name: string | number): TransactionLog;
};

export type LogBuffer = Buffer & { dataView: DataView; logId: number; size: number };

export type TransactionLogQueryOptions = {
	start?: number;
	end?: number;
	exactStart?: boolean;
	startFromLastFlushed?: boolean;
	readUncommitted?: boolean;
	exclusiveStart?: boolean;
};

export type TransactionEntry = { timestamp: number; data: Buffer; endTxn: boolean };

/**
 * A position within a transaction log, identifying a log file by its sequence
 * number and a byte `offset` within that file.
 */
export type TransactionLogPosition = { sequence: number; offset: number };

/**
 * A detailed statistics snapshot for a single transaction log store, returned
 * by {@link TransactionLog.getStats}. All sizes are in bytes; timestamps are
 * milliseconds since the Unix epoch.
 *
 * Memory note: `memory.mappedBytes` is virtual address space — the active write
 * file is mapped at the full configured `maxFileSize` on POSIX, so it does not
 * reflect resident memory. `memory.overlayBytes` (POSIX only; 0 on Windows) is
 * the file-backed portion and is the closer proxy for real consumption.
 */
export type TransactionLogStats = {
	name: string;
	path: string;
	fileCount: number;
	currentSequenceNumber: number;
	oldestSequenceNumber: number;
	totalSizeBytes: number;
	currentFileSize: number;
	pendingTransactions: number;
	uncommittedTransactions: number;
	replayGapBytes: number;
	memory: {
		mappedBytes: number;
		overlayBytes: number;
		activeMaps: number;
	};
	nextLogPosition: TransactionLogPosition;
	lastFlushedPosition: TransactionLogPosition;
	lastCommittedPosition: TransactionLogPosition | null;
	purge: {
		oldestFileAgeMs: number;
		purgeableFiles: number;
		retainedUnflushedFiles: number;
		lastPurgeMs: number;
	};
	totals: {
		transactionsWritten: number;
		entriesWritten: number;
		bytesWritten: number;
		rotations: number;
		filesPurged: number;
		bytesPurged: number;
		purgeRuns: number;
		databaseFlushes: number;
		writeFailures: number;
	};
	config: {
		maxFileSize: number;
		retentionMs: number;
		maxAgeThreshold: number;
	};
};

export type TransactionLog = {
	new (db: NativeDatabase, name: string): TransactionLog;
	addEntry(data: Buffer | Uint8Array, txnId?: number): void;
	getLogFileSize(sequenceId?: number): number;
	getStats(): TransactionLogStats;
	name: string;
	path: string;
	query(options?: TransactionLogQueryOptions): IterableIterator<TransactionEntry>;
	_currentLogBuffer: LogBuffer;
	_findPosition(timestamp: number): number;
	_getLastCommittedPosition(): Buffer;
	_getLastFlushed(): number;
	_getMemoryMapOfFile(sequenceId: number): LogBuffer | undefined;
	_lastCommittedPosition: Float64Array;
	_logBuffers: Map<number, WeakRef<LogBuffer>>;
};

/**
 * Shape of options that can be passed to the native iterator constructor for
 * the rare case of advanced RocksDB ReadOptions overrides. Common iterator
 * options are passed via the bitmask `flags` argument instead.
 */
export type NativeIteratorAdvancedOptions = {
	adaptiveReadahead?: boolean;
	asyncIO?: boolean;
	autoReadaheadSize?: boolean;
	backgroundPurgeOnIteratorCleanup?: boolean;
	fillCache?: boolean;
	readaheadSize?: number;
	tailing?: boolean;
};

/**
 * The result of a single native iterator step. A number value matches one of
 * the `ITERATOR_RESULT_*` constants. The slow-path object is returned when
 * the data does not fit in the shared key/value buffers, or when the decoder
 * needs a stable value buffer.
 */
export type NativeIteratorResult = number | { key: Buffer; value?: Buffer };

export declare class NativeIteratorCls {
	constructor(
		context: StoreContext,
		flags: number,
		startKeyEnd: number,
		endKeyStart: number,
		endKeyEnd: number,
		options?: NativeIteratorAdvancedOptions
	);
	next(): NativeIteratorResult;
	return(): void;
	throw(err?: unknown): void;
}

export type NativeDatabaseMode = 'optimistic' | 'pessimistic';

export type NativeDatabaseOptions = {
	dbWriteBufferSize?: number;
	disableWAL?: boolean;
	enableStats?: boolean;
	maxWriteBufferNumber?: number;
	maxWriteBufferSizeToMaintain?: number;
	mode?: NativeDatabaseMode;
	name?: string;
	noBlockCache?: boolean;
	parallelismThreads?: number;
	readOnly?: boolean;
	statsLevel?: (typeof stats.StatsLevel)[keyof typeof stats.StatsLevel];
	transactionLogMaxAgeThreshold?: number;
	transactionLogMaxSize?: number;
	transactionLogRetentionMs?: number;
	transactionLogsPath?: string;
	/**
	 * When true, transaction writes to this column family invalidate the
	 * VerificationTable slot for each written key at write time (not at
	 * commit time). Enable only for column families whose records are
	 * cached (e.g. the primary CF of a table). Default: false.
	 */
	verificationTable?: boolean;
	writeBufferSize?: number;
};

type ResolveCallback<T> = (value: T) => void;
type RejectCallback = (err: Error) => void;

export type UserSharedBufferCallback = () => void;

export type PurgeLogsOptions = {
	before?: number;
	destroy?: boolean;
	/**
	 * When `true`, count the entries in each purged log file (extra work) and
	 * return `PurgedLog[]` instead of the default `string[]` of file paths.
	 */
	includeEntryCounts?: boolean;
	name?: string;
};

/**
 * A purged transaction log file and the number of entries it held, returned by
 * `purgeLogs()` when `includeEntryCounts` is `true`.
 */
export type PurgedLog = { path: string; entries: number };

export type NativeDatabase = {
	new (): NativeDatabase;
	addListener(event: string, callback: (...args: any[]) => void): void;
	backup(
		resolve: ResolveCallback<number>,
		reject: RejectCallback,
		backupDir: string,
		options?: BackupOptions
	): void;
	// `emit` is invoked once per file header (kind 0: name, size, mtime) and once
	// per payload chunk (kind 1: Buffer). It must return a promise; native awaits
	// it before producing the next event (backpressure), and a rejection aborts.
	backupStream(
		resolve: ResolveCallback<void>,
		reject: RejectCallback,
		emit: (kind: number, data: string | Uint8Array, size: number, mtime: number) => Promise<void>,
		options?: { flushBeforeBackup?: boolean; transactionLogs?: boolean }
	): void;
	clear(resolve: ResolveCallback<void>, reject: RejectCallback): void;
	clearSync(): void;
	close(): void;
	compact(resolve: ResolveCallback<void>, reject: RejectCallback, start?: Key, end?: Key): void;
	compactSync(start?: Key, end?: Key): void;
	columns: string[];
	createCheckpoint(
		resolve: ResolveCallback<void>,
		reject: RejectCallback,
		targetPath: string
	): void;
	destroy(): void;
	drop(resolve: ResolveCallback<void>, reject: RejectCallback): void;
	dropSync(): void;
	flush(resolve: ResolveCallback<void>, reject: RejectCallback): void;
	flushSync(): void;
	notify(event: string | BufferWithDataView, args?: any[]): boolean;
	// Note that keyLengthOrKeyBuffer can be the length of the key if it was written into the shared buffer, or a direct buffer
	get(
		keyLengthOrKeyBuffer: number | Buffer,
		resolve: ResolveCallback<Buffer | number>,
		reject: RejectCallback,
		txnId?: number,
		expectedVersion?: number
	): number;
	getCount(options?: RangeOptions, txnId?: number): number;
	getDBIntProperty(propertyName: string): number | undefined;
	getDBProperty(propertyName: string): string | undefined;
	getMonotonicTimestamp(): number;
	getOldestSnapshotTimestamp(): number;
	getStat(statName: string): number | StatsHistogramData;
	getStats(all?: false): StatsDefault;
	getStats(all: true): StatsAll;
	getSync(
		keyLengthOrKeyBuffer: number | Buffer,
		flags: number,
		txnId?: number,
		expectedVersion?: number
	): Buffer;
	getUserSharedBuffer(
		key: BufferWithDataView,
		defaultBuffer: ArrayBuffer,
		callback?: UserSharedBufferCallback
	): ArrayBuffer;
	hasLock(key: BufferWithDataView): boolean;
	listeners(event: string | BufferWithDataView): number;
	listLogs(): string[];
	opened: boolean;
	open(path: string, options?: NativeDatabaseOptions): void;
	populateVersion(keyLengthOrKeyBuffer: number | Buffer, version: number): void;
	purgeLogs(options: PurgeLogsOptions & { includeEntryCounts: true }): PurgedLog[];
	purgeLogs(options?: PurgeLogsOptions & { includeEntryCounts?: false }): string[];
	purgeLogs(options?: PurgeLogsOptions): string[] | PurgedLog[];
	putSync(key: BufferWithDataView, value: any, txnId?: number): void;
	removeListener(event: string | BufferWithDataView, callback: () => void): boolean;
	removeSync(key: BufferWithDataView, txnId?: number): void;
	// Provide a buffer that is used as the default/shared buffer for keys, where functions that provide a key can do so by assigning the key to the shared buffer and providing the length.
	// A null value will reset the buffer.
	setDefaultKeyBuffer(buffer: Buffer | Uint8Array | null): void;
	// Provide a buffer that is used as the default/shared buffer for value, where functions that use or return a value can do so by assigning the value to the shared buffer and providing/returning the length.
	// A null value will reset the buffer.
	setDefaultValueBuffer(buffer: Buffer | Uint8Array | null): void;
	// Provide a Uint32Array(2)-backed buffer used by iterators to communicate
	// the key length (index 0) and value length (index 1) of each iteration
	// step without per-iteration NAPI property accesses.
	setIteratorState(buffer: Buffer | Uint8Array): void;
	tryLock(key: BufferWithDataView, callback?: () => void): boolean;
	unlock(key: BufferWithDataView): void;
	useLog(name: string): TransactionLog;
	verifyVersion(keyLengthOrKeyBuffer: number | Buffer, version: number): boolean;
	withLock(key: BufferWithDataView, callback: () => void | Promise<void>): Promise<void>;
};

export type RocksDatabaseConfig = {
	blockCacheSize?: number;
	/**
	 * Number of slots in the process-global verification table. Each slot is
	 * 8 bytes; the default of 128K slots is 1 MB. Set to 0 to disable.
	 *
	 * Must be configured before the first database is opened. Once the table
	 * is materialized, attempts to change this value will throw.
	 */
	verificationTableEntries?: number;
	compactOnClose?: boolean;
	/**
	 * Total memtable memory limit (bytes) shared across every database opened
	 * in this process. When set, RocksDB uses a single `WriteBufferManager` so
	 * write buffers are bounded process-wide rather than per database. 0 (the
	 * default) disables the manager.
	 *
	 * Can be updated at runtime; the new size takes effect on the existing
	 * manager via `SetBufferSize`.
	 */
	writeBufferManagerSize?: number;
	/**
	 * When `true`, memtable memory is "charged" against the shared block cache
	 * so the block cache and write buffers draw from a single pool. During
	 * write bursts the cache shrinks to make room for memtables; once
	 * memtables flush, the cache can grow back into the reclaimed space.
	 *
	 * Has no effect when the block cache is disabled (size 0) or
	 * `writeBufferManagerSize` is 0. Must be set on the same `config()` call
	 * that first enables the manager — changing it after the manager has been
	 * created has no effect on the running instance.
	 *
	 * @default false
	 */
	writeBufferManagerCostToCache?: boolean;
	/**
	 * When `true`, writes are stalled once the manager's `buffer_size` is
	 * exceeded, providing a hard cap on memtable memory. When `false`,
	 * memtables are allowed to grow past the limit and flushes are simply
	 * scheduled more aggressively. Off by default to favor write throughput
	 * over hard memory bounding.
	 *
	 * @default false
	 */
	writeBufferManagerAllowStall?: boolean;
};

const nativeExtRE = /\.node$/;
const req = createRequire(import.meta.url);

/**
 * Locates the native binding in the `build` directory, then the `prebuilds`
 * directory.
 *
 * @returns The path to the native binding.
 */
function locateBinding(): string {
	const baseDir = dirname(dirname(fileURLToPath(import.meta.url)));

	// check build directory
	for (const type of ['Release', 'Debug'] as const) {
		try {
			const dir = join(baseDir, 'build', type);
			const files = readdirSync(dir);
			for (const file of files) {
				if (nativeExtRE.test(file)) {
					return resolve(dir, file);
				}
			}

			/* v8 ignore next -- @preserve */
		} catch {}
	}

	// determine the Linux C runtime
	let runtime = '';
	if (process.platform === 'linux') {
		let isMusl = false;
		try {
			isMusl = readFileSync('/usr/bin/ldd', 'utf8').includes('musl');
		} catch {
			// `/usr/bin/ldd` likely doesn't exist
			if (typeof process.report?.getReport === 'function') {
				process.report.excludeEnv = true;
				const report = process.report.getReport() as unknown as {
					header?: { glibcVersionRuntime?: string };
					sharedObjects?: string[];
				};
				isMusl =
					(!report?.header || !report.header.glibcVersionRuntime) &&
					Array.isArray(report?.sharedObjects) &&
					report.sharedObjects.some(
						(obj) => obj.includes('libc.musl-') || obj.includes('ld-musl-')
					);
			}
			try {
				isMusl =
					isMusl || execSync('ldd --version', { encoding: 'utf8', stdio: 'pipe' }).includes('musl');
			} catch {
				// ldd may not exist on some systems such as Docker Hardened Images
			}
		}
		runtime = isMusl ? '-musl' : '-glibc';
	}

	// the following lines are non-trivial to test, so we'll ignore them
	/* v8 ignore next 10 -- @preserve */

	// check node_modules
	try {
		return require.resolve(`@harperfast/rocksdb-js-${process.platform}-${process.arch}${runtime}`);
	} catch {}

	throw new Error('Unable to locate rocksdb-js native binding');
}

export type RegistryStatusDB = {
	path: string;
	refCount: number;
	columnFamilies: string[];
	transactions: number;
	closables: number;
	locks: number;
	userSharedBuffers: number;
	listenerCallbacks: number;
};

export type RegistryStatus = RegistryStatusDB[];

const bindingPath = locateBinding();
// console.log(`Loading binding from ${bindingPath}`);
const binding = req(bindingPath);

export const config: (options: RocksDatabaseConfig) => void = binding.config;
export const FRESH_VERSION_FLAG: number = binding.constants.FRESH_VERSION_FLAG;
export const addGlobalListener: (event: string, callback: (...args: any[]) => void) => void =
	binding.addListener;
export const removeGlobalListener: (event: string, callback: (...args: any[]) => void) => boolean =
	binding.removeListener;
export const globalListenerCount: (event: string) => number = binding.listenerCount;
export const globalNotify: (event: string, args?: any[]) => boolean = binding.notify;
export const constants: {
	ALWAYS_CREATE_NEW_BUFFER_FLAG: number;
	NOT_IN_MEMORY_CACHE_FLAG: number;
	ONLY_IF_IN_MEMORY_CACHE_FLAG: number;
	POPULATE_VERSION_FLAG: number;
	FRESH_VERSION_FLAG: number;
	/**
	 * Sentinel value resolved (not rejected) by `commit()` when
	 * `coordinatedRetry: true` and the transaction encountered an IsBusy
	 * conflict. JS should retry the transaction body immediately.
	 */
	RETRY_NOW_VALUE: number;
	TRANSACTION_LOG_TOKEN: number;
	TRANSACTION_LOG_ENTRY_HEADER_SIZE: number;
	TRANSACTION_LOG_FILE_HEADER_SIZE: number;
	ITERATOR_REVERSE_FLAG: number;
	ITERATOR_INCLUSIVE_END_FLAG: number;
	ITERATOR_EXCLUSIVE_START_FLAG: number;
	ITERATOR_INCLUDE_VALUES_FLAG: number;
	ITERATOR_NEEDS_STABLE_VALUE_BUFFER_FLAG: number;
	ITERATOR_CONTEXT_IS_TRANSACTION_FLAG: number;
	ITERATOR_RESULT_DONE: number;
	ITERATOR_RESULT_FAST: number;
} = binding.constants;
export const NativeDatabase: NativeDatabase = binding.Database;
export const NativeIterator: typeof NativeIteratorCls = binding.Iterator;
export const NativeTransaction: NativeTransaction = binding.Transaction;
export const TransactionLog: TransactionLog = binding.TransactionLog;
export const registryStatus: () => RegistryStatus = binding.registryStatus;
export const shutdown: () => void = binding.shutdown;
export const currentThreadId: () => number = binding.currentThreadId;

/**
 * Advises the kernel that the file-backed pages of every mapped transaction log
 * are cold (Linux MADV_COLD), so they are reclaimed first under memory pressure
 * without being freed — useful during replication catch-up, where a full read of
 * the logs would otherwise inflate the container's reclaimable cache toward its
 * cgroup limit. No-op on kernels < 5.4, macOS, and Windows.
 *
 * The transaction log registry is a process-global singleton shared across all
 * worker threads, so a single call cools every worker's maps. Call it on an
 * interval from one thread (e.g. an `unref()`ed timer on the main thread).
 *
 * @returns the number of maps cooled and total file-backed bytes advised.
 */
export const coolTransactionLogs: () => { maps: number; bytes: number } =
	binding.coolTransactionLogs;

/**
 * Number of live transaction-log memory maps across the process. Internal —
 * used by tests to verify that releasing a frozen log's external buffer unmaps
 * the underlying mapping rather than leaving it retained.
 */
export const transactionLogMapCount: () => number = binding.transactionLogMapCount;

/**
 * Creates a native file lock using the specified file path (`flock` on POSIX,
 * `LockFileEx` on Windows), creating the file and any missing parent
 * directories. Exclusive by default; pass `shared` for a shared (reader) lock
 * that coexists with other shared holders but conflicts with an exclusive
 * holder in either direction. Returns an opaque non-zero token to pass to
 * `fileLockRelease`, or `0` if a conflicting holder — in any process,
 * container, or worker thread — currently has it. Throws on a hard error. The
 * OS handle is owned entirely in native code (no fd crosses into JS), and the
 * kernel releases the lock when the handle closes, including on process death.
 */
export const tryFileLock: (file: string, shared?: boolean) => number = binding.tryFileLock;

/**
 * Releases a file lock acquired via `tryFileLock`. A no-op for
 * token `0` or an unknown token.
 */
export const fileLockRelease: (token: number) => void = binding.fileLockRelease;

// Module-level backup management functions. These operate on a backup directory
// and do not require an open database. Wrapped by the `backups` namespace in
// `backup.ts`; creating a backup is a `RocksDatabase` instance method.
export const nativeBackupRestore: (
	resolve: ResolveCallback<void>,
	reject: RejectCallback,
	backupDir: string,
	dbDir: string,
	walDir: string,
	options?: { backupId?: number; keepLogFiles?: boolean; mode?: RestoreOptions['mode'] }
) => void = binding.backupRestore;
export const nativeBackupList: (
	resolve: ResolveCallback<BackupInfo[]>,
	reject: RejectCallback,
	backupDir: string
) => void = binding.backupList;
export const nativeBackupDelete: (
	resolve: ResolveCallback<void>,
	reject: RejectCallback,
	backupDir: string,
	backupId: number
) => void = binding.backupDelete;
export const nativeBackupPurge: (
	resolve: ResolveCallback<void>,
	reject: RejectCallback,
	backupDir: string,
	keepCount: number
) => void = binding.backupPurge;
export const nativeBackupVerify: (
	resolve: ResolveCallback<void>,
	reject: RejectCallback,
	backupDir: string,
	backupId: number,
	verifyWithChecksum: boolean
) => void = binding.backupVerify;

// Module-level transaction log store validation. Operates on a store directory
// (a closed database's store or a backup snapshot) and does not require an open
// database. Wrapped by `validateTransactionLogStore` in
// `validate-transaction-log.ts`.
export const nativeValidateTransactionLog: (
	resolve: ResolveCallback<TransactionLogStoreValidation>,
	reject: RejectCallback,
	path: string,
	strict: boolean
) => void = binding.validateTransactionLog;

export const stats: {
	StatsLevel: {
		DisableAll: number;
		ExceptTickers: number;
		ExceptHistogramOrTimers: number;
		ExceptTimers: number;
		ExceptDetailedTimers: number;
		ExceptTimeForMutex: number;
		All: number;
	};
} = binding.stats;

export const version: string = binding.version;
