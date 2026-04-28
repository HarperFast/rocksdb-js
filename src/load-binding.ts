import type { RangeOptions } from './dbi.js';
import type { BufferWithDataView, Key } from './encoding.js';
import type { StoreContext } from './store.js';
import { execSync } from 'node:child_process';
import { readdirSync, readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export type TransactionOptions = {
	/**
	 * Whether to disable snapshots.
	 *
	 * @default false
	 */
	disableSnapshot?: boolean;

	/**
	 * The maximum number of times to retry the transaction.
	 *
	 * @default 3
	 */
	maxRetries?: number;

	/**
	 * Whether to retry the transaction if it fails with `IsBusy`.
	 *
	 * @default `true` when the transaction is bound to a transaction log, otherwise `false`
	 */
	retryOnBusy?: boolean;
};

export type NativeTransaction = {
	id: number;
	new (context: NativeDatabase, options?: TransactionOptions): NativeTransaction;
	abort(): void;
	commit(resolve: () => void, reject: (err: Error) => void): void;
	commitSync(): void;
	// Note that keyLengthOrKeyBuffer can be the length of the key if it was written into the shared buffer, or a direct buffer
	get(
		keyLengthOrKeyBuffer: number | Buffer,
		resolve: (value: Buffer) => void,
		reject: (err: Error) => void
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

export type TransactionLog = {
	new (db: NativeDatabase, name: string): TransactionLog;
	addEntry(data: Buffer | Uint8Array, txnId?: number): void;
	getLogFileSize(sequenceId?: number): number;
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
	disableWAL?: boolean;
	enableStats?: boolean;
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
};

type ResolveCallback<T> = (value: T) => void;
type RejectCallback = (err: Error) => void;

export type UserSharedBufferCallback = () => void;

export type PurgeLogsOptions = { before?: number; destroy?: boolean; name?: string };

export type StatsHistogramData = {
	average: number;
	count: number;
	max: number;
	median: number;
	min: number;
	percentile95: number;
	percentile99: number;
	standardDeviation: number;
	sum: number;
};

export type NativeDatabase = {
	new (): NativeDatabase;
	addListener(event: string, callback: (...args: any[]) => void): void;
	clear(resolve: ResolveCallback<void>, reject: RejectCallback): void;
	clearSync(): void;
	close(): void;
	destroy(): void;
	drop(resolve: ResolveCallback<void>, reject: RejectCallback): void;
	dropSync(): void;
	flush(resolve: ResolveCallback<void>, reject: RejectCallback): void;
	flushSync(): void;
	notify(event: string | BufferWithDataView, args?: any[]): boolean;
	// Note that keyLengthOrKeyBuffer can be the length of the key if it was written into the shared buffer, or a direct buffer
	get(
		keyLengthOrKeyBuffer: number | Buffer,
		resolve: ResolveCallback<Buffer>,
		reject: RejectCallback,
		txnId?: number
	): number;
	getCount(options?: RangeOptions, txnId?: number): number;
	getDBIntProperty(propertyName: string): number;
	getDBProperty(propertyName: string): string;
	getMonotonicTimestamp(): number;
	getOldestSnapshotTimestamp(): number;
	getStat(statName: string): number | StatsHistogramData;
	getStats(all?: boolean): Record<string, number | StatsHistogramData>;
	getSync(keyLengthOrKeyBuffer: number | Buffer, flags: number, txnId?: number): Buffer;
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
	purgeLogs(options?: PurgeLogsOptions): string[];
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
	withLock(key: BufferWithDataView, callback: () => void | Promise<void>): Promise<void>;
};

export type RocksDatabaseConfig = { blockCacheSize?: number };

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
export const constants: {
	ALWAYS_CREATE_NEW_BUFFER_FLAG: number;
	NOT_IN_MEMORY_CACHE_FLAG: number;
	ONLY_IF_IN_MEMORY_CACHE_FLAG: number;
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
export const stats: {
	histograms: string[];
	tickers: string[];
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
