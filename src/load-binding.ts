import type { IteratorOptions, RangeOptions } from './dbi.js';
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
	new (name: string): TransactionLog;
	addEntry(data: Buffer | Uint8Array, txnId?: number): void;
	getLogFileSize(sequenceId?: number): number;
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

export declare class NativeIteratorCls<T> implements Iterator<T> {
	constructor(context: StoreContext, options: IteratorOptions);
	next(): IteratorResult<T>;
	return(): IteratorResult<T>;
	throw(): IteratorResult<T>;
}

export type NativeDatabaseMode = 'optimistic' | 'pessimistic';

export type NativeDatabaseOptions = {
	disableWAL?: boolean;
	mode?: NativeDatabaseMode;
	name?: string;
	noBlockCache?: boolean;
	parallelismThreads?: number;
	transactionLogMaxAgeThreshold?: number;
	transactionLogMaxSize?: number;
	transactionLogRetentionMs?: number;
	transactionLogsPath?: string;
};

type ResolveCallback<T> = (value: T) => void;
type RejectCallback = (err: Error) => void;

export type UserSharedBufferCallback = () => void;

export type PurgeLogsOptions = { before?: number; destroy?: boolean; name?: string };

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
	TRANSACTION_LOG_TOKEN: number;
	TRANSACTION_LOG_FILE_HEADER_SIZE: number;
	TRANSACTION_LOG_ENTRY_HEADER_SIZE: number;
	ONLY_IF_IN_MEMORY_CACHE_FLAG: number;
	NOT_IN_MEMORY_CACHE_FLAG: number;
	ALWAYS_CREATE_NEW_BUFFER_FLAG: number;
} = binding.constants;
export const NativeDatabase: NativeDatabase = binding.Database;
export const NativeIterator: typeof NativeIteratorCls = binding.Iterator;
export const NativeTransaction: NativeTransaction = binding.Transaction;
export const TransactionLog: TransactionLog = binding.TransactionLog;
export const registryStatus: () => RegistryStatus = binding.registryStatus;
export const shutdown: () => void = binding.shutdown;
export const version: string = binding.version;
