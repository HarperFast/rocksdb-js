import type { BackupStreamOptions } from './backup-stream.js';
import type { BackupOptions } from './backup.js';
import { DBI, type DBITransactional } from './dbi.js';
import type { BufferWithDataView, Encoder, EncoderFunction, Key } from './encoding.js';
import {
	addGlobalListener,
	config,
	globalListenerCount,
	globalNotify,
	removeGlobalListener,
	type PurgedLog,
	type PurgeLogsOptions,
	type RocksDatabaseConfig,
	type NativeTransactionOptions,
} from './load-binding.js';
import type { StatsAll, StatsDefault, StatsValue } from './stats.js';
import {
	type ArrayBufferWithNotify,
	CompactOptions,
	ITERATOR_STATE_BUFFER,
	KEY_BUFFER,
	Store,
	type StoreOptions,
	type UserSharedBufferOptions,
	VALUE_BUFFER,
} from './store.js';
import {
	RETRY_NOW,
	Transaction,
	TransactionAbandonedError,
	TransactionAlreadyAbortedError,
	TransactionIsBusyError,
} from './transaction.js';
import { Encoder as MsgpackEncoder } from 'msgpackr';
import { existsSync, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import * as orderedBinary from 'ordered-binary';

export type TransactionCallback<T> = (txn: Transaction, attempt: number) => T | PromiseLike<T>;

export interface RocksDatabaseOptions extends StoreOptions {
	/**
	 * The column family name.
	 *
	 * @default 'default'
	 */
	name?: string;
}

export interface TransactionOptions extends NativeTransactionOptions {
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
}

export type RocksDBStat = StatsValue;
export type RocksDBStats = StatsDefault | StatsAll;

/**
 * The main class for interacting with a RocksDB database.
 *
 * Before using this class, you must open the database first.
 *
 * @example
 * ```typescript
 * const db = RocksDatabase.open('/path/to/database');
 * await db.put('key', 'value');
 * const value = await db.get('key');
 * db.close();
 * ```
 */
export class RocksDatabase extends DBI<DBITransactional> {
	/**
	 * The name of the database.
	 */
	#name: string;

	constructor(pathOrStore: string | Store, options?: RocksDatabaseOptions) {
		if (typeof pathOrStore === 'string') {
			super(new Store(pathOrStore, options));
		} else if (pathOrStore instanceof Store) {
			super(pathOrStore);
		} else {
			throw new TypeError('Invalid database path or store');
		}
		this.#name = options?.name ?? 'default';
	}

	/**
	 * Removes all data from the database asynchronously.
	 *
	 * @example
	 * ```typescript
	 * const db = RocksDatabase.open('/path/to/database');
	 * await db.clear();
	 * ```
	 */
	clear(): Promise<void> {
		if (this.store.encoder?.structures !== undefined) {
			this.store.encoder.structures = [];
		}

		return new Promise((resolve, reject) => {
			this.store.db.clear(resolve, reject);
		});
	}

	/**
	 * Removes all entries from the database synchronously.
	 *
	 * @example
	 * ```typescript
	 * const db = RocksDatabase.open('/path/to/database');
	 * db.clearSync();
	 * ```
	 */
	clearSync(): void {
		if (this.store.encoder?.structures !== undefined) {
			this.store.encoder.structures = [];
		}

		this.store.db.clearSync();
	}

	/**
	 * Closes the database.
	 *
	 * @example
	 * ```typescript
	 * const db = RocksDatabase.open('/path/to/database');
	 * db.close();
	 * ```
	 */
	close(): void {
		this.store.close();
	}

	/**
	 * Compacts the entire key range of the database asynchronously.
	 * This triggers manual compaction which removes tombstones and reclaims space.
	 *
	 * @example
	 * ```typescript
	 * const db = RocksDatabase.open('/path/to/database');
	 * await db.compact();
	 * ```
	 */
	compact(options?: CompactOptions): Promise<void> {
		return this.store.compact(options);
	}

	/**
	 * Creates a backup of the entire database (all column families) into the
	 * given directory, creating parent directories as needed, and resolves with
	 * the new backup id. Use the `backups` namespace to restore, list, delete,
	 * purge, or verify backups.
	 *
	 * When the database was opened with `disableWAL`, the memtable is flushed
	 * before the backup by default so unflushed data is not lost.
	 *
	 * @example
	 * ```typescript
	 * import { backups } from '@harperfast/rocksdb-js';
	 *
	 * const db = RocksDatabase.open('/path/to/database');
	 * const id = await db.backup('/path/to/backups');
	 * ```
	 */
	backup(backupDir: string, options?: BackupOptions): Promise<number>;
	/**
	 * Streams a consistent snapshot of the entire database to a `WritableStream`
	 * as a tar archive, with no intermediate copy written to disk. Resolves once
	 * the stream has been fully written and closed.
	 *
	 * Backpressure is honored end to end, so a slow consumer (e.g. a network or
	 * S3 upload) paces the backup rather than buffering it in memory. The archive
	 * unpacks with any tar tool into a directory that opens as a RocksDB database.
	 *
	 * @example
	 * ```typescript
	 * const file = await fetch(uploadUrl, { method: 'PUT', body: stream });
	 * await db.backup(stream); // `stream` is the request's WritableStream
	 * ```
	 */
	backup(stream: WritableStream<Uint8Array>, options?: BackupStreamOptions): Promise<void>;
	backup(
		target: string | WritableStream<Uint8Array>,
		options?: BackupOptions | BackupStreamOptions
	): Promise<number | void> {
		if (typeof target === 'string') {
			return this.store.backup(target, options as BackupOptions);
		}
		return this.store.backup(target, options as BackupStreamOptions);
	}

	/**
	 * Creates a hardlinked, point-in-time, fully independent copy of the entire
	 * database (all column families) at `targetPath` and resolves once written.
	 *
	 * Unlike a backup, a checkpoint is a normal, writable sibling database: open
	 * it with {@link RocksDatabase.open} and it diverges independently from the
	 * source. SST and blob files are hardlinked when `targetPath` is on the same
	 * filesystem as the database and copied otherwise (other files such as the
	 * MANIFEST are always copied), so the operation is near-instant on the same
	 * filesystem. The memtable is flushed so the checkpoint includes the latest
	 * writes even when the WAL is disabled.
	 *
	 * Parent directories are created as needed. `targetPath` itself must not
	 * already exist (RocksDB creates the checkpoint directory) and rejects with
	 * `Create checkpoint failed: target path exists` if it does. The caller is
	 * responsible for eventual cleanup of the directory.
	 *
	 * @example
	 * ```typescript
	 * const db = RocksDatabase.open('/path/to/database');
	 * await db.createCheckpoint('/path/to/checkpoint');
	 * const branch = RocksDatabase.open('/path/to/checkpoint');
	 * ```
	 */
	createCheckpoint(targetPath: string): Promise<void> {
		return new Promise((resolve, reject) => {
			if (existsSync(targetPath)) {
				reject(new Error('Create checkpoint failed: target path exists'));
				return;
			}
			// Create parent directories as needed (a throw here rejects the promise),
			// then hand off to the native worker synchronously so it registers its
			// in-flight operation before this call returns.
			mkdirSync(dirname(targetPath), { recursive: true });
			this.store.db.createCheckpoint(resolve, reject, targetPath);
		});
	}

	/**
	 * Compacts the entire key range of the database synchronously.
	 * This triggers manual compaction which removes tombstones and reclaims space.
	 *
	 * @example
	 * ```typescript
	 * const db = RocksDatabase.open('/path/to/database');
	 * db.compactSync();
	 * ```
	 */
	compactSync(options?: CompactOptions): void {
		return this.store.compactSync(options);
	}

	/**
	 * Returns the list of column families in the RocksDB database.
	 */
	get columns(): string[] {
		return this.store.db.columns;
	}

	/**
	 * Set global database settings.
	 *
	 * @param options - The options for the database.
	 *
	 * @example
	 * ```typescript
	 * RocksDatabase.config({ blockCacheSize: 1024 * 1024 });
	 * ```
	 */
	static config(options: RocksDatabaseConfig): void {
		config(options);
	}

	/**
	 * Registers a process-wide event listener. Internal events emitted by the
	 * native binding use namespaced keys (e.g. `'transactionLog:warning'`).
	 *
	 * Listeners are not tied to any specific database — they fire for every
	 * matching event emitted in this process.
	 *
	 * @example
	 * ```typescript
	 * RocksDatabase.on('transactionLog:warning', (warning) => {
	 *   console.warn(warning);
	 * });
	 * ```
	 */
	static on(event: string, callback: (...args: any[]) => void): void {
		addGlobalListener(event, callback);
	}

	/**
	 * Alias for {@link RocksDatabase.on}, mirroring the Node `EventEmitter` API.
	 */
	static addListener(event: string, callback: (...args: any[]) => void): void {
		addGlobalListener(event, callback);
	}

	/**
	 * Removes a previously-registered process-wide event listener. The
	 * callback identity must match the one passed to {@link RocksDatabase.on}.
	 *
	 * @returns `true` if a matching listener was removed.
	 */
	static off(event: string, callback: (...args: any[]) => void): boolean {
		return removeGlobalListener(event, callback);
	}

	/**
	 * Alias for {@link RocksDatabase.off}, mirroring the Node `EventEmitter` API.
	 */
	static removeListener(event: string, callback: (...args: any[]) => void): boolean {
		return removeGlobalListener(event, callback);
	}

	/**
	 * Returns the number of process-wide listeners registered for the given event.
	 */
	static listenerCount(event: string): number {
		return globalListenerCount(event);
	}

	/**
	 * Emits a process-wide event. Mostly intended for tests and as a peer to
	 * {@link RocksDatabase.on} — native code should call `emitGlobalEvent` in
	 * `napi/global_events.h` directly rather than round-tripping through JS.
	 *
	 * @returns `true` if there was at least one listener.
	 */
	static notify(event: string, ...args: any[]): boolean {
		return globalNotify(event, args);
	}

	// committed

	destroy(): void {
		this.store.db.destroy();
	}

	async drop(): Promise<void> {
		return new Promise((resolve, reject) => {
			this.store.db.drop(resolve, reject);
		});
	}

	dropSync(): void {
		return this.store.db.dropSync();
	}

	get encoder(): Encoder | null {
		return this.store.encoder;
	}

	/**
	 * Flushes the underlying database by performing a commit or clearing any buffered operations.
	 *
	 * @return {void} Does not return a value.
	 */
	flush(): Promise<void> {
		return new Promise((resolve, reject) => this.store.db.flush(resolve, reject));
	}

	/**
	 * Synchronously flushes the underlying database by performing a commit or clearing any buffered operations.
	 *
	 * @return {void} Does not return a value.
	 */
	flushSync(): void {
		return this.store.db.flushSync();
	}

	// flushed

	/**
	 * Gets a RocksDB database property as an integer.
	 *
	 * @param propertyName - The name of the property to retrieve (e.g., 'rocksdb.num-blob-files').
	 * @returns The property value as a number.
	 *
	 * @example
	 * ```typescript
	 * const db = RocksDatabase.open('/path/to/database');
	 * const blobFiles = db.getDBIntProperty('rocksdb.num-blob-files');
	 * const numKeys = db.getDBIntProperty('rocksdb.estimate-num-keys');
	 * ```
	 */
	getDBIntProperty(propertyName: string): number | undefined {
		return this.store.db.getDBIntProperty(propertyName);
	}

	/**
	 * Gets a RocksDB database property as a string.
	 *
	 * @param propertyName - The name of the property to retrieve (e.g., 'rocksdb.levelstats').
	 * @returns The property value as a string.
	 *
	 * @example
	 * ```typescript
	 * const db = RocksDatabase.open('/path/to/database');
	 * const levelStats = db.getDBProperty('rocksdb.levelstats');
	 * const stats = db.getDBProperty('rocksdb.stats');
	 * ```
	 */
	getDBProperty(propertyName: string): string | undefined {
		return this.store.db.getDBProperty(propertyName);
	}

	/**
	 * Retrieves the estimated number of keys in the database.
	 *
	 * @example
	 * ```typescript
	 * const db = RocksDatabase.open('/path/to/database');
	 * const estimated = db.getEstimatedKeyCount();
	 * console.log(estimated);
	 * ```
	 */
	getEstimatedKeyCount(): number {
		return this.getDBIntProperty('rocksdb.estimate-num-keys') ?? 0;
	}

	/**
	 * Returns the current timestamp as a monotonically increasing timestamp in
	 * milliseconds represented as a decimal number.
	 *
	 * @returns The current monotonic timestamp in milliseconds.
	 */
	getMonotonicTimestamp(): number {
		return this.store.db.getMonotonicTimestamp();
	}

	/**
	 * Returns a number representing a unix timestamp of the oldest unreleased
	 * snapshot.
	 *
	 * @returns The oldest snapshot timestamp.
	 */
	getOldestSnapshotTimestamp(): number {
		return this.store.db.getOldestSnapshotTimestamp();
	}

	/**
	 * Gets a RocksDB statistic.
	 *
	 * @param statName - The name of the statistic to retrieve.
	 * @returns The statistic value.
	 */
	getStat(statName: string): RocksDBStat {
		return this.store.db.getStat(statName);
	}

	/**
	 * Gets the RocksDB statistics. The RocksDB ticker/histogram stats require
	 * statistics to be enabled, but the result always includes a summarized,
	 * aggregate set of `txnlog.*` keys (across all of this database's transaction
	 * logs), regardless of whether statistics are enabled. For detailed per-log
	 * statistics, including memory-map usage, use `log.getStats()` on the log
	 * returned by {@link RocksDatabase#useLog}.
	 *
	 * @example
	 * ```typescript
	 * const db = RocksDatabase.open('/path/to/database');
	 * const stats = db.getStats();
	 * stats['txnlog.totalSizeBytes']; // bytes across all transaction logs
	 * ```
	 */
	getStats(all?: false): StatsDefault;
	getStats(all: true): StatsAll;
	getStats(all = false): StatsDefault | StatsAll {
		return all ? this.store.db.getStats(true) : this.store.db.getStats(false);
	}

	/**
	 * Gets or creates a buffer that can be shared across worker threads.
	 *
	 * @param key - The key to get or create the buffer for.
	 * @param defaultBuffer - The default buffer to copy and use if the buffer
	 * does not exist.
	 * @param [options] - The options for the buffer.
	 * @param [options.callback] - A optional callback that receives
	 * key-specific events.
	 * @returns An `ArrayBuffer` that is internally backed by a shared block of
	 * memory. The buffer also has `notify()` and `cancel()` methods that can be
	 * used to notify the specified `options.callback`.
	 *
	 * @example
	 * ```typescript
	 * const db = RocksDatabase.open('/path/to/database');
	 * const buffer = db.getUserSharedBuffer('foo', new ArrayBuffer(10));
	 */
	getUserSharedBuffer(
		key: Key,
		defaultBuffer: ArrayBuffer,
		options?: UserSharedBufferOptions
	): ArrayBufferWithNotify {
		return this.store.getUserSharedBuffer(key, defaultBuffer, options);
	}

	/**
	 * Returns whether the database has a lock for the given key.
	 *
	 * @param key - The key to check.
	 * @returns `true` if the database has a lock for the given key, `false`
	 * otherwise.
	 *
	 * @example
	 * ```typescript
	 * const db = RocksDatabase.open('/path/to/database');
	 * db.hasLock('foo'); // false
	 * db.tryLock('foo', () => {});
	 * db.hasLock('foo'); // true
	 * ```
	 */
	hasLock(key: Key): boolean {
		return this.store.hasLock(key);
	}

	async ifNoExists(_key: Key): Promise<void> {
		//
	}

	/**
	 * Checks the process-global verification table for a fresh version match
	 * on `key`. Returns `true` when the table currently records `version`. Use
	 * this as a fast cache-freshness check before falling back to a full read:
	 *
	 * ```typescript
	 * if (db.verifyVersion(key, cachedEntry.version)) {
	 *   return cachedEntry.value;
	 * }
	 * const value = db.getSync(key);
	 * db.populateVersion(key, extractVersion(value));
	 * ```
	 */
	verifyVersion(key: Key, version: number): boolean {
		return this.store.verifyVersion(key, version);
	}

	/**
	 * Seeds the verification-table slot for `key` with `version`. Has no
	 * effect if the slot is currently lock-tagged or if the verification
	 * table is disabled.
	 */
	populateVersion(key: Key, version: number): void {
		this.store.populateVersion(key, version);
	}

	/**
	 * Returns whether the database is open.
	 *
	 * @returns `true` if the database is open, `false` otherwise.
	 */
	isOpen(): boolean {
		return this.store.isOpen();
	}

	/**
	 * Lists all transaction log names.
	 *
	 * @returns an array of transaction log names.
	 */
	listLogs(): string[] {
		return this.store.listLogs();
	}

	/**
	 * The name of the database.
	 */
	get name(): string {
		return this.#name;
	}

	/**
	 * Whether the database is open in readonly mode.
	 */
	get readOnly(): boolean {
		return this.store.readOnly;
	}

	/**
	 * Sugar method for opening a database.
	 *
	 * @param pathOrStore - The filesystem path to the database or a custom store.
	 * @param options - The options for the database.
	 * @returns A new RocksDatabase instance.
	 *
	 * @example
	 * ```typescript
	 * const db = RocksDatabase.open('/path/to/database');
	 * ```
	 */
	static open(pathOrStore: string | Store, options?: RocksDatabaseOptions): RocksDatabase {
		return new RocksDatabase(pathOrStore, options).open();
	}

	/**
	 * Opens the database. This function returns immediately if the database is
	 * already open.
	 *
	 * @returns A new RocksDatabase instance.
	 *
	 * @example
	 * ```typescript
	 * const db = new RocksDatabase('/path/to/database');
	 * db.open();
	 * ```
	 */
	open(): RocksDatabase {
		const { store } = this;

		if (store.open()) {
			// already open
			return this;
		}

		store.db.setDefaultValueBuffer(VALUE_BUFFER);
		store.db.setDefaultKeyBuffer(KEY_BUFFER);
		store.db.setIteratorState(ITERATOR_STATE_BUFFER);

		/**
		 * The encoder initialization precedence is:
		 * 1. encoder.Encoder
		 * 2. encoder.encode()
		 * 3. encoding === `msgpack`
		 * 4. encoding === `ordered-binary`
		 * 5. encoder.writeKey()
		 */
		let EncoderClass: EncoderFunction | undefined = store.encoder?.Encoder;
		if (store.encoding === false) {
			store.encoder = null;
			EncoderClass = undefined;
		} else if (typeof EncoderClass === 'function') {
			store.encoder = null;
		} else if (
			typeof store.encoder?.encode !== 'function' &&
			(!store.encoding || store.encoding === 'msgpack')
		) {
			store.encoding = 'msgpack';
			EncoderClass = MsgpackEncoder;
		}

		if (EncoderClass) {
			const opts: Record<string, any> = {
				copyBuffers: true,
				freezeData: store.freezeData,
				randomAccessStructure: store.randomAccessStructure,
			};
			const { sharedStructuresKey } = store;
			if (sharedStructuresKey) {
				opts.getStructures = (): any => {
					const buffer = this.getBinarySync(sharedStructuresKey);
					return buffer && store.decoder?.decode
						? store.decoder.decode(buffer as BufferWithDataView)
						: undefined;
				};
				opts.saveStructures = (
					structures: any,
					isCompatible: boolean | ((existingStructures: any) => boolean)
				) => {
					return this.transactionSync(
						(txn: Transaction, _attempt: number) => {
							// note: we need to get a fresh copy of the shared structures,
							// so we don't want to use the transaction's getBinarySync()
							const existingStructuresBuffer = this.getBinarySync(sharedStructuresKey);
							const existingStructures =
								existingStructuresBuffer && store.decoder?.decode
									? store.decoder.decode(existingStructuresBuffer as BufferWithDataView)
									: undefined;
							if (typeof isCompatible == 'function') {
								if (!isCompatible(existingStructures)) {
									return false;
								}
							} else if (existingStructures && existingStructures.length !== isCompatible) {
								return false;
							}
							txn.putSync(sharedStructuresKey, structures);
						},
						{ retryOnBusy: true }
					);
				};
			}
			store.encoder = new EncoderClass({ ...opts, ...store.encoder });
			store.decoder = store.encoder;
		} else if (typeof store.encoder?.encode === 'function') {
			if (!store.decoder) {
				store.decoder = store.encoder;
			}
		} else if (store.encoding === 'ordered-binary') {
			store.encoder = { readKey: orderedBinary.readKey, writeKey: orderedBinary.writeKey };
			store.decoder = store.encoder;
		}

		if (typeof store.encoder?.writeKey === 'function' && !store.encoder?.encode) {
			// define a fallback encode method that uses writeKey to encode values
			store.encoder = {
				...store.encoder,
				encode: (value: any, _mode?: number): Buffer => {
					const bytesWritten = store.writeKey(value, store.encodeBuffer, 0);
					return store.encodeBuffer.subarray(0, bytesWritten);
				},
				copyBuffers: true,
			};
		}

		if (store.encoder) {
			store.encoder.name = this.#name;
		}

		if (store.decoder && store.decoder.needsStableBuffer !== true) {
			store.decoderCopies = true;
		}

		if (store.decoder?.readKey && !store.decoder.decode) {
			store.decoder.decode = (buffer: BufferWithDataView): any => {
				if (store.decoder?.readKey) {
					return store.decoder.readKey(buffer, 0, buffer.end);
				}
				return buffer;
			};
			store.decoderCopies = true;
		}

		return this;
	}

	/**
	 * Returns the path to the database.
	 */
	get path(): string {
		return this.store.path;
	}

	/**
	 * Purges transaction logs.
	 *
	 * By default returns the paths of the deleted log files. Pass
	 * `includeEntryCounts: true` to instead return, for each deleted file, its
	 * path and the number of entries it held (`{ path, entries }`).
	 */
	purgeLogs(options: PurgeLogsOptions & { includeEntryCounts: true }): PurgedLog[];
	purgeLogs(options?: PurgeLogsOptions & { includeEntryCounts?: false }): string[];
	purgeLogs(options?: PurgeLogsOptions): string[] | PurgedLog[];
	purgeLogs(options?: PurgeLogsOptions): string[] | PurgedLog[] {
		return this.store.db.purgeLogs(options);
	}

	/**
	 * The status of the database.
	 */
	get status(): 'open' | 'closed' {
		return this.store.isOpen() ? 'open' : 'closed';
	}

	/**
	 * Executes all operations in the callback as a single transaction.
	 *
	 * @param callback - A async function that receives the transaction as an argument.
	 * @returns A promise that resolves the `callback` return value.
	 *
	 * @example
	 * ```typescript
	 * const db = RocksDatabase.open('/path/to/database');
	 * await db.transaction(async (txn) => {
	 *   await txn.put('key', 'value');
	 * });
	 * ```
	 */
	async transaction<T>(
		callback: TransactionCallback<T>,
		options?: TransactionOptions
	): Promise<T | void> {
		if (typeof callback !== 'function') {
			throw new TypeError('Callback must be a function');
		}

		const maxRetries = options?.maxRetries ?? 3;
		const txn = new Transaction(this.store, options);
		let result: T | PromiseLike<T>;

		this.notify('begin-transaction');

		for (let attempt = 1; attempt <= maxRetries; attempt++) {
			try {
				result = await callback(txn, attempt);
			} catch (callbackErr) {
				return this.#abortTransaction(txn, callbackErr);
			}

			let commitResult: typeof RETRY_NOW | void;
			try {
				commitResult = await txn.commit();
			} catch (commitErr) {
				if (commitErr instanceof TransactionAlreadyAbortedError) {
					return;
				}
				if (
					commitErr instanceof TransactionIsBusyError &&
					(options?.retryOnBusy ?? commitErr.hasLog) &&
					attempt < maxRetries
				) {
					// retry the transaction
					continue;
				}

				this.#abandonTransaction(txn, commitErr);
				return;
			}

			if (commitResult !== RETRY_NOW) {
				return result;
			}
			// coordinatedRetry: the conflict resolved, retry immediately — but only
			// if attempts remain. On the final attempt a RETRY_NOW must not fall
			// out of the loop silently (that would resolve undefined and leave the
			// transaction un-aborted); abandon it like an exhausted ERR_BUSY retry.
			if (attempt >= maxRetries) {
				this.#abandonTransaction(
					txn,
					new Error(`Transaction did not commit after ${maxRetries} coordinated retries`)
				);
				return;
			}
		}
	}

	/**
	 * Executes all operations in the callback as a single transaction.
	 *
	 * @param callback - A function that receives the transaction as an
	 * argument. If the callback return promise-like value, it is awaited
	 * before committing the transaction. Otherwise, the callback is treated as
	 * synchronous.
	 * @returns The `callback` return value.
	 *
	 * @example
	 * ```typescript
	 * const db = RocksDatabase.open('/path/to/database');
	 * await db.transaction(async (txn) => {
	 *   await txn.put('key', 'value');
	 * });
	 * ```
	 */
	transactionSync<T>(
		callback: TransactionCallback<T>,
		options?: TransactionOptions
	): T | PromiseLike<T> | void {
		if (typeof callback !== 'function') {
			throw new TypeError('Callback must be a function');
		}

		const maxRetries = options?.maxRetries ?? 3;

		const isRetryable = (err: Error | unknown, attempt: number) => {
			return (
				err instanceof TransactionIsBusyError &&
				(options?.retryOnBusy ?? err.hasLog) &&
				attempt <= maxRetries
			);
		};

		const runAttempt = (attempt: number): T | PromiseLike<T> | void => {
			const txn = new Transaction(this.store, options);
			let result: T | PromiseLike<T>;

			try {
				result = callback(txn, attempt);
			} catch (callbackErr) {
				return this.#abortTransaction(txn, callbackErr);
			}

			// despite being 'sync', we need to support async operations
			if (typeof (result as PromiseLike<T>)?.then === 'function') {
				return (result as PromiseLike<T>).then((value: T | undefined) => {
					try {
						txn.commitSync();
						return value;
					} catch (commitErr) {
						if (commitErr instanceof TransactionAlreadyAbortedError) {
							return;
						}
						if (isRetryable(commitErr, attempt)) {
							return runAttempt(attempt + 1) as PromiseLike<T>;
						}
						this.#abandonTransaction(txn, commitErr);
					}
				}) as PromiseLike<T>;
			}

			try {
				txn.commitSync();
				return result;
			} catch (commitErr) {
				if (commitErr instanceof TransactionAlreadyAbortedError) {
					return;
				}
				if (isRetryable(commitErr, attempt)) {
					return runAttempt(attempt + 1);
				}
				this.#abandonTransaction(txn, commitErr);
			}
		};

		this.notify('begin-transaction');
		return runAttempt(1);
	}

	#abortTransaction(txn: Transaction, callbackErr: Error | unknown): void {
		// either a user error or a already aborted/committed error
		try {
			// in the event of a user error, we need to abort the transaction
			txn.abort();
		} catch (abortErr) {
			if (abortErr instanceof TransactionAlreadyAbortedError) {
				return;
			}
		}
		// rethrow the user error
		throw callbackErr;
	}

	#abandonTransaction(txn: Transaction, commitErr: Error | unknown): void {
		try {
			txn.abort();
		} catch (abortErr) {
			if (abortErr instanceof TransactionAbandonedError) {
				throw abortErr;
			}
		}

		// rethrow the original error
		throw commitErr;
	}

	/**
	 * Attempts to acquire a lock for a given key. If the lock is available,
	 * the function returns `true` and the optional callback is never called.
	 * If the lock is not available, the function returns `false` and the
	 * callback is queued until the lock is released.
	 *
	 * @param key - The key to lock.
	 * @param onUnlocked - A callback to call when the lock is released.
	 *
	 * @example
	 * ```typescript
	 * const db = RocksDatabase.open('/path/to/database');
	 * db.tryLock('foo', () => {
	 *   console.log('lock acquired');
	 * });
	 * ```
	 * @returns `true` if the lock was acquired, `false` otherwise.
	 */
	tryLock(key: Key, onUnlocked?: () => void): boolean {
		return this.store.tryLock(key, onUnlocked);
	}

	/**
	 * Releases the lock on the given key and calls any queued `onUnlocked`
	 * callback handlers.
	 *
	 * @param key - The key to unlock.
	 * @returns `true` if the lock was released or `false` if the lock did not
	 * exist.
	 *
	 * @example
	 * ```typescript
	 * const db = RocksDatabase.open('/path/to/database');
	 * db.tryLock('foo', () => {});
	 * db.unlock('foo'); // returns `true`
	 * db.unlock('foo'); // already unlocked, returns `false`
	 * ```
	 */
	unlock(key: Key): void {
		return this.store.unlock(key);
	}

	/**
	 * Excecutes a function using a thread-safe lock to ensure mutual
	 * exclusion.
	 *
	 * @param callback - A callback to call when the lock is acquired.
	 * @returns A promise that resolves when the lock is acquired.
	 *
	 * @example
	 * ```typescript
	 * const db = RocksDatabase.open('/path/to/database');
	 * await db.withLock(async (waited) => {
	 *   console.log('lock acquired', waited);
	 * });
	 * ```
	 */
	withLock(key: Key, callback: () => void | Promise<void>): Promise<void> | undefined {
		return this.store.withLock(key, callback);
	}
}
