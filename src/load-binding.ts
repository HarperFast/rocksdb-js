import type { BackupInfo, BackupOptions, RestoreOptions } from './backup.ts';
import type { RangeOptions } from './dbi.ts';
import type { BufferWithDataView, Key } from './encoding.ts';
import type { StatsAll, StatsDefault, StatsHistogramData } from './stats.ts';
import type { StoreContext } from './store.ts';
import type { TransactionLogStoreValidation } from './validate-transaction-log.ts';
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
} from './stats.ts';
import { execSync } from 'node:child_process';
import { readdirSync, readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

/**
 * The `Error` subclass passed to `'error'` event listeners and returned by
 * `db.getLastError()` when RocksDB reports a background error (e.g. a write
 * failing at the filesystem level). A real `Error` instance (`instanceof Error`
 * and `instanceof BackgroundError` both hold) with the fields below. Only a
 * hard-or-worse error (`writesDisabled`) stops writes; a soft error is
 * auto-recoverable. When `writesDisabled` is `true` and the underlying condition
 * has cleared, call {@link RocksDatabase.resume}. See HarperFast/rocksdb-js#730.
 *
 * The runtime constructor is defined natively and exported as
 * {@link BackgroundError}; the same name is both a value (for `instanceof`) and
 * this instance type.
 *
 * @example
 * ```typescript
 * db.on('error', (err: BackgroundError) => {
 *   if (err.writesDisabled) console.error(`writes disabled: ${err.message}`);
 * });
 * ```
 */
export interface BackgroundError extends Error {
	/** Always `'BackgroundError'`. */
	name: string;
	/** Discriminator for the error class; always `'background'` here. */
	type: string;
	/**
	 * The RocksDB `Status::Severity` as a number: 1 soft, 2 hard, 3 fatal,
	 * 4 unrecoverable.
	 */
	severity: number;
	/** Human-readable severity: `'soft'`, `'hard'`, `'fatal'`, or `'unrecoverable'`. */
	severityName: string;
	/**
	 * Whether RocksDB has disabled writes on the database in response to this
	 * error (severity hard or worse, i.e. `severity >= 2`). Only when `true` is
	 * {@link RocksDatabase.resume} warranted; a soft error auto-recovers and
	 * leaves writes enabled. Distinct from opening the database in read-only
	 * mode — this is RocksDB halting writes after a background failure.
	 */
	writesDisabled: boolean;
	/**
	 * The RocksDB `BackgroundErrorReason` as a number, present when the error
	 * originated from a reason-bearing callback (flush, compaction, etc.).
	 */
	reason?: number;
	/** Human-readable reason, e.g. `'flush'` or `'compaction'`. */
	reasonName?: string;
}

/**
 * The shape accepted by `db.setLastError(...)` to inject or reset a background
 * error. `message` is required; the rest default/omit as with a real error.
 * `type` defaults to `'background'`. Pass `null`/nothing to `setLastError` to
 * clear instead of an object of this shape.
 */
export type BackgroundErrorOptions = {
	message: string;
	severity?: number;
	severityName?: string;
	writesDisabled?: boolean;
	reason?: number;
	reasonName?: string;
	type?: string;
};

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
	abandonWrites(): void;
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
	// The transaction holds no napi_ref to its database (#741), so the owning
	// NativeDatabase must be supplied per-call; native rejects a foreign one.
	useLog(name: string | number, jsDatabase: NativeDatabase): TransactionLog;
};

export type LogBuffer = Buffer & {
	dataView: DataView;
	logId: number;
	size: number;
};

export type TransactionLogQueryOptions = {
	start?: number;
	end?: number;
	exactStart?: boolean;
	startFromLastFlushed?: boolean;
	readUncommitted?: boolean;
	exclusiveStart?: boolean;
};

export type TransactionEntry = {
	timestamp: number;
	data: Buffer;
	endTxn: boolean;
};

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

/**
 * One volume RocksDB may place SST files on.
 *
 * RocksDB fills paths in order: newer/smaller levels go to earlier entries and
 * spill to the next once the running total of estimated level sizes exceeds an
 * entry's `targetSize`. Placement is a static function of the target sizes and
 * the level-size options — it does not react to actual disk usage.
 *
 * Two constraints are worth knowing before using this:
 *
 * - A path's **index** is what gets recorded per SST file in the MANIFEST, so
 *   entries may only ever be **appended** across reopens. Reordering or
 *   removing one makes RocksDB look for existing files in the wrong directory.
 * - Supplying more than one entry disables
 *   `level_compaction_dynamic_level_bytes`, so level sizing becomes static.
 * - A directory must not be shared with another **database** — SST file
 *   numbers come from a per-database counter, so each would delete the other's
 *   live files during obsolete-file scanning.
 *
 * Blob files do **not** follow these paths — every blob file goes to
 * `blobs.dir` (or alongside the first path when unset).
 */
export type NativeStoragePath = {
	/**
	 * Directory SST files may be written to. Resolved to an absolute path
	 * against the process working directory when the database is opened, so the
	 * stored value cannot resolve to a different volume in a process started
	 * from somewhere else.
	 */
	path: string;
	/**
	 * Bytes this path should try to hold. Best-effort: RocksDB places a file
	 * here while the running total of estimated level sizes fits, then moves on
	 * to the next path. The last path is the fallback and takes everything that
	 * doesn't fit earlier, regardless of its target.
	 */
	targetSize: number;
};

/**
 * Blob-file settings for ONE column family.
 *
 * These are per-column-family in RocksDB and it does not restore them on open,
 * so — exactly like `compression` — they are applied to the family being opened
 * and to no other, and a field left out keeps whatever that family persisted
 * rather than reverting to the documented default. The defaults below describe a
 * family being CREATED.
 */
export type NativeBlobOptions = {
	/**
	 * Whether values at or above `minSize` are written to blob files.
	 *
	 * Turning this off does not make existing blob files unreadable, and with
	 * `garbageCollection` enabled compaction gradually pulls their values back
	 * inline into SST files. That costs write amplification — rewriting large
	 * values at every level is exactly what blob separation avoids.
	 *
	 * @default true
	 */
	enabled?: boolean;
	/**
	 * Smallest value stored in a blob file rather than inline in an SST file.
	 *
	 * @default 2048
	 */
	minSize?: number;
	/**
	 * Directory blob files are written to and read from. When unset they live
	 * alongside the SST files in the first entry of `paths`. Resolved to an
	 * absolute path against the process working directory when the database is
	 * opened.
	 *
	 * Unlike the other fields here, omitting this means "alongside the SST
	 * files" rather than "inherit": dropping it from a configuration is rejected
	 * at open rather than silently continuing to read blob files from a
	 * directory the configuration no longer mentions.
	 *
	 * Setting this decouples blob placement from SST placement, which is the
	 * only way to put large values on a different volume than the LSM tree
	 * (`paths` does not affect blob files).
	 *
	 * Must not be shared with another **database**: blob file numbers come from
	 * a per-database counter, so two databases pointed at one directory mint
	 * colliding names and each deletes the other's live files during
	 * obsolete-file scanning. Column families of the *same* database draw from
	 * one counter and may share. The same rule applies to `paths` entries.
	 *
	 * Requires a native build linked against a RocksDB carrying the `blob_dir`
	 * patch; opening with this set otherwise throws.
	 *
	 * A blob file's directory is derived from this option every time the file
	 * is opened — it is **not** recorded per file the way an SST's path index
	 * is. Reopening with a different directory therefore strands the existing
	 * blob files rather than moving them, so a mismatch against what the
	 * database was last opened with is rejected at open — see
	 * {@link NativeBlobOptions.allowDirChange} for the migration path.
	 */
	dir?: string;
	/**
	 * Acknowledges that the existing blob files have already been moved to
	 * `dir`, permitting an open that would otherwise be rejected as a mismatch
	 * against the directory recorded in the database's `OPTIONS` file.
	 *
	 * Nothing is moved for you — this only suppresses the check, so opening with
	 * it set before actually relocating the `.blob` files makes every value at
	 * or above `minSize` unreadable.
	 *
	 * @default false
	 */
	allowDirChange?: boolean;
	/**
	 * Whether compaction relocates live values out of the oldest blob files so
	 * those files can be deleted. Required for blob files to ever shrink.
	 *
	 * @default true
	 */
	garbageCollection?: boolean;
	/**
	 * Fraction (0–1) of the oldest blob files eligible for relocation. The
	 * RocksDB default of 0.25 leaves three quarters of the blob files
	 * untouched by any given compaction; raise it toward 1 to drain them.
	 *
	 * @default 0.25
	 */
	garbageCollectionAgeCutoff?: number;
	/**
	 * Garbage ratio (0–1) above which RocksDB schedules targeted compactions
	 * to reclaim the oldest blob files. The default of 1.0 never forces one.
	 *
	 * @default 1
	 */
	garbageCollectionForceThreshold?: number;
	/**
	 * Whether values written by a flush are inserted into the blob cache
	 * immediately instead of waiting to be read back. Only has an effect when
	 * a blob cache is configured via `RocksDatabase.config({ blobCacheSize })`.
	 *
	 * @default false
	 */
	prepopulateCache?: boolean;
};

export type NativeDatabaseOptions = {
	/**
	 * The friendly name of a compression algorithm compiled into this RocksDB
	 * build (see `supportedCompression`), e.g. `'lz4'`, `'zstd'`, `'none'`. The
	 * higher-level `StoreOptions.compression` (which also accepts an object with
	 * a level) is normalized down to this string plus `compressionLevel`.
	 */
	compression?: string;
	/**
	 * Compression level forwarded to RocksDB's `compression_opts.level`. Meaning
	 * is algorithm-specific; omit to use the algorithm's default.
	 */
	compressionLevel?: number;
	/**
	 * Apply `compression` to every column family the underlying `DB::Open` opens, rather than
	 * only the one named by `name`. Requires an explicit `compression`.
	 */
	compressionForAllColumnFamilies?: boolean;
	dbWriteBufferSize?: number;
	disableWAL?: boolean;
	/**
	 * Blob-file (large value) settings. Values at or above `blobs.minSize` are
	 * stored in separate blob files rather than inline in SST files, so
	 * compaction does not rewrite them at every level.
	 */
	blobs?: NativeBlobOptions;
	/**
	 * Volumes SST files may be placed on, mapped to RocksDB's `db_paths`.
	 * See {@link NativeStoragePath}.
	 */
	paths?: NativeStoragePath[];
	enableStats?: boolean;
	/**
	 * Verbosity of RocksDB's informational logging (`info_log_level`): `0`
	 * (debug), `1` (info), `2` (warn), `3` (error), `4` (fatal), or `5`
	 * (header-only). Omit to leave RocksDB's own default (`INFO_LEVEL` in a
	 * release build of the linked RocksDB library).
	 */
	infoLogLevel?: number;
	/**
	 * Per-file size cap (bytes) for informational log files (`LOG` /
	 * `LOG.old.*`, `max_log_file_size`). RocksDB retains up to 5 of these files
	 * (`keep_log_file_num`, not currently exposed as an option), so total
	 * informational-log footprint is bounded at roughly `5 * maxLogFileSize`.
	 *
	 * @default 16777216 (16MB)
	 */
	maxLogFileSize?: number;
	maxOpenFiles?: number;
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

export type FlushOptions = {
	/**
	 * Whether the flush may proceed even though it will stall writes for its duration.
	 *
	 * Maps to `rocksdb::FlushOptions::allow_write_stall`. RocksDB's default is `false`, which
	 * means the opposite of what the name suggests on first read: the flush **waits** until it
	 * can run without causing a stall. That wait has no timeout, so a database sitting in a stall
	 * condition — immutable-memtable backlog, L0 stop trigger, pending-compaction-bytes limit, an
	 * exhausted WriteBufferManager budget — blocks the caller for as long as the condition lasts.
	 * `flush()` runs on the libuv threadpool, so there the wait shows up as a promise that simply
	 * never settles while the event loop stays alive — and it parks that whole worker, not just
	 * the caller's promise: `UV_THREADPOOL_SIZE` defaults to 4, so a handful of concurrently
	 * stalled flushes can exhaust the pool and stall every unrelated `fs`/`dns`/`crypto` call and
	 * cold-cache `get()` in the process, not only this database's own operations. `flushSync()`
	 * takes the same wait on the JS thread, so it is the worse of the two here rather than the
	 * safer one: it freezes the event loop outright instead of parking a pool worker, and it holds
	 * the in-flight operation claim that `close()` waits on for the duration.
	 *
	 * Pass `true` when the flush is a durability gate the caller is blocked on and stalling
	 * writers is the acceptable cost of it completing — decide that up front, before issuing the
	 * call, since there is no way to cancel an in-flight `flush()` and turn `true` into an escape
	 * hatch after the fact. It is also not a free way out of an existing stall: forcing the
	 * memtable switch adds another L0 file to the level whose file count may be *causing* the
	 * stop trigger, so it can prolong the condition it is meant to clear. Weigh the cost
	 * database-wide, not per-caller: a flush covers **every column family** on the database, and
	 * the descriptor is process-global and shared across `worker_threads`, so the stall lands on
	 * every other column family and every other handle that opened the same path — not just the
	 * one you called. It also relocates the hang rather than removing it: a stalled `db->Write()`
	 * blocks the database's single `CommitWorker` thread, which dispatches every
	 * `Transaction.commit()` in order, so the stall queues up every commit behind it — including
	 * ones from callers that never touched flush — until the stall clears.
	 *
	 * Note this is a *different* knob from the `writeBufferManagerAllowStall` config, and their
	 * polarity is nearly opposite: that one decides whether the WriteBufferManager may stall
	 * writers at all, this one decides whether a manual flush is willing to cause a stall rather
	 * than wait one out.
	 *
	 * @default false
	 */
	allowWriteStall?: boolean;
};

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
	compact(
		resolve: ResolveCallback<void>,
		reject: RejectCallback,
		start?: Key,
		end?: Key,
		bottommost?: boolean
	): void;
	compactSync(start?: Key, end?: Key, bottommost?: boolean): void;
	columns: string[];
	createCheckpoint(
		resolve: ResolveCallback<void>,
		reject: RejectCallback,
		targetPath: string
	): void;
	destroy(): void;
	drop(resolve: ResolveCallback<void>, reject: RejectCallback): void;
	dropSync(): void;
	flush(resolve: ResolveCallback<void>, reject: RejectCallback, options?: FlushOptions): void;
	flushSync(options?: FlushOptions): void;
	notify(event: string | BufferWithDataView, args?: any[]): boolean;
	// Note that keyLengthOrKeyBuffer can be the length of the key if it was written into the shared buffer, or a direct buffer
	get(
		keyLengthOrKeyBuffer: number | Buffer,
		resolve: ResolveCallback<Buffer | number>,
		reject: RejectCallback,
		txnId?: number,
		expectedVersion?: number
	): number;
	estimateCount(startKey?: Buffer, endKey?: Buffer): { count: number; confidence: number };
	getCompression(): { algorithm: string; level?: number };
	getCount(options?: RangeOptions, txnId?: number): number;
	getLastError(): BackgroundError | null;
	setLastError(error?: BackgroundErrorOptions | null): void;
	getDBIntProperty(propertyName: string): number | undefined;
	getDBProperty(propertyName: string): string | undefined;
	getLogOptions(): { maxLogFileSize: number; infoLogLevel: number };
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
	resume(): void;
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
	// jsDatabase is accepted (and ignored) so Store.useLog can pass it
	// unconditionally for both StoreContext shapes; only the transaction
	// variant consumes it.
	useLog(name: string, jsDatabase?: NativeDatabase): TransactionLog;
	verifyVersion(keyLengthOrKeyBuffer: number | Buffer, version: number): boolean;
	withLock(key: BufferWithDataView, callback: () => void | Promise<void>): Promise<void>;
};

export type RocksDatabaseConfig = {
	blockCacheSize?: number;
	/**
	 * Capacity (bytes) of the process-wide cache for blob (large value)
	 * contents. RocksDB does **not** put blob values in the block cache, so
	 * with the default of 0 every blob read is real I/O — which matters a lot
	 * when blob files live on slower storage than the SST files
	 * (see `blobs.dir`).
	 *
	 * Kept separate from the block cache so large values cannot evict
	 * index/filter/data blocks. While this has never been set explicitly, a
	 * `config()` call that supplies `blockCacheSize` derives
	 * `Math.floor(blockCacheSize / 10)` for it — capacity that is **additional**
	 * to the block-cache capacity, so an existing `config({ blockCacheSize })`
	 * call raises the process memory ceiling by 10% with no code change. Setting
	 * this explicitly (including to `0`) latches: later calls that supply only
	 * `blockCacheSize` leave it alone.
	 *
	 * Must be set **before** a database is opened to affect it: the cache is
	 * attached to a column family at open, so a database opened while this was
	 * `0` keeps uncached blob reads for its lifetime. Changing it later resizes
	 * the cache for databases that already have it attached.
	 *
	 * @default 0, or 10% of `blockCacheSize` until set explicitly
	 */
	blobCacheSize?: number;
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
					isMusl ||
					execSync('ldd --version', {
						encoding: 'utf8',
						stdio: 'pipe',
					}).includes('musl');
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

export type RegistryStatusTransaction = {
	/** The transaction id assigned by the database descriptor. */
	id: number;
	/** Milliseconds since the transaction handle was created. */
	ageMs: number;
};

export type RegistryStatusDB = {
	path: string;
	refCount: number;
	columnFamilies: string[];
	transactions: number;
	/**
	 * One entry per live transaction handle. An `ageMs` beyond any plausible request lifetime,
	 * against a nonzero `rocksdb.num-snapshots`, identifies a handle holding back reclamation for
	 * its whole database.
	 */
	transactionDetails: RegistryStatusTransaction[];
	closables: number;
	locks: number;
	userSharedBuffers: number;
	listenerCallbacks: number;
};

export type RegistryStatus = RegistryStatusDB[];

const bindingPath = locateBinding();
// console.log(`Loading binding from ${bindingPath}`);
const binding = req(bindingPath);

/**
 * The native `BackgroundError` constructor (a real `Error` subclass). Exported
 * as both a value — for `err instanceof BackgroundError` — and, via declaration
 * merging with the interface above, a type. Instances are produced by the
 * `'error'` event and `db.getLastError()`; consumers rarely construct their own.
 * The `details` param is required and typed to the instance fields so a bare
 * `new BackgroundError()` (which would omit `severity` / `writesDisabled` / …)
 * is a type error.
 */
export const BackgroundError: new (
	details: Pick<
		BackgroundError,
		'message' | 'severity' | 'severityName' | 'writesDisabled' | 'reason' | 'reasonName'
	> & { type?: string }
) => BackgroundError = binding.BackgroundError;

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
	 * Producer flag in a value's metadata word (4 big-endian bytes at offset 8, top byte `0x0E`,
	 * low 24 bits flags), declaring that this version does NOT uniquely identify the value —
	 * the producer has stored more than one distinct value under it.
	 *
	 * Set it and the VerificationTable stops treating version equality as evidence for this value:
	 * a read never answers `FRESH_VERSION_FLAG` for it and never publishes its version to a slot,
	 * so a consumer holding a differing cached copy at that version cannot have it confirmed.
	 * Clear it again on the next write that gives the value a version of its own.
	 *
	 * Read only from values in a column family that opted into the verification table. Do not call
	 * `populateVersion()` for a marked version: that explicit primitive never sees the value and
	 * cannot enforce this flag.
	 */
	VERSION_NOT_UNIQUE_FLAG: number;
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
/**
 * The friendly names of every compression algorithm compiled into the loaded
 * RocksDB native binding, e.g. `['none', 'snappy', 'lz4', 'zstd']`. The set is
 * fixed for a given binary (it depends on which compression libraries RocksDB
 * was linked against), so this is a static list. `'none'` (no compression) is
 * always present. Use it to validate a `compression` option or to pick an
 * algorithm that is actually available at runtime.
 */
export const supportedCompression: readonly string[] = Object.freeze(
	binding.supportedCompression as string[]
);
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
 * Test-only: force the next `count` transaction commits to fail with TryAgain (rolled back, so
 * no data is committed), reproducing a stranded-snapshot conflict deterministically. Pass 0 to
 * disarm. Used by the ERR_TRY_AGAIN retry regression test.
 */
export const forceTryAgainForTesting: (count: number) => void = binding.forceTryAgainForTesting;

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
	options?: {
		backupId?: number;
		keepLogFiles?: boolean;
		mode?: RestoreOptions['mode'];
	}
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
