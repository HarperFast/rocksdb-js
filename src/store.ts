import { type BackupStreamOptions, backupToStream } from './backup-stream.ts';
import { assertBackupDirOutsideDatabase, type BackupOptions } from './backup.ts';
import { DBIterator, type DBIteratorValue } from './dbi-iterator.ts';
import type {
	CountEstimate,
	CountEstimateOptions,
	DBITransactional,
	IteratorOptions,
	RangeOptions,
} from './dbi.ts';
import {
	type BufferWithDataView,
	createFixedBuffer,
	type Encoder,
	type Encoding,
	initKeyEncoder,
	type Key,
	type KeyEncoding,
	type ReadKeyFunction,
	type WriteKeyFunction,
} from './encoding.ts';
import {
	constants,
	NativeDatabase,
	type NativeDatabaseOptions,
	NativeIterator,
	NativeTransaction,
	stats,
	supportedCompression,
	type TransactionLog,
	type UserSharedBufferCallback,
} from './load-binding.ts';
import { parseDuration } from './util.ts';
import { ExtendedIterable } from '@harperfast/extended-iterable';

const {
	ONLY_IF_IN_MEMORY_CACHE_FLAG,
	NOT_IN_MEMORY_CACHE_FLAG,
	ALWAYS_CREATE_NEW_BUFFER_FLAG,
	FRESH_VERSION_FLAG,
	POPULATE_VERSION_FLAG,
	ITERATOR_REVERSE_FLAG,
	ITERATOR_INCLUSIVE_END_FLAG,
	ITERATOR_EXCLUSIVE_START_FLAG,
	ITERATOR_INCLUDE_VALUES_FLAG,
	ITERATOR_NEEDS_STABLE_VALUE_BUFFER_FLAG,
	ITERATOR_CONTEXT_IS_TRANSACTION_FLAG,
	ITERATOR_HAS_TRANSACTION_ID_FLAG,
} = constants;
const KEY_BUFFER_SIZE = 4096;

export const KEY_BUFFER: BufferWithDataView = createFixedBuffer(KEY_BUFFER_SIZE);
export const VALUE_BUFFER: BufferWithDataView = createFixedBuffer(64 * 1024);

/**
 * Backing buffer for the shared iterator state. Layout (Uint32Array view):
 *   [0] = key length written into KEY_BUFFER by the most recent iterator step
 *   [1] = value length written into VALUE_BUFFER by the most recent step
 */
export const ITERATOR_STATE_BUFFER: Buffer = Buffer.allocUnsafeSlow(8);
export const ITERATOR_STATE: Uint32Array = new Uint32Array(
	ITERATOR_STATE_BUFFER.buffer,
	ITERATOR_STATE_BUFFER.byteOffset,
	2
);

const MAX_KEY_SIZE = 1024 * 1024; // 1MB
const RESET_BUFFER_MODE = 1024;
const REUSE_BUFFER_MODE = 512;
const SAVE_BUFFER_SIZE = 8192;
// const WRITE_BUFFER_SIZE = 65536;

export type StoreContext = NativeDatabase | NativeTransaction;
export type StoreGetOptions = GetOptions & DBITransactional;
export type StoreIteratorOptions = IteratorOptions & DBITransactional;
export type StorePutOptions = PutOptions & DBITransactional;
export type StoreRangeOptions = RangeOptions & DBITransactional;
export type StoreRemoveOptions = DBITransactional | unknown;

export type CompactOptions = {
	start?: Key;
	end?: Key;
	/**
	 * Compact the bottommost level as well, rewriting every file in range.
	 *
	 * RocksDB skips the bottommost level by default when no compaction filter is installed, and
	 * that is where most of the data sits — so an ordinary `compact()` leaves it untouched. Since a
	 * changed `compression` algorithm only governs newly written files, rewriting that level is the
	 * only way to re-encode data that already exists.
	 *
	 * This rewrites the whole range regardless of whether RocksDB considers it worth doing, so it
	 * is as expensive as the data is large. Defaults to `false`.
	 */
	bottommost?: boolean;
};

/**
 * A RocksDB block/blob compression algorithm. Which algorithms are actually
 * available depends on the libraries the native binding was compiled with —
 * check `supportedCompression` at runtime. `'none'` disables compression and
 * is always available.
 */
export type CompressionAlgorithm =
	| 'none'
	| 'snappy'
	| 'zlib'
	| 'bzip2'
	| 'lz4'
	| 'lz4hc'
	| 'xpress'
	| 'zstd';

/**
 * The `compression` open option: either an algorithm name, or an object with
 * an algorithm and an optional level (forwarded to RocksDB's
 * `compression_opts.level`; meaning is algorithm-specific).
 */
export type CompressionOption =
	| CompressionAlgorithm
	| { algorithm: CompressionAlgorithm; level?: number };

/**
 * The compression currently in effect for a column family, as returned by the
 * `compression` getter. `level` is present only when a non-default compression
 * level is configured.
 */
export type CompressionInfo = {
	algorithm: CompressionAlgorithm;
	level?: number;
};

/**
 * The informational-log settings currently in effect for a database, as
 * returned by the `logOptions` getter. These are database-wide (`DBOptions`)
 * settings, not per-column-family.
 */
export type LogOptions = {
	/**
	 * Per-file size cap (bytes) for informational log files (`LOG` /
	 * `LOG.old.*`).
	 */
	maxLogFileSize: number;
	/**
	 * Verbosity of informational logging: `0` (debug), `1` (info), `2` (warn),
	 * `3` (error), `4` (fatal), or `5` (header-only).
	 */
	infoLogLevel: number;
};

/**
 * Normalizes the public `compression` option into the primitive fields the
 * native layer expects. When `option` is omitted, or is an object without an
 * `algorithm`, returns an empty object and lets the native layer apply the
 * default (LZ4 when the build supports it, else RocksDB's own default).
 * Throws a `TypeError` for a malformed option and an
 * `Error` for an algorithm that isn't supported by this build.
 */
export function normalizeCompression(option: CompressionOption | undefined): {
	compression?: string;
	compressionLevel?: number;
} {
	// When unset, leave compression to the native layer, which defaults to LZ4
	// when the build supports it (see Database::Open).
	if (option === undefined || option === null) {
		return {};
	}

	let algorithm: string;
	let level: number | undefined;

	if (typeof option === 'string') {
		algorithm = option;
	} else if (typeof option === 'object' && !Array.isArray(option)) {
		if (option.algorithm === undefined || option.algorithm === null) {
			if (option.level !== undefined && option.level !== null) {
				throw new TypeError('Compression level cannot be specified without an algorithm');
			}
			return {};
		}
		algorithm = option.algorithm;
		if (option.level !== undefined && option.level !== null) {
			// TypeScript types `level` as a number. Accept a numeric string too (a
			// common shape from loosely-typed config), but do NOT let bare `Number()`
			// coerce other loose inputs — `true`, `[]`, `[6]`, and `''`/whitespace all
			// become numbers and would silently mis-tune or drop compression. Anything
			// that is not a number or a non-blank numeric string is rejected.
			const raw = option.level as unknown;
			const coerced =
				typeof raw === 'number'
					? raw
					: typeof raw === 'string' && raw.trim() !== ''
						? Number(raw)
						: Number.NaN;
			if (!Number.isInteger(coerced) || coerced < -2147483648 || coerced > 2147483647) {
				throw new TypeError(
					`compression level must be a 32-bit integer, got ${JSON.stringify(option.level)}`
				);
			}
			level = coerced;
		}
	} else {
		throw new TypeError('compression must be an algorithm name or an { algorithm, level } object');
	}

	if (!supportedCompression.includes(algorithm)) {
		throw new Error(
			`Unsupported compression algorithm "${algorithm}". This build supports: ${supportedCompression.join(', ')}`
		);
	}

	return { compression: algorithm, compressionLevel: level };
}

/**
 * Options for the `Store` class.
 */
export interface StoreOptions extends Omit<
	NativeDatabaseOptions,
	'compression' | 'compressionLevel' | 'mode' | 'transactionLogRetentionMs'
> {
	/**
	 * The block/blob compression algorithm for this column family. Accepts an
	 * algorithm name (`'lz4'`, `'zstd'`, `'none'`, ...) or an object with an
	 * `algorithm` and optional `level`. Applies to both SST blocks and blob
	 * files (large values). Which algorithms are available depends on the native
	 * build — see `supportedCompression`.
	 *
	 * When omitted, defaults to `lz4` if the build supports it, otherwise
	 * RocksDB's own default (Snappy when linked, else no compression).
	 * Compression is a dynamically-changeable option: reopening with a
	 * different algorithm governs subsequently written files; existing files
	 * keep their compression until rewritten by compaction. Use the
	 * `compression` getter to read the value currently in effect.
	 *
	 * @default 'lz4'
	 */
	compression?: CompressionOption;

	/**
	 * Apply `compression` to every column family in the database, not only the one being opened.
	 *
	 * RocksDB opens all of a database's column families in a single native call, and by default
	 * each of the ones you did not name keeps its persisted algorithm. That is right when codecs
	 * are chosen per table, but it leaves a caller that wants one codec for the whole database
	 * unable to say so: those families are already open at their old algorithm before it gets a
	 * chance to ask, and a column family's compression cannot be changed while it is open.
	 *
	 * Setting this applies the requested algorithm to all of them instead — including families
	 * that were persisted with a different one, which is the point when adopting a codec on a
	 * database that predates it.
	 *
	 * Only meaningful with an explicit `compression` (it throws otherwise), and only on the open
	 * that actually creates the database handle; later opens of the same path reuse that handle
	 * and cannot revisit the decision. As always, the algorithm governs newly written files —
	 * existing SST/blob files keep theirs until rewritten (see `compact({ bottommost: true })`).
	 *
	 * @default false
	 */
	compressionForAllColumnFamilies?: boolean;

	decoder?: Encoder | null;
	encoder?: Encoder | null;
	encoding?: Encoding;
	freezeData?: boolean;
	keyEncoder?: { readKey?: ReadKeyFunction<Key>; writeKey?: WriteKeyFunction };
	keyEncoding?: KeyEncoding;
	// mapSize?: number;
	// maxDbs?: number;
	// maxFreeSpaceToLoad?: number;
	// maxFreeSpaceToRetain?: number;
	// maxReaders?: number;
	maxKeySize?: number;
	// noReadAhead?: boolean;
	// noSync?: boolean;
	// overlappingSync?: boolean;
	// pageSize?: number;
	pessimistic?: boolean;

	/**
	 * If `true`, the encoder will use a random access structure.
	 */
	randomAccessStructure?: boolean;

	/**
	 * When `true`, the database is opened in read-only mode. Write operations
	 * will throw an error with code `ERR_DATABASE_READONLY`, except for
	 * `flush()`/`flushSync()`/`compact()`/`compactSync()`, which succeed as
	 * no-ops.
	 */
	readOnly?: boolean;

	sharedStructuresKey?: symbol;

	/**
	 * A string containing the amount of time or the number of milliseconds to
	 * retain transaction logs before purging.
	 *
	 * @default '3d' (3 days)
	 */
	transactionLogRetention?: number | string;

	// trackMetrics?: boolean;
}

/**
 * Options for the `getUserSharedBuffer()` method.
 */
export type UserSharedBufferOptions = { callback?: UserSharedBufferCallback };

/**
 * The return type of `getUserSharedBuffer()`.
 */
export type ArrayBufferWithNotify = ArrayBuffer & {
	cancel: () => void;
	notify: () => void;
};

/**
 * Whether `codec` is a pre-constructed encoder/decoder instance that `open()`
 * would reuse as-is (rather than instantiate fresh from an `Encoder` factory).
 * Such an instance carries per-column-family mutable state (its `name`, msgpack
 * `structures`), so it cannot be shared across column families.
 */
function sharesMutableCodec(codec: Encoder | null | undefined): boolean {
	return (
		codec != null &&
		typeof codec === 'object' &&
		typeof (codec as { Encoder?: unknown }).Encoder !== 'function' &&
		(typeof codec.encode === 'function' || typeof codec.decode === 'function')
	);
}

/**
 * A store wraps the `NativeDatabase` binding and database settings so that a
 * single database instance can be shared between the main `RocksDatabase`
 * instance and the `Transaction` instance.
 *
 * A store is bound to a single `RocksDatabase` instance and must not be shared
 * between `RocksDatabase` instances. Each `RocksDatabase` claims its store on
 * construction (see `claim()`); passing a store that another `RocksDatabase`
 * already owns throws. (Sharing with the owning database's `Transaction`
 * instances is expected and allowed.)
 */
export class Store {
	/**
	 * Whether a `RocksDatabase` has claimed this store. Durable across
	 * close/reopen (unlike the handle's open state), so a store reused after its
	 * owner temporarily closed is still recognized as in-use. See `claim()`.
	 */
	#claimed: boolean = false;

	/**
	 * Shallow snapshot of the options this store was constructed with, used by
	 * `createColumnFamilyStore()` to derive sibling column-family stores. Snapshot
	 * (not the caller's object) so later mutations of the caller's options don't
	 * leak into derived views.
	 */
	#columnFamilyOptions?: StoreOptions;

	/**
	 * The database instance.
	 */
	db: NativeDatabase;

	/**
	 * The decoder instance. This is commonly the same as the `encoder`
	 * instance.
	 */
	decoder: Encoder | null;

	/**
	 * Whether the decoder copies the buffer when encoding values.
	 */
	decoderCopies: boolean = false;

	/**
	 * The compression algorithm (or `{ algorithm, level }`) requested for this
	 * store's column family. Normalized and applied when the database opens.
	 */
	compression?: CompressionOption;

	/**
	 * Whether `compression` applies to every column family in the database rather than just this
	 * store's. See `RocksDatabaseOptions.compressionForAllColumnFamilies`.
	 */
	compressionForAllColumnFamilies: boolean = false;

	/**
	 * Whether to disable the write ahead log.
	 */
	disableWAL: boolean;

	/**
	 * Whether to enable RocksDB statistics.
	 */
	enableStats: boolean;

	/**
	 * Reusable buffer for encoding values using `writeKey()` when the custom
	 * encoder does not provide a `encode()` method.
	 */
	encodeBuffer: BufferWithDataView;

	/**
	 * The encoder instance.
	 */
	encoder: Encoder | null;

	/**
	 * The encoding used to encode values. Defaults to `'msgpack'` in
	 * `RocksDatabase.open()`.
	 */
	encoding: Encoding | null;

	/**
	 * Encoder specific option used to signal that the data should be frozen.
	 */
	freezeData: boolean;

	/**
	 * Verbosity of RocksDB's informational logging (`info_log_level`). Omit to
	 * leave RocksDB's own default.
	 */
	infoLogLevel?: number;

	/**
	 * Reusable buffer for encoding keys.
	 */
	keyBuffer: BufferWithDataView;

	/**
	 * The key encoding to use for keys. Defaults to `'ordered-binary'`.
	 */
	keyEncoding: KeyEncoding;

	/**
	 * The maximum key size.
	 */
	maxKeySize: number;

	/**
	 * Per-file size cap (bytes) for informational log files (`LOG` /
	 * `LOG.old.*`). Bounds total informational-log footprint to roughly `5 *
	 * maxLogFileSize` (RocksDB retains up to 5 of these files). A value of `0`
	 * is RocksDB's "single unbounded log file" mode: it disables size-based
	 * rotation, so the log can grow without limit — set it only if you want that.
	 *
	 * @default 16777216 (16MB)
	 */
	maxLogFileSize?: number;

	/**
	 * The maximum number of table files RocksDB keeps open. `0` (the default)
	 * derives a budget from the effective per-process open-file limit (an
	 * eighth of the limit — several databases can share one process — clamped
	 * to `[1024, 262144]`); `-1` holds every table file open (the RocksDB
	 * default, which can exhaust the process file-descriptor limit when
	 * compaction falls behind under sustained ingest); a positive `int32` is
	 * an explicit cap.
	 */
	maxOpenFiles?: number;

	/**
	 * The maximum number of memtables that can be queued per column family
	 * before writes stall. Higher values absorb write bursts while flushes catch
	 * up, at the cost of memory.
	 */
	maxWriteBufferNumber?: number;

	/**
	 * The bytes of recent memtable history to retain in memory for transaction
	 * conflict checking. `-1` derives the value from
	 * `maxWriteBufferNumber * writeBufferSize`, except under a stalling
	 * `writeBufferManager`, where it derives `0`: retained history is charged to
	 * that manager and is only trimmable down to this target, so a target the
	 * budget cannot hold stalls writes permanently. Set a number to size it
	 * yourself against the budget and the column-family count.
	 */
	maxWriteBufferSizeToMaintain?: number;

	/**
	 * The total memtable budget in bytes across all column families. When the
	 * sum of memtables reaches this size, RocksDB flushes the largest one.
	 * Defaults to `0`, which disables the global trigger so per-CF
	 * `writeBufferSize` drives flushing.
	 *
	 * Set this only if you know your column-family count: it is one budget split
	 * across all of them, so a value that suits a few families starves many. To
	 * bound total memtable memory instead, configure a WriteBufferManager.
	 *
	 * Database-wide, so it binds when the path is first opened in this process:
	 * a later open of the same path — including from another worker thread —
	 * keeps the first opener's value rather than overriding or rejecting it.
	 */
	dbWriteBufferSize?: number;

	/**
	 * The name of the store (e.g. the column family). Defaults to `'default'`.
	 */
	name: string;

	/**
	 * Whether to disable the block cache.
	 */
	noBlockCache?: boolean;

	/**
	 * The number of threads to use for parallel operations. This is a RocksDB
	 * option. When undefined, the native layer picks
	 * `max(1, hardware_concurrency() / 2)`.
	 */
	parallelismThreads?: number;

	/**
	 * The path to the database.
	 */
	path: string;

	/**
	 * Whether to use pessimistic locking for transactions. When `true`,
	 * transactions will fail as soon as a conflict is detected. When `false`,
	 * transactions will only fail when `commit()` is called.
	 */
	pessimistic: boolean;

	/**
	 * Whether the database is open in readonly mode. When `true`, write
	 * operations will throw an error with code `ERR_DATABASE_READONLY`, except
	 * for `flush()`/`flushSync()`/`compact()`/`compactSync()`, which succeed as
	 * no-ops.
	 */
	readOnly: boolean;

	/**
	 * Encoder specific flag used to signal that the encoder should use a random
	 * access structure.
	 */
	randomAccessStructure: boolean;

	/**
	 * The function used to encode keys.
	 */
	readKey: ReadKeyFunction<Key>;

	/**
	 * The key used to store shared structures.
	 */
	sharedStructuresKey?: symbol;

	/**
	 * The level of statistics to capture.
	 */
	statsLevel?: (typeof stats.StatsLevel)[keyof typeof stats.StatsLevel];

	/**
	 * The threshold for the transaction log file's last modified time to be
	 * older than the retention period before it is rotated to the next sequence
	 * number. A threshold of 0 means ignore age check.
	 */
	transactionLogMaxAgeThreshold?: number;

	/**
	 * The target maximum size of a transaction log before rotation. Transactions
	 * are never split across files, so one transaction may exceed this target.
	 */
	transactionLogMaxSize?: number;

	/**
	 * A string containing the amount of time or the number of milliseconds to
	 * retain transaction logs before purging.
	 *
	 * @default '3d' (3 days)
	 */
	transactionLogRetention?: number | string;

	/**
	 * The path to the transaction logs directory.
	 */
	transactionLogsPath?: string;

	/**
	 * Whether this store's column family participates in the VerificationTable.
	 */
	verificationTable?: boolean;

	/**
	 * The per-column-family memtable size in bytes at which the memtable is
	 * sealed and flushed.
	 */
	writeBufferSize?: number;

	/**
	 * The function used to encode keys using the shared `keyBuffer`.
	 */
	writeKey: WriteKeyFunction;

	/**
	 * Initializes the store with a new `NativeDatabase` instance.
	 *
	 * @param path - The path to the database.
	 * @param options - The options for the store.
	 */
	constructor(path: string, options?: StoreOptions) {
		if (!path || typeof path !== 'string') {
			throw new TypeError('Invalid database path');
		}

		if (options !== undefined && options !== null && typeof options !== 'object') {
			throw new TypeError('Database options must be an object');
		}

		const { keyEncoding, readKey, writeKey } = initKeyEncoder(
			options?.keyEncoding,
			options?.keyEncoder
		);

		this.db = new NativeDatabase();
		this.compression = options?.compression;
		this.compressionForAllColumnFamilies = options?.compressionForAllColumnFamilies === true;
		this.dbWriteBufferSize = options?.dbWriteBufferSize;
		this.decoder = options?.decoder ?? null;
		this.disableWAL = options?.disableWAL ?? false;
		this.enableStats = options?.enableStats ?? false;
		this.encodeBuffer = createFixedBuffer(SAVE_BUFFER_SIZE);
		this.encoder = options?.encoder ?? null;
		this.encoding = options?.encoding ?? null;
		this.freezeData = options?.freezeData ?? false;
		this.infoLogLevel = options?.infoLogLevel;
		this.keyBuffer = KEY_BUFFER;
		this.keyEncoding = keyEncoding;
		this.maxKeySize = options?.maxKeySize ?? MAX_KEY_SIZE;
		this.maxLogFileSize = options?.maxLogFileSize;
		this.maxOpenFiles = options?.maxOpenFiles;
		this.maxWriteBufferNumber = options?.maxWriteBufferNumber;
		this.maxWriteBufferSizeToMaintain = options?.maxWriteBufferSizeToMaintain;
		this.name = options?.name ?? 'default';
		this.noBlockCache = options?.noBlockCache;
		this.parallelismThreads = options?.parallelismThreads;
		this.path = path;
		this.pessimistic = options?.pessimistic ?? false;
		this.readOnly = options?.readOnly ?? false;
		this.randomAccessStructure = options?.randomAccessStructure ?? false;
		this.readKey = readKey;
		this.sharedStructuresKey = options?.sharedStructuresKey;
		this.statsLevel = options?.statsLevel;
		this.transactionLogMaxAgeThreshold = options?.transactionLogMaxAgeThreshold;
		this.transactionLogMaxSize = options?.transactionLogMaxSize;
		this.transactionLogRetention = options?.transactionLogRetention;
		this.transactionLogsPath = options?.transactionLogsPath;
		this.verificationTable = options?.verificationTable;
		this.writeBufferSize = options?.writeBufferSize;
		this.writeKey = writeKey;
		this.#columnFamilyOptions = options ? { ...options } : undefined;
	}

	/**
	 * Creates an independent `Store` for another column family of the same
	 * database, used by {@link RocksDatabase#use}. The returned store shares the
	 * underlying database but must have its own codec state: a mutable
	 * encoder/decoder instance cannot be shared across column families (opening a
	 * view mutates the encoder's `name`, and clearing a view resets its
	 * `structures`), so a pre-constructed instance is rejected here — supply an
	 * encoder factory (`{ Encoder }`) or a named `encoding` instead.
	 *
	 * Override this in a `Store` subclass whose constructor cannot be recreated
	 * from `(path, options)` (e.g. it takes injected dependencies) or that needs
	 * to control how a view's codec state is built.
	 *
	 * @param name - The column family name for the derived store.
	 * @param options - Options overriding this store's, for the derived store.
	 * @returns A new, independent `Store` bound to `name`.
	 */
	createColumnFamilyStore(name: string, options?: StoreOptions): Store {
		const derived: StoreOptions = { ...this.#columnFamilyOptions, ...options, name };

		if (sharesMutableCodec(derived.encoder) || sharesMutableCodec(derived.decoder)) {
			throw new Error(
				'Cannot derive a column family view from a Store configured with a ' +
					'pre-constructed encoder/decoder instance — its mutable state would be ' +
					'shared across column families. Provide an encoder factory (`{ Encoder }`) ' +
					'or a named `encoding`, or override `Store#createColumnFamilyStore`.'
			);
		}

		const StoreClass = this.constructor as new (path: string, options?: StoreOptions) => Store;
		const store = new StoreClass(this.path, derived);
		if (store.name !== name) {
			throw new Error(
				`Store#createColumnFamilyStore produced a store bound to "${store.name}" instead ` +
					`of "${name}"; a custom Store must forward options to super() or override this method.`
			);
		}
		return store;
	}

	/**
	 * Closes the database.
	 */
	close(): void {
		this.db.close();
	}

	/**
	 * Claims this store for a `RocksDatabase`. A store is bound to a single
	 * database instance, so claiming a store that another `RocksDatabase` has
	 * already claimed throws. The claim is durable across close/reopen, so a
	 * store reused after its owner temporarily closed still throws.
	 */
	claim(): void {
		if (this.#claimed) {
			throw new Error('Store is already in use by another RocksDatabase instance');
		}
		this.#claimed = true;
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
		let startBuffer: Buffer | undefined;
		let endBuffer: Buffer | undefined;

		if (options?.start !== undefined) {
			const start = this.encodeKey(options.start);
			startBuffer = Buffer.from(start.subarray(start.start, start.end));
		}
		if (options?.end !== undefined) {
			const end = this.encodeKey(options.end);
			endBuffer = Buffer.from(end.subarray(end.start, end.end));
		}

		return new Promise((resolve, reject) =>
			this.db.compact(resolve, reject, startBuffer, endBuffer, options?.bottommost === true)
		);
	}

	/**
	 * Creates a backup of the entire database (all column families) into the
	 * given directory and resolves with the new backup id. Parent directories
	 * are created as needed. See `backups` for restore and management.
	 *
	 * @example
	 * ```typescript
	 * const id = await db.backup('/path/to/backups');
	 * ```
	 */
	async backup(backupDir: string, options?: BackupOptions): Promise<number>;
	async backup(stream: WritableStream<Uint8Array>, options?: BackupStreamOptions): Promise<void>;
	async backup(
		target: string | WritableStream<Uint8Array>,
		options?: BackupOptions | BackupStreamOptions
	): Promise<number | void> {
		// Duck-type rather than `instanceof WritableStream` so a stream from a
		// different realm (or a polyfill) still routes to the streaming path.
		if (typeof target !== 'string' && typeof target?.getWriter === 'function') {
			return backupToStream(this.db, target, options as BackupStreamOptions);
		}
		// Refuse to write backup files into the live database directory (or a
		// subdirectory of it), which would clobber RocksDB's own files.
		assertBackupDirOutsideDatabase(this.path, target as string);
		// The native side creates the directory (with missing parents) and holds
		// the on-disk single-writer lock for the duration of the backup — see
		// `runCreateBackup` in `src/binding/database/backup.cpp` and
		// `withBackupDirLock`. Rejects if the directory is already locked.
		return new Promise((resolve, reject) =>
			this.db.backup(resolve, reject, target as string, options as BackupOptions)
		);
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
		let startBuffer: Buffer | undefined;
		let endBuffer: Buffer | undefined;

		if (options?.start !== undefined) {
			const start = this.encodeKey(options.start);
			startBuffer = Buffer.from(start.subarray(start.start, start.end));
		}
		if (options?.end !== undefined) {
			const end = this.encodeKey(options.end);
			endBuffer = Buffer.from(end.subarray(end.start, end.end));
		}

		this.db.compactSync(startBuffer, endBuffer, options?.bottommost === true);
	}

	/**
	 * Decodes a key from the database.
	 *
	 * @param key - The key to decode.
	 * @returns The decoded key.
	 */
	decodeKey(key: Buffer): Key {
		return this.readKey(key as BufferWithDataView, 0, key.length);
	}

	/**
	 * Decodes a value from the database.
	 *
	 * @param value - The value to decode.
	 * @returns The decoded value.
	 */
	decodeValue(value: BufferWithDataView): any {
		if (value?.length > 0 && typeof this.decoder?.decode === 'function') {
			return this.decoder.decode(value, { end: value.end });
		}
		return value;
	}

	/**
	 * Encodes a key for the database.
	 *
	 * @param key - The key to encode.
	 * @returns The encoded key.
	 */
	encodeKey(key: Key): BufferWithDataView {
		if (key === undefined) {
			throw new Error('Key is required');
		}

		const bytesWritten = this.writeKey(key, this.keyBuffer, 0);
		if (bytesWritten === 0) {
			throw new Error('Zero length key is not allowed');
		}

		this.keyBuffer.end = bytesWritten;

		return this.keyBuffer;
	}

	/**
	 * Encodes a value for the database.
	 *
	 * @param value - The value to encode.
	 * @returns The encoded value.
	 */
	encodeValue(value: any): BufferWithDataView | Uint8Array {
		if (value && value['\x10binary-data\x02']) {
			return value['\x10binary-data\x02'];
		}

		if (typeof this.encoder?.encode === 'function') {
			if (this.encoder.copyBuffers) {
				return this.encoder.encode(value, REUSE_BUFFER_MODE | RESET_BUFFER_MODE);
			}

			const valueBuffer = this.encoder.encode(value);
			if (typeof valueBuffer === 'string') {
				return Buffer.from(valueBuffer);
			}
			return valueBuffer;
		}

		if (typeof value === 'string') {
			return Buffer.from(value);
		}

		if (value instanceof Uint8Array) {
			return value;
		}

		throw new Error(`Invalid value put in database (${typeof value}), consider using an encoder`);
	}

	get(
		context: StoreContext,
		key: Key,
		alwaysCreateNewBuffer: boolean = false,
		options?: StoreGetOptions
	): any | undefined {
		const keyParam = getKeyParam(this.encodeKey(key));
		let flags = 0;
		if (alwaysCreateNewBuffer) {
			// used by getBinary to force a new safe long-lived buffer
			flags |= ALWAYS_CREATE_NEW_BUFFER_FLAG;
		}
		if (options?.populateVersion) {
			flags |= POPULATE_VERSION_FLAG;
		}
		const txnId = this.getTxnId(options);
		const expectedVersion = options?.expectedVersion;
		// getSync is the fast path, which can return immediately if the entry is in memory cache, but we want to fail otherwise
		const result = context.getSync(
			keyParam,
			flags | ONLY_IF_IN_MEMORY_CACHE_FLAG,
			txnId,
			expectedVersion
		);
		if (typeof result === 'number') {
			// return a number indicates it is using the default buffer
			if (result === NOT_IN_MEMORY_CACHE_FLAG) {
				// is not in memory cache, use async get since this will involve disk access
				return new Promise((resolve, reject) => {
					// We still use the same shared buffer for the key, the native side will make a copy for the async task
					context.get(keyParam, resolve, reject, txnId, expectedVersion);
				});
			}
			if (result === FRESH_VERSION_FLAG) {
				return result;
			}
			// continue with fast path
			VALUE_BUFFER.end = result;
			return VALUE_BUFFER;
		} // else it is undefined or it is a new buffer
		return result;
	}

	/**
	 * Estimates the number of keys in a range from RocksDB statistics
	 * (memtable stats + approximate SST sizes with range-local entry density)
	 * without iterating. Estimates always reflect committed state, so there is
	 * no transactional variant.
	 */
	estimateCount(options?: CountEstimateOptions): CountEstimate {
		let startBuffer: Buffer | undefined;
		let endBuffer: Buffer | undefined;
		const reverse = options?.reverse ?? false;
		const start = reverse ? options?.end : options?.start;
		const end = reverse ? options?.start : options?.end;
		const exclusiveStart = options?.exclusiveStart ?? reverse;
		const inclusiveEnd = options?.inclusiveEnd ?? reverse;

		if (start !== undefined) {
			const encodedStart = this.encodeKey(start);
			// A zero byte appended to a key is its bytewise successor, turning an
			// inclusive bound into an exclusive one (and vice versa for the end).
			startBuffer = copyEncodedKey(encodedStart, exclusiveStart);
		}

		if (end !== undefined) {
			const encodedEnd = this.encodeKey(end);
			endBuffer = copyEncodedKey(encodedEnd, inclusiveEnd);
		}

		return this.db.estimateCount(startBuffer, endBuffer);
	}

	getCount(context: StoreContext, options?: StoreRangeOptions): number {
		options = { ...options };

		if (options?.start !== undefined) {
			const start = this.encodeKey(options.start);
			options.start = Buffer.from(start.subarray(start.start, start.end));
		}

		if (options?.end !== undefined) {
			const end = this.encodeKey(options.end);
			options.end = Buffer.from(end.subarray(end.start, end.end));
		}

		return context.getCount(options, this.getTxnId(options));
	}

	getKeys(context: StoreContext, options?: StoreIteratorOptions): any | undefined {
		return this.getRange(context, { ...options, values: false }).map((item) => item.key);
	}

	getKeysCount(context: StoreContext, options?: StoreRangeOptions): number {
		return this.getCount(context, options);
	}

	getRange(
		context: StoreContext,
		options?: StoreIteratorOptions
	): ExtendedIterable<DBIteratorValue<any>> {
		if (!this.db.opened) {
			throw new Error('Database not open');
		}

		// Resolve start/end keys, honoring the `key` shortcut for single-key
		// matches and swapping start/end when iterating in reverse.
		let startUnencoded = options?.key ?? options?.start;
		let endUnencoded = options?.key ?? options?.end;

		const includeValues = options?.values ?? true;
		const reverse = options?.reverse ?? false;
		const txnId = this.getTxnId(options);

		let exclusiveStart = options?.exclusiveStart ?? false;
		let inclusiveEnd = options?.inclusiveEnd ?? false;
		if (options?.key !== undefined) {
			inclusiveEnd = true;
		}

		if (reverse) {
			const tmp = startUnencoded;
			startUnencoded = endUnencoded;
			endUnencoded = tmp;
			// preserve previous default behavior: reverse iteration treats
			// missing exclusiveStart/inclusiveEnd as `true`
			exclusiveStart = options?.exclusiveStart ?? true;
			inclusiveEnd = options?.inclusiveEnd ?? true;
		}

		// Encode both keys back-to-back into the shared key buffer. Each key
		// is identified to the native side by its (start, end) offsets.
		const keyBuffer = this.keyBuffer;
		let startKeyEnd = 0;
		let endKeyStart = 0;
		let endKeyEnd = 0;

		if (startUnencoded !== undefined) {
			startKeyEnd = this.writeKey(startUnencoded, keyBuffer, 0);
			if (startKeyEnd === 0) {
				throw new Error('Zero length key is not allowed');
			}
		}

		if (endUnencoded !== undefined) {
			if (endUnencoded === startUnencoded) {
				// `key` shortcut: reuse the encoded start key bytes
				endKeyStart = 0;
				endKeyEnd = startKeyEnd;
			} else {
				endKeyStart = startKeyEnd;
				endKeyEnd = this.writeKey(endUnencoded, keyBuffer, endKeyStart);
				if (endKeyEnd === endKeyStart) {
					throw new Error('Zero length key is not allowed');
				}
			}
		}

		let flags = 0;
		if (reverse) flags |= ITERATOR_REVERSE_FLAG;
		if (exclusiveStart) flags |= ITERATOR_EXCLUSIVE_START_FLAG;
		if (inclusiveEnd) flags |= ITERATOR_INCLUSIVE_END_FLAG;
		if (includeValues) flags |= ITERATOR_INCLUDE_VALUES_FLAG;
		if (includeValues && !this.decoderCopies) {
			flags |= ITERATOR_NEEDS_STABLE_VALUE_BUFFER_FLAG;
		}
		// Tell the native constructor whether `context` is a Transaction (vs.
		// the database) so it can skip a napi_instanceof check. The store
		// holds the canonical NativeDatabase reference, so identity comparison
		// is sufficient.
		if (context !== this.db) {
			flags |= ITERATOR_CONTEXT_IS_TRANSACTION_FLAG;
		}
		if (txnId !== undefined) {
			flags |= ITERATOR_HAS_TRANSACTION_ID_FLAG;
		}

		// Only pass the advanced ReadOptions object on the rare path where any
		// of the underlying RocksDB iterator options are actually overridden.
		const advancedOptions =
			options !== undefined &&
			(options.adaptiveReadahead !== undefined ||
				options.asyncIO !== undefined ||
				options.autoReadaheadSize !== undefined ||
				options.backgroundPurgeOnIteratorCleanup !== undefined ||
				options.fillCache !== undefined ||
				options.readaheadSize !== undefined ||
				options.tailing !== undefined)
				? options
				: undefined;

		return new ExtendedIterable(
			// @ts-expect-error ExtendedIterable v1 constructor type definition is incorrect
			new DBIterator(
				new NativeIterator(
					context,
					flags,
					startKeyEnd,
					endKeyStart,
					endKeyEnd,
					advancedOptions,
					txnId
				),
				this,
				includeValues,
				options?.limit
			)
		);
	}

	getSync(
		context: StoreContext,
		key: Key,
		alwaysCreateNewBuffer: boolean = false,
		options?: StoreGetOptions
	): any | undefined {
		const keyParam = getKeyParam(this.encodeKey(key));
		let flags = 0;
		if (alwaysCreateNewBuffer) {
			flags |= ALWAYS_CREATE_NEW_BUFFER_FLAG;
		}
		if (options?.populateVersion) {
			flags |= POPULATE_VERSION_FLAG;
		}
		// we are using the shared buffer for keys, so we just pass in the key ending point (much faster than passing in a buffer)
		const result = context.getSync(
			keyParam,
			flags,
			this.getTxnId(options),
			options?.expectedVersion
		);
		if (typeof result === 'number') {
			if (result === FRESH_VERSION_FLAG) {
				return result;
			}
			// return a number indicates it is using the default buffer
			VALUE_BUFFER.end = result;
			return VALUE_BUFFER;
		} // else it is undefined or it is a new buffer
		return result;
	}

	/**
	 * Checks if the data method options object contains a transaction ID and
	 * returns it.
	 */
	getTxnId(options?: DBITransactional | unknown): number | undefined {
		let txnId: number | undefined;
		const transaction = (options as DBITransactional)?.transaction;
		if (!this.readOnly && transaction) {
			txnId = transaction.id;
			if (txnId === undefined) {
				throw new TypeError('Invalid transaction');
			}
			// ids are per database; column families of one database share its path
			if (transaction.store !== undefined && transaction.store.path !== this.path) {
				throw new TypeError('Transaction belongs to a different database');
			}
		}
		return txnId;
	}

	/**
	 * Gets or creates a buffer that can be shared across worker threads.
	 *
	 * @param key - The key to get or create the buffer for.
	 * @param defaultBuffer - The default buffer to copy and use if the buffer
	 * does not exist.
	 * @param [options] - The options for the buffer.
	 * @param [options.callback] - A optional callback is called when `notify()`
	 * on the returned buffer is called.
	 * @returns An `ArrayBuffer` that is internally backed by a rocksdb-js
	 * managed buffer. The buffer also has `notify()` and `cancel()` methods
	 * that can be used to notify the specified `options.callback`.
	 */
	getUserSharedBuffer(
		key: Key,
		defaultBuffer: ArrayBuffer,
		options?: UserSharedBufferOptions
	): ArrayBufferWithNotify {
		const encodedKey = this.encodeKey(key);

		if (options !== undefined && typeof options !== 'object') {
			throw new TypeError('Options must be an object');
		}

		const buffer = this.db.getUserSharedBuffer(
			encodedKey,
			defaultBuffer,
			options?.callback
		) as ArrayBufferWithNotify;

		// note: the notification methods need to re-encode the key because
		// encodeKey() uses a shared key buffer
		buffer.notify = (...args: any[]) => {
			return this.db.notify(this.encodeKey(key), args);
		};
		buffer.cancel = () => {
			if (options?.callback) {
				this.db.removeListener(this.encodeKey(key), options.callback);
			}
		};
		return buffer;
	}

	/**
	 * Checks if a lock exists.
	 * @param key The lock key.
	 * @returns `true` if the lock exists, `false` otherwise
	 */
	hasLock(key: Key): boolean {
		return this.db.hasLock(this.encodeKey(key));
	}

	/**
	 * Checks if the database is open.
	 *
	 * @returns `true` if the database is open, `false` otherwise.
	 */
	isOpen(): boolean {
		return this.db.opened;
	}

	/**
	 * Lists all transaction log names.
	 *
	 * @returns an array of transaction log names.
	 */
	listLogs(): string[] {
		return this.db.listLogs();
	}

	/**
	 * Opens the database. This must be called before any database operations
	 * are performed.
	 */
	open(): boolean {
		if (this.db.opened) {
			return true;
		}

		const { compression, compressionLevel } = normalizeCompression(this.compression);

		this.db.open(this.path, {
			compression,
			compressionLevel,
			compressionForAllColumnFamilies: this.compressionForAllColumnFamilies,
			dbWriteBufferSize: this.dbWriteBufferSize,
			disableWAL: this.disableWAL,
			enableStats: this.enableStats,
			infoLogLevel: this.infoLogLevel,
			maxLogFileSize: this.maxLogFileSize,
			maxOpenFiles: this.maxOpenFiles,
			maxWriteBufferNumber: this.maxWriteBufferNumber,
			maxWriteBufferSizeToMaintain: this.maxWriteBufferSizeToMaintain,
			mode: this.pessimistic ? 'pessimistic' : 'optimistic',
			name: this.name,
			noBlockCache: this.noBlockCache,
			parallelismThreads: this.parallelismThreads,
			readOnly: this.readOnly,
			statsLevel: this.statsLevel,
			transactionLogMaxAgeThreshold: this.transactionLogMaxAgeThreshold,
			transactionLogMaxSize: this.transactionLogMaxSize,
			transactionLogRetentionMs: this.transactionLogRetention
				? parseDuration(this.transactionLogRetention)
				: undefined,
			transactionLogsPath: this.transactionLogsPath,
			verificationTable: this.verificationTable,
			writeBufferSize: this.writeBufferSize,
		});

		return false;
	}

	putSync(context: StoreContext, key: Key, value: any, options?: StorePutOptions): void {
		if (!this.db.opened) {
			throw new Error('Database not open');
		}

		// IMPORTANT!
		// We MUST encode the value before the key because if the `sharedStructuresKey`
		// is set, it will be used by `getStructures()` and `saveStructures()` which in
		// turn will encode the `sharedStructuresKey` into the shared `keyBuffer`
		// overwriting this method's encoded key!
		const valueBuffer = this.encodeValue(value);

		context.putSync(this.encodeKey(key), valueBuffer, this.getTxnId(options));
	}

	removeSync(context: StoreContext, key: Key, options?: StoreRemoveOptions): void {
		if (!this.db.opened) {
			throw new Error('Database not open');
		}

		context.removeSync(this.encodeKey(key), this.getTxnId(options));
	}

	/**
	 * Attempts to acquire a lock for a given key. If the lock is available,
	 * the function returns `true` and the optional callback is never called.
	 * If the lock is not available, the function returns `false` and the
	 * callback is queued until the lock is released.
	 *
	 * @param key - The key to lock.
	 * @param onUnlocked - A callback to call when the lock is released.
	 * @returns `true` if the lock was acquired, `false` otherwise.
	 */
	tryLock(key: Key, onUnlocked?: () => void): boolean {
		if (onUnlocked !== undefined && typeof onUnlocked !== 'function') {
			throw new TypeError('Callback must be a function');
		}

		return this.db.tryLock(this.encodeKey(key), onUnlocked);
	}

	/**
	 * Releases the lock on the given key and calls any queued `onUnlocked`
	 * callback handlers.
	 *
	 * @param key - The key to unlock.
	 */
	unlock(key: Key): void {
		return this.db.unlock(this.encodeKey(key));
	}

	/**
	 * Gets or creates a transaction log instance.
	 *
	 * @param context - The context to use for the transaction log.
	 * @param name - The name of the transaction log.
	 * @returns The transaction log.
	 */
	useLog(context: StoreContext, name: string | number): TransactionLog {
		if (typeof name !== 'string' && typeof name !== 'number') {
			throw new TypeError('Log name must be a string or number');
		}
		if (typeof name === 'string' && /[\t\n\r\\/]/.test(name)) {
			throw new Error(`Invalid transaction log name "${name}"`);
		}
		// Pass the JS database per-call: the native transaction deliberately
		// holds no napi_ref to it (HarperFast/rocksdb-js#741); the database
		// context ignores the extra argument.
		return context.useLog(String(name), this.db);
	}

	/**
	 * Checks the process-global verification table for a fresh version match
	 * on `key`. Returns `true` when the table currently records `version` for
	 * this database+column-family. Provides a fast cache-freshness check
	 * before falling back to a full read.
	 */
	verifyVersion(key: Key, version: number): boolean {
		const keyParam = getKeyParam(this.encodeKey(key));
		return this.db.verifyVersion(keyParam, version);
	}

	/**
	 * Seeds the verification-table slot for `key` with `version`. Has no
	 * effect if the slot is currently lock-tagged or the table is disabled.
	 * Useful after a full read where the caller already knows the version.
	 */
	populateVersion(key: Key, version: number): void {
		const keyParam = getKeyParam(this.encodeKey(key));
		this.db.populateVersion(keyParam, version);
	}

	/**
	 * Acquires a lock on the given key and calls the callback.
	 *
	 * @param key - The key to lock.
	 * @param callback - The callback to call when the lock is acquired.
	 * @returns A promise that resolves when the lock is acquired.
	 */
	withLock(key: Key, callback: () => void | Promise<void>): Promise<void> {
		if (typeof callback !== 'function') {
			return Promise.reject(new TypeError('Callback must be a function'));
		}

		return this.db.withLock(this.encodeKey(key), callback);
	}
}

/**
 * Copies an encoded key out of the shared key buffer (which the next
 * `encodeKey` call would clobber), optionally appending a zero byte to
 * produce the key's bytewise successor.
 */
function copyEncodedKey(encoded: BufferWithDataView, appendSuccessorByte: boolean): Buffer {
	const length = encoded.end - encoded.start;
	const copy = Buffer.allocUnsafe(length + (appendSuccessorByte ? 1 : 0));
	copy.set(encoded.subarray(encoded.start, encoded.end));
	if (appendSuccessorByte) {
		copy[length] = 0;
	}
	return copy;
}

/**
 * Ensure that they key has been copied into our shared buffer, and return the ending position
 * @param keyBuffer
 */
function getKeyParam(keyBuffer: BufferWithDataView): number | Buffer {
	if (keyBuffer.buffer === KEY_BUFFER.buffer) {
		if (keyBuffer.end >= 0) {
			return keyBuffer.end;
		}
		if (keyBuffer.byteOffset === 0) {
			return keyBuffer.byteLength;
		}
	}
	if (keyBuffer.length > KEY_BUFFER.length) {
		// for larger key buffers, we pass it straight in
		return keyBuffer;
	}
	KEY_BUFFER.set(keyBuffer);
	return keyBuffer.length;
}

export interface GetOptions {
	/**
	 * When set, the native layer checks the verification table before reading.
	 * If the slot holds this version, returns `FRESH_VERSION_FLAG` immediately
	 * (the cached value is still valid). After a DB read, also seeds the slot
	 * with the version extracted from the value.
	 */
	expectedVersion?: number;

	/**
	 * When `true`, after a DB read the native layer automatically seeds the
	 * verification-table slot with the version extracted from the value.
	 * Eliminates the need for a separate `populateVersion` call on cold reads.
	 *
	 * @default false
	 */
	populateVersion?: boolean;

	/**
	 * Whether to skip decoding the value.
	 *
	 * @default false
	 */
	skipDecode?: boolean;
}

export interface PutOptions {
	append?: boolean;
	instructedWrite?: boolean;
	noDupData?: boolean;
	noOverwrite?: boolean;
}
