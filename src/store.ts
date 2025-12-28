import {
	NativeDatabase,
	NativeIterator,
	NativeTransaction,
	type UserSharedBufferCallback,
	type NativeDatabaseOptions,
	type TransactionLog,
	constants,
} from './load-binding.js';
import {
	Encoding,
	initKeyEncoder,
	createFixedBuffer,
	type BufferWithDataView,
	type Encoder,
	type Key,
	type KeyEncoding,
	type ReadKeyFunction,
	type WriteKeyFunction,
} from './encoding.js';
import type { DBITransactional, IteratorOptions, RangeOptions } from './dbi.js';
import { DBIterator, type DBIteratorValue } from './dbi-iterator.js';
import { Transaction } from './transaction.js';
import { ExtendedIterable } from '@harperfast/extended-iterable';
import { parseDuration } from './util.js';
const { ONLY_IF_IN_MEMORY_CACHE_FLAG, NOT_IN_MEMORY_CACHE_FLAG, ALWAYS_CREATE_BUFFER_FLAG } = constants;
export const KEY_BUFFER: BufferWithDataView = createFixedBuffer(16 * 1024);
export const VALUE_BUFFER: BufferWithDataView = createFixedBuffer(64 * 1024);

const KEY_BUFFER_SIZE = 4096;
const MAX_KEY_SIZE = 1024 * 1024; // 1MB
const RESET_BUFFER_MODE = 1024;
const REUSE_BUFFER_MODE = 512;
const SAVE_BUFFER_SIZE = 8192;
// const WRITE_BUFFER_SIZE = 65536;

export type Context = NativeDatabase | NativeTransaction;

/**
 * Options for the `Store` class.
 */
export interface StoreOptions extends Omit<NativeDatabaseOptions,
	| 'mode'
	| 'transactionLogRetentionMs'
> {
	decoder?: Encoder | null;
	encoder?: Encoder | null;
	encoding?: Encoding;
	freezeData?: boolean;
	keyEncoder?: {
		readKey?: ReadKeyFunction<Key>;
		writeKey?: WriteKeyFunction;
	};
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

	// readOnly?: boolean;

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
export type UserSharedBufferOptions = {
	callback?: UserSharedBufferCallback;
};

/**
 * The return type of `getUserSharedBuffer()`.
 */
export type ArrayBufferWithNotify = ArrayBuffer & {
	cancel: () => void;
	notify: () => void;
};

/**
 * A store wraps the `NativeDatabase` binding and database settings so that a
 * single database instance can be shared between the main `RocksDatabase`
 * instance and the `Transaction` instance.
 *
 * This store should not be shared between `RocksDatabase` instances.
 */
export class Store {
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
	 * Whether to disable the write ahead log.
	 */
	disableWAL: boolean;

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
	 * The name of the store (e.g. the column family). Defaults to `'default'`.
	 */
	name: string;

	/**
	 * Whether to disable the block cache.
	 */
	noBlockCache?: boolean;

	/**
	 * The number of threads to use for parallel operations. This is a RocksDB
	 * option.
	 */
	parallelismThreads: number;

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
	 * The threshold for the transaction log file's last modified time to be
	 * older than the retention period before it is rotated to the next sequence
	 * number. A threshold of 0 means ignore age check.
	 */
	transactionLogMaxAgeThreshold?: number;

	/**
	 * The maximum size of a transaction log before it is rotated to the next
	 * sequence number.
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
		this.decoder = options?.decoder ?? null;
		this.disableWAL = options?.disableWAL ?? false;
		this.encodeBuffer = createFixedBuffer(SAVE_BUFFER_SIZE);
		this.encoder = options?.encoder ?? null;
		this.encoding = options?.encoding ?? null;
		this.freezeData = options?.freezeData ?? false;
		this.keyBuffer = KEY_BUFFER;
		this.keyEncoding = keyEncoding;
		this.maxKeySize = options?.maxKeySize ?? MAX_KEY_SIZE;
		this.name = options?.name ?? 'default';
		this.noBlockCache = options?.noBlockCache;
		this.parallelismThreads = options?.parallelismThreads ?? 1;
		this.path = path;
		this.pessimistic = options?.pessimistic ?? false;
		this.randomAccessStructure = options?.randomAccessStructure ?? false;
		this.readKey = readKey;
		this.sharedStructuresKey = options?.sharedStructuresKey;
		this.transactionLogMaxAgeThreshold = options?.transactionLogMaxAgeThreshold;
		this.transactionLogMaxSize = options?.transactionLogMaxSize;
		this.transactionLogRetention = options?.transactionLogRetention;
		this.transactionLogsPath = options?.transactionLogsPath;
		this.writeKey = writeKey;
	}

	/**
	 * Closes the database.
	 */
	close(): void {
		this.db.close();
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
				return this.encoder.encode(
					value,
					REUSE_BUFFER_MODE | RESET_BUFFER_MODE
				);
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
		context: NativeDatabase | NativeTransaction,
		key: Key,
		alwaysCreateNewBuffer: boolean = false,
		txnId?: number,
	): any | undefined {
		let keyEnd = this.encodeKey(key).end;
		if (alwaysCreateNewBuffer) {
			keyEnd |= ALWAYS_CREATE_BUFFER_FLAG;
		}
		let result = context.getSync(
			keyEnd | ONLY_IF_IN_MEMORY_CACHE_FLAG,
			txnId,
		);
		if (typeof result === 'number') { // return a number indicates it is using the default buffer
			if (result === NOT_IN_MEMORY_CACHE_FLAG) {
				// is not in memory cache, use async get
				return new Promise((resolve, reject) => {
					context.get(keyEnd, resolve, reject, txnId);
				});
			}
			VALUE_BUFFER.end = result;
			return VALUE_BUFFER;
		}
		return result;
	}

	getCount(context: NativeDatabase | NativeTransaction, options?: RangeOptions): number {
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

	getRange(
		context: NativeDatabase | NativeTransaction,
		options?: IteratorOptions & DBITransactional
	): ExtendedIterable<DBIteratorValue<any>> {
		if (!this.db.opened) {
			throw new Error('Database not open');
		}

		options = { ...options };

		const unencodedStartKey = options.key ?? options.start;

		if (unencodedStartKey !== undefined) {
			const start = this.encodeKey(unencodedStartKey);
			options.start = Buffer.from(start.subarray(start.start, start.end));
		}

		if (options.key !== undefined) {
			options.end = options.start;
			options.inclusiveEnd = true;
		} else if (options.end !== undefined) {
			const end = this.encodeKey(options.end);
			options.end = Buffer.from(end.subarray(end.start, end.end));
		}

		return new ExtendedIterable(
			// @ts-expect-error ExtendedIterable v1 constructor type definition is incorrect
			new DBIterator(
				new NativeIterator(context, options) as Iterator<DBIteratorValue<any>>,
				this,
				options
			)
		);
	}

	getSync(
		context: NativeDatabase | NativeTransaction,
		key: Key,
		alwaysCreateNewBuffer: boolean = false,
		options?: GetOptions & DBITransactional
	): any | undefined {
		let keyEnd = this.encodeKey(key).end;
		if (alwaysCreateNewBuffer) {
			keyEnd |= ALWAYS_CREATE_BUFFER_FLAG;
		}
		let result = context.getSync(
			keyEnd,
			this.getTxnId(options)
		);
		if (typeof result === 'number') { // return a number indicates it is using the default buffer
			VALUE_BUFFER.end = result;
			return VALUE_BUFFER;
		}
		return result;
	}

	/**
	 * Checks if the data method options object contains a transaction ID and
	 * returns it.
	 */
	getTxnId(options?: DBITransactional | unknown): number | undefined {
		let txnId: number | undefined;
		if (options && typeof options === 'object' && 'transaction' in options) {
			txnId = (options.transaction as Transaction)?.id;
			if (txnId === undefined) {
				throw new TypeError('Invalid transaction');
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

		this.db.open(this.path, {
			disableWAL: this.disableWAL,
			mode: this.pessimistic ? 'pessimistic' : 'optimistic',
			name: this.name,
			noBlockCache: this.noBlockCache,
			parallelismThreads: this.parallelismThreads,
			transactionLogMaxAgeThreshold: this.transactionLogMaxAgeThreshold,
			transactionLogMaxSize: this.transactionLogMaxSize,
			transactionLogRetentionMs: this.transactionLogRetention
				? parseDuration(this.transactionLogRetention)
				: undefined,
			transactionLogsPath: this.transactionLogsPath
		});

		return false;
	}

	putSync(
		context: NativeDatabase | NativeTransaction,
		key: Key,
		value: any,
		options?: PutOptions & DBITransactional
	): void {
		if (!this.db.opened) {
			throw new Error('Database not open');
		}

		// IMPORTANT!
		// We MUST encode the value before the key because if the `sharedStructuresKey`
		// is set, it will be used by `getStructures()` and `saveStructures()` which in
		// turn will encode the `sharedStructuresKey` into the shared `keyBuffer`
		// overwriting this method's encoded key!
		const valueBuffer = this.encodeValue(value);

		context.putSync(
			this.encodeKey(key),
			valueBuffer,
			this.getTxnId(options)
		);
	}

	removeSync(
		context: NativeDatabase | NativeTransaction,
		key: Key,
		options?: DBITransactional | undefined
	): void {
		if (!this.db.opened) {
			throw new Error('Database not open');
		}

		context.removeSync(
			this.encodeKey(key),
			this.getTxnId(options)
		);
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
	useLog(
		context: NativeDatabase | NativeTransaction,
		name: string | number
	): TransactionLog {
		if (typeof name !== 'string' && typeof name !== 'number') {
			throw new TypeError('Log name must be a string or number');
		}
		return context.useLog(String(name));
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

		return this.db.withLock(
			this.encodeKey(key),
			callback
		);
	}
}

export interface GetOptions {
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
};
