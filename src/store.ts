import {
	NativeDatabase,
	NativeIterator,
	NativeTransaction,
	type NativeDatabaseOptions,
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
import { ExtendedIterable } from '@harperdb/extended-iterable';

const KEY_BUFFER_SIZE = 4096;
const MAX_KEY_SIZE = 1024 * 1024; // 1MB
const RESET_BUFFER_MODE = 1024;
const REUSE_BUFFER_MODE = 512;
const SAVE_BUFFER_SIZE = 8192;
// const WRITE_BUFFER_SIZE = 65536;

/**
 * Options for the `Store` class.
 */
export interface StoreOptions extends Omit<NativeDatabaseOptions, 'mode'> {
	decoder?: Encoder | null;
	encoder?: Encoder | null;
	encoding?: Encoding;
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
	// readOnly?: boolean;
	sharedStructuresKey?: symbol;
	// trackMetrics?: boolean;
}

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
	 * The function used to encode keys.
	 */
	readKey: ReadKeyFunction<Key>;

	/**
	 * The key used to store shared structures.
	 */
	sharedStructuresKey?: symbol;

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
		this.encodeBuffer = createFixedBuffer(SAVE_BUFFER_SIZE);
		this.encoder = options?.encoder ?? null;
		this.encoding = options?.encoding ?? null;
		this.keyBuffer = createFixedBuffer(KEY_BUFFER_SIZE);
		this.keyEncoding = keyEncoding;
		this.maxKeySize = options?.maxKeySize ?? MAX_KEY_SIZE;
		this.name = options?.name ?? 'default';
		this.noBlockCache = options?.noBlockCache;
		this.parallelismThreads = options?.parallelismThreads ?? 1;
		this.path = path;
		this.pessimistic = options?.pessimistic ?? false;
		this.readKey = readKey;
		this.sharedStructuresKey = options?.sharedStructuresKey;
		this.writeKey = writeKey;
	}

	/**
	 * Closes the database.
	 */
	close() {
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
	decodeValue(value: Buffer): any {
		if (value?.length > 0 && typeof this.decoder?.decode === 'function') {
			return this.decoder.decode(value);
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
		resolve: (value: Buffer) => void,
		reject: (err: unknown) => void,
		txnId?: number
	) {
		return context.get(
			this.encodeKey(key),
			resolve,
			reject,
			txnId
		);
	}

	getCount(context: NativeDatabase | NativeTransaction, options?: RangeOptions) {
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

	getSync(context: NativeDatabase | NativeTransaction, key: Key, options?: GetOptions & DBITransactional) {
		return context.getSync(
			this.encodeKey(key),
			this.getTxnId(options)
		);
	}

	/**
	 * Checks if the data method options object contains a transaction ID and
	 * returns it.
	 */
	getTxnId(options?: DBITransactional | unknown) {
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
	 * Checks if the database is open.
	 *
	 * @returns `true` if the database is open, `false` otherwise.
	 */
	isOpen() {
		return this.db.opened;
	}

	/**
	 * Opens the database. This must be called before any database operations
	 * are performed.
	 */
	open() {
		if (this.db.opened) {
			return true;
		}

		this.db.open(this.path, {
			name: this.name,
			noBlockCache: this.noBlockCache,
			parallelismThreads: this.parallelismThreads,
			mode: this.pessimistic ? 'pessimistic' : 'optimistic',
		});
	}

	putSync(context: NativeDatabase | NativeTransaction, key: Key, value: any, options?: PutOptions & DBITransactional) {
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

	removeSync(context: NativeDatabase | NativeTransaction, key: Key, options?: DBITransactional | undefined) {
		if (!this.db.opened) {
			throw new Error('Database not open');
		}

		context.removeSync(
			this.encodeKey(key),
			this.getTxnId(options)
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

	// ifNotTxnId?: number;
	// currentThread?: boolean;
}

export interface PutOptions {
	append?: boolean;
	instructedWrite?: boolean;
	noDupData?: boolean;
	noOverwrite?: boolean;
};
