import {
	NativeDatabase,
	type NativeDatabaseOptions,
} from './load-binding.js';
import {
	Encoding,
	type Encoder,
	type KeyEncoding,
	type ReadKeyFunction,
	type WriteKeyFunction,
	initKeyEncoder,
} from './encoding.js';
import type { BufferWithDataView, Key } from './types';

// const MAX_KEY_SIZE = 1024 * 1024; // 1MB
const KEY_BUFFER_SIZE = 4096;
const REUSE_BUFFER_MODE = 512;
const RESET_BUFFER_MODE = 1024;
// const WRITE_BUFFER_SIZE = 65536;

/**
 * Options for the `Store` class.
 */
export interface StoreOptions extends Omit<NativeDatabaseOptions, 'mode'> {
	// cache?: boolean;
	decoder?: Encoder | null;
	// dupSort?: boolean;
	encoder?: Encoder | null;
	encoding?: 'msgpack' | 'ordered-binary';
	keyEncoder?: {
		readKey?: ReadKeyFunction<Key>;
		writeKey?: WriteKeyFunction<Buffer | number>;
	};
	keyEncoding?: KeyEncoding;
	// mapSize?: number;
	// maxDbs?: number;
	// maxFreeSpaceToLoad?: number;
	// maxFreeSpaceToRetain?: number;
	// maxReaders?: number;
	// maxKeySize?: number;
	// noReadAhead?: boolean;
	// noSync?: boolean;
	// overlappingSync?: boolean;
	// pageSize?: number;
	pessimistic?: boolean;
	// readOnly?: boolean;
	sharedStructuresKey?: symbol;
	// trackMetrics?: boolean;
	// useVersions?: boolean;
}

/**
 * A store wraps the `NativeDatabase` binding and database settings so that a
 * single database instance can be shared between the main `RocksDatabase`
 * instance and the `Transaction` instance.
 *
 * This store should not be shared between `RocksDatabase` instances.
 */
export class Store {
	db: NativeDatabase;
	decoder: Encoder | null;
	decoderCopies: boolean = false;
	encoder: Encoder | null;
	encoding: Encoding | null;
	keyBuffer: BufferWithDataView;
	name: string;
	noBlockCache?: boolean;
	parallelismThreads: number;
	path: string;
	pessimistic: boolean;
	readKey: ReadKeyFunction<Key> | null = null;
	sharedStructuresKey?: symbol;
	writeKey: WriteKeyFunction<Key> | null = null;

	/**
	 * Initializes the store with a new `NativeDatabase` instance.
	 *
	 * @param path - The path to the database.
	 * @param options - The options for the store.
	 */
	constructor(path: string, options?: StoreOptions) {
		this.db = new NativeDatabase();
		this.decoder = options?.decoder ?? null;
		this.encoder = options?.encoder ?? null;
		this.encoding = options?.encoding ?? null;

		this.keyBuffer = Buffer.allocUnsafeSlow(KEY_BUFFER_SIZE);
		this.keyBuffer.dataView = new DataView(this.keyBuffer.buffer);
		this.keyBuffer.start = 0;
		this.keyBuffer.end = 0;

		Object.assign(this, initKeyEncoder(
			options?.keyEncoding ?? 'ordered-binary',
			options?.keyEncoder
		));

		this.name = options?.name ?? 'default';
		this.noBlockCache = options?.noBlockCache;
		this.parallelismThreads = options?.parallelismThreads ?? 1;
		this.path = path;
		this.pessimistic = options?.pessimistic ?? false;
		this.sharedStructuresKey = options?.sharedStructuresKey;
	}

	/**
	 * Closes the database.
	 */
	close() {
		this.db.close();
	}

	/**
	 * Decodes a value from the database.
	 *
	 * @param value - The value to decode.
	 * @returns The decoded value.
	 */
	decodeValue(value: Buffer): any {
		if (typeof this.decoder?.decode === 'function') {
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

		if (!this.writeKey) {
			throw new Error('Write key function is not set');
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
	encodeValue(value: any): Buffer | Uint8Array {
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
}