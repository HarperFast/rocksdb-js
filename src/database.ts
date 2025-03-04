import { DBI } from './util/load-binding.js';
import type { Key } from './types.js';
import {
	readBufferKey,
	readUint32Key,
	writeBufferKey,
	writeUint32Key,
	type Encoder,
	type Decoder,
	type Encoding,
	type KeyEncoding,
	type ReadKeyFunction,
	type WriteKeyFunction,
} from './encoding.js';
import * as orderedBinary from 'ordered-binary';
import { Transaction } from './transaction.js';

/**
 * This class is the public API. It exposes the internal native `Database` class.
 */
export class RocksDatabase {
	#cache: boolean;
	#dbi: DBI;
	#decoder?: Decoder | null;
	#dupSort: boolean;
	#encoder: Encoder | null;
	#encoding: Encoding;
	#keyBuffer: Buffer;
	#keyEncoding: KeyEncoding;
	#keyEncoder?: {
		readKey?: ReadKeyFunction<Key>;
		writeKey?: WriteKeyFunction<Buffer | number>;
	};
	#name?: string;
	#parallelism?: number;
	#path: string;
	#readKey: ReadKeyFunction<Key>;
	#useVersions: boolean;
	#writeKey: WriteKeyFunction<Key>;

	constructor(
		path: string,
		options?: RocksDatabaseOptions
	) {
		if (!path || typeof path !== 'string') {
			throw new TypeError('Path is required');
		}

		if (options !== undefined && typeof options !== 'object') {
			throw new TypeError('Options must be an object');
		}

		this.#cache = options?.cache ?? false; // TODO: better name?
		this.#dbi = new DBI();
		this.#dupSort = options?.dupSort ?? false; // TODO: better name?
		this.#encoder = options?.encoder ?? null;
		this.#encoding = options?.encoding ?? 'msgpack';
		this.#keyBuffer = Buffer.allocUnsafeSlow(0x1000); // 4KB
		this.#keyEncoder = options?.keyEncoder;
		this.#keyEncoding = options?.keyEncoding ?? 'ordered-binary';
		this.#name = options?.name;
		this.#parallelism = options?.parallelism;
		this.#path = path;
		this.#readKey = orderedBinary.readKey;
		this.#useVersions = options?.useVersions ?? false; // TODO: better name?
		this.#writeKey = orderedBinary.writeKey;

		if (this.#dupSort && (this.#cache || this.#useVersions)) {
			throw new Error('The dupSort flag can not be combined with versions or caching');
		}
	}

	/**
	 * In memory lock mechanism for cache resolution.
	 * @param key 
	 * @param version 
	 */
	attemptLock(key: Key, version: number) {
		//
	}

	async clear(): Promise<void> {
		//
	}

	/**
	 * Closes the database.
	 *
	 * @example
	 * ```ts
	 * const db = await RocksDatabase.open('/path/to/database');
	 * db.close();
	 * ```
	 */
	close() {
		this.#dbi.close();
	}

	// committed

	doesExist(key: Key, versionOrValue: number | Buffer) {
		//
	}

	async drop(): Promise<void> {
		//
	}

	dropSync() {
		//
	}

	// flushed

		/**
	 * Retrieves the value for the given key, then returns the decoded value.
	 */
	async get(key: Key, options?: GetOptions): Promise<any | undefined> {
		// TODO: Remove async?
		// TODO: Return Promise<any> | any?
		// TODO: Call this.getBinaryFast(key, options)
		// TODO: decode the bytes into a value/object
		// TODO: if decoder copies, then call getBinaryFast()

		if (this.#encoding === 'binary' || this.#decoder) {
			const bytes = this.getBinary(key, options);

			if (this.#decoder) {
				// TODO: decode
			}

			return bytes;
		}

		if (!this.#dbi.opened) {
			throw new Error('Database not open');
		}

		const result = this.#dbi.get(key);
		if (result && this.#encoding === 'json') {
			return JSON.parse(result.toString());
		}

		return result;
	}

	/**
	 * Retrieves the binary data for the given key. This is just like `get()`,
	 * but bypasses the decoder.
	 *
	 * Note: Used by HDBreplication.
	 */
	async getBinary(key: Key, options?: GetOptions): Promise<Buffer | undefined> {
		const value = this.getBinaryFast(key, options);
		return Buffer.from('TODO');
	}

	/**
	 * Retrieves the binary data for the given key using a preallocated,
	 * reusable buffer. Data in the buffer is only valid until the next get
	 * operation (including cursor operations).
	 *
	 * Note: The reusable buffer slightly differs from a typical buffer:
	 * - `.length` is set to the size of the value
	 * - `.byteLength` is set to the size of the full allocated memory area for
	 *   the buffer (usually much larger).
	 */
	async getBinaryFast(key: Key, options?: GetOptions): Promise<Buffer | undefined> {
		const keyLength = this.#writeKey(key, this.#keyBuffer, 0);
		return Buffer.from('TODO');
	}

	/**
	 * Retrieves a value for the given key as an "entry" object.
	 *
	 * An entry object contains a `value` property and when versions are enabled,
	 * it also contains a `version` property.
	 */
	getEntry(key: Key, options?: GetOptions) {
		const value = this.get(key, options);

		if (value !== undefined) {
			// TODO: if versions are enabled, add a `version` property
			return {
				value,
			};
		}
	}

	/**
	 * Retrieves all keys within a range.
	 */
	getKeys(options?: GetRangeOptions) {
		//
	}

	getRange(options?: GetRangeOptions) {
		//
	}

	getStats() {
		return {
			free: {},
			root: {},
		};
	}

	getUserSharedBuffer(key: Key, defaultBuffer?: Buffer) {
		//
	}

	getValues(key: Key, options?: GetRangeOptions) {
		//
	}

	getValuesCount(key: Key, options?: GetRangeOptions) {
		//
	}

	hasLock(key: Key, version: number): boolean {
		return false;
	}

	async ifNoExists(key: Key): Promise<void> {
		//
	}

	async ifVersion(
		key: Key,
		version?: number | null,
		options?: {
			allowNotFound?: boolean;
			ifLessThan?: number;
		}
	): Promise<void> {
		//
	}

	isOpen() {
		return this.#dbi.opened;
	}

	/**
	 * Sugar method for opening a database.
	 *
	 * @param path - The filesystem path to the database.
	 * @param options - The options for the database.
	 * @returns A new RocksDatabase instance.
	 *
	 * @example
	 * ```ts
	 * const db = await RocksDatabase.open('/path/to/database');
	 * ```
	 */
	static async open(
		path: string,
		options?: RocksDatabaseOptions
	): Promise<RocksDatabase> {
		return new RocksDatabase(path, options).open();
	}

	/**
	 * Opens the database. This function returns immediately if the database is
	 * already open.
	 *
	 * @returns A new RocksDatabase instance.
	 *
	 * @example
	 * ```ts
	 * const db = new RocksDatabase('/path/to/database');
	 * await db.open();
	 * ```
	 */
	async open(): Promise<RocksDatabase> {
		if (this.#dbi.opened) {
			return this;
		}

		this.#dbi.open(this.#path, {
			name: this.#name,
			parallelism: this.#parallelism,
		});

		if (this.#encoding === 'ordered-binary') {
			this.#encoder = {
				writeKey: orderedBinary.writeKey,
				readKey: orderedBinary.readKey,
			};
		} else {
			const encoderFn = this.#encoder?.encode;
			let EncoderClass = this.#encoder?.Encoder;
			if (EncoderClass) {
				// since we have a custom encoder class, null out the encoder so we
				// don't pass it to the custom encoder class constructor
				this.#encoder = null;
			}
			if (!EncoderClass && !encoderFn && (this.#encoding === 'cbor' || this.#encoding === 'msgpack')) {
				EncoderClass = await import(this.#encoding === 'cbor' ? 'cbor-x' : 'msgpackr').then(m => m.Encoder);
			}
			if (EncoderClass) {
				this.#encoder = new EncoderClass({
					...this.#encoder
				});
			} else if (!encoderFn && this.#encoding === 'json') {
				this.#encoder = {
					encode: (value: any) => JSON.stringify(value),
				};
			}
		}

		if (this.#encoder?.writeKey && !this.#encoder.encode) {
			this.#encoder.encode = (value: any, mode?: number): Buffer => {
				// TODO: Implement
				return Buffer.from('');
			};
			this.#encoder.copyBuffers = true;
		}

		if (this.#keyEncoding === 'uint32') {
			this.#readKey = readUint32Key;
			this.#writeKey = writeUint32Key as WriteKeyFunction<Key>;
		} else if (this.#keyEncoding === 'binary') {
			this.#readKey = readBufferKey;
			this.#writeKey = writeBufferKey as WriteKeyFunction<Key>;
		} else if (this.#keyEncoder) {
			const { readKey, writeKey } = this.#keyEncoder;
			if (!readKey || !writeKey) {
				throw new Error('Custom key encoder must provide both readKey and writeKey');
			}
			this.#readKey = readKey;
			this.#writeKey = writeKey as WriteKeyFunction<Key>;
		}

		return this;
	}

	put(key: Key, value: any, options?: PutOptions) {
		if (!this.#dbi.opened) {
			throw new Error('Database not open');
		}

		this.#dbi.put(key, value);
	}

	remove(key: Key, _ifVersionOrValue?: symbol | number | null) {
		if (!this.#dbi.opened) {
			throw new Error('Database not open');
		}

		this.#dbi.remove(key);
	}

	async transaction(callback: (txn: Transaction) => Promise<void>) {
		const txn = new Transaction();
		try {
			await callback(txn);
			await txn.commit();
		} finally {
			await txn.abort();
		}
	}

	unlock(key: Key, version: number): boolean {
		//

		return true;
	}
}

type GetOptions = {
	ifNotTxnId?: number;
	transaction?: Transaction;
};

type GetRangeOptions = {
	end?: Key | Uint8Array;
	exactMatch?: boolean;
	exclusiveStart?: boolean;
	inclusiveEnd?: boolean;
	limit?: number;
	key?: Key;
	offset?: number;
	onlyCount?: boolean;
	reverse?: boolean;
	snapshot?: boolean;
	start?: Key | Uint8Array;
	transaction?: Transaction;
	values?: boolean;
	valuesForKey?: boolean;
	versions?: boolean;
};

type PutOptions = {
	append?: boolean;
	ifVersion?: number;
	instructedWrite?: boolean;
	noDupData?: boolean;
	noOverwrite?: boolean;
	version?: number;
};

interface RocksDatabaseOptions {
	cache?: boolean;
	dupSort?: boolean;
	encoder?: Encoder;
	encoding?: Encoding;
	keyEncoder?: {
		readKey?: ReadKeyFunction<Key>;
		writeKey?: WriteKeyFunction<Buffer | number>;
	};
	keyEncoding?: KeyEncoding;
	name?: string; // defaults to 'default'
	parallelism?: number;
	useVersions?: boolean;
};
