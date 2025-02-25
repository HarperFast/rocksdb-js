import { Transaction } from './transaction.js';
import * as orderedBinary from 'ordered-binary';
import type { Key } from './types.js';
import {
	readBufferKey,
	readUint32Key,
	writeBufferKey,
	writeUint32Key,
	type ReadKeyFunction,
	type WriteKeyFunction,
} from './encoding.js';
import { Database } from './util/load-binding.js';

type Decoder = {
	decode?: (buffer: Buffer) => any;
};

type Encoder = {
	copyBuffers?: boolean;
	encode?: (value: any, mode?: number) => Buffer | string;
	Encoder?: new (options: any) => Encoder;
	readKey?: (buffer: Buffer, start: number, end: number, inSequence?: boolean) => any;
	writeKey?: (key: any, target: Buffer, position: number, inSequence?: boolean) => number;
};

type Encoding = 'binary' | 'cbor' | 'json' | 'msgpack' | 'ordered-binary' | 'string';

type KeyEncoding = 'binary' | 'ordered-binary' | 'uint32';

export type RocksStoreOptions = {
	cache?: boolean;
	dupSort?: boolean;
	encoder?: Encoder;
	encoding?: Encoding;
	keyEncoder?: {
		readKey?: ReadKeyFunction<Key>;
		writeKey?: WriteKeyFunction<Buffer | number>;
	};
	keyEncoding?: KeyEncoding;
	parallelism?: number;
	useVersions?: boolean;
};

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

/**
 * A store represents a RocksDB column family.
 */
export class RocksStore {
	db: Database;

	#cache: boolean;
	#decoder?: Decoder | null;
	#dupSort: boolean;
	#encoder: Encoder | null;
	#encoding: Encoding;
	#initialized: boolean;
	#keyBuffer: Buffer;
	#keyEncoding: KeyEncoding;
	#keyEncoder?: {
		readKey?: ReadKeyFunction<Key>;
		writeKey?: WriteKeyFunction<Buffer | number>;
	};
	#readKey: ReadKeyFunction<Key>;
	#useVersions: boolean;
	#writeKey: WriteKeyFunction<Key>;

	constructor(db: Database, options?: RocksStoreOptions) {
		this.db = db;

		this.#keyBuffer = Buffer.allocUnsafeSlow(0x1000); // 4KB

		this.#cache = options?.cache ?? false; // TODO: better name?
		this.#dupSort = options?.dupSort ?? false; // TODO: better name?
		this.#encoder = options?.encoder ?? null;
		this.#encoding = options?.encoding ?? 'msgpack';
		this.#initialized = false;
		this.#keyEncoder = options?.keyEncoder;
		this.#keyEncoding = options?.keyEncoding ?? 'ordered-binary';
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

		const result = this.db.get(key);
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

	/**
	 * Initializes the store. This must be called before using the store.
	 */
	async init(): Promise<RocksStore> {
		if (this.#initialized) {
			return this;
		}

		// initialize the encoder
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

		this.#initialized = true;
		return this;
	}

	async openDB(): Promise<RocksStore> {
		// TODO: create a new column family
		return this;
	}

	put(key: Key, value: any, options?: PutOptions) {
		this.db.put(key, value);
	}

	remove(key: Key, ifVersionOrValue?: symbol | number | null) {
		// This should be similar to put, except no need to pass in the value
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
