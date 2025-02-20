import assert from 'node:assert';
import { Transaction } from './transaction.js';
import * as orderedBinary from 'ordered-binary';
import type { Database } from './util/load-binding.js';
import type { Key } from './types.js';

type Decoder = {};

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
	decoder?: Decoder;
	encoder?: Encoder;
	encoding?: Encoding;
	keyEncoding?: KeyEncoding;
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
	decoder: Decoder | null;
	encoder: Encoder | null;
	encoding: Encoding;
	keyEncoding: KeyEncoding;
	initialized: boolean;

	constructor(db: Database, options?: RocksStoreOptions) {
		this.db = db;
		this.decoder = options?.decoder ?? null;
		this.encoder = options?.encoder ?? null;
		this.encoding = options?.encoding ?? 'msgpack';
		this.initialized = false;
		this.keyEncoding = options?.keyEncoding ?? 'ordered-binary';
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

	async get(key: Key, options?: GetOptions): Promise<Buffer | undefined> {
		assert(this.initialized, 'Store not initialized');
		// TODO: Remove async?
		// TODO: Return Promise<any> | any?
		// TODO: Call this.getBinaryFast(key, options) and decode the bytes into a value/object
		return this.db.get(key);
	}

	/**
	 * Calls `getBinaryFast()`. Used by HDBreplication
	 */
	async getBinary(key: Key, options?: GetOptions): Promise<Buffer | undefined> {
		// TODO: singleton buffer (default 64KB) that is reused to hold the data from any immediate returns
		return Buffer.from('TODO');
	}

	async getBinaryFast(key: Key, options?: GetOptions): Promise<Buffer | undefined> {
		return Buffer.from('TODO');
	}

	getEntry(key: Key, options?: GetOptions) {
		const value = this.get(key, options);

		if (value === undefined) {
			return;
		}

		return {
			value,
		};
	}

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
		if (this.initialized) {
			return this;
		}

		this.db.open();

		// initialize the encoder
		if (this.encoding === 'ordered-binary') {
			this.encoder = {
				writeKey: orderedBinary.writeKey,
				readKey: orderedBinary.readKey,
			};
		} else {
			const encoderFn = this.encoder?.encode;
			let EncoderClass = this.encoder?.Encoder;
			if (EncoderClass) {
				// since we have a custom encoder class, null out the encoder so we
				// don't pass it to the custom encoder class constructor
				this.encoder = null;
			}
			if (!EncoderClass && !encoderFn && (this.encoding === 'cbor' || this.encoding === 'msgpack')) {
				EncoderClass = await import(this.encoding === 'cbor' ? 'cbor-x' : 'msgpackr').then(m => m.Encoder);
			}
			if (EncoderClass) {
				this.encoder = new EncoderClass({ ...this.encoder });
			} else if (!encoderFn && this.encoding === 'json') {
				this.encoder = {
					encode: (value: any) => JSON.stringify(value),
				};
			}
		}

		if (this.encoder?.writeKey && !this.encoder.encode) {
			this.encoder.encode = (value: any, mode?: number): Buffer => {
				// TODO: Implement
				return Buffer.from('');
			};
			this.encoder.copyBuffers = true;
		}

		// TODO: init decoder

		// TODO: init writeKey and readKey

		// TODO: create a new column family

		this.initialized = true;
		return this;
	}

	put(key: Key, value: any, options?: PutOptions) {
		assert(this.initialized, 'Store not initialized');
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
