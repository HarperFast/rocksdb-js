import {
	NativeDatabase,
	type NativeDatabaseOptions,
	type NativeDatabaseMode,
} from './util/load-binding';
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
import type { Key } from './types';

/**
 * Options for the `Store` class.
 */
export interface StoreOptions extends Omit<NativeDatabaseOptions, 'mode'> {
	decoder?: Decoder | null;
	encoder?: Encoder;
	encoding?: Encoding;
	keyBuffer?: Buffer;
	keyEncoder?: {
		readKey?: ReadKeyFunction<Key>;
		writeKey?: WriteKeyFunction<Buffer | number>;
	};
	keyEncoding?: KeyEncoding;
	pessimistic?: boolean;
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
	decoder?: Decoder | null;
	encoder: Encoder | null;
	encoding: Encoding;
	keyBuffer: Buffer;
	keyEncoding: KeyEncoding;
	keyEncoder?: {
		readKey?: ReadKeyFunction<Key>;
		writeKey?: WriteKeyFunction<Buffer | number>;
	};
	name: string;
	parallelism: number;
	path: string;
	pessimistic: boolean;
	readKey: ReadKeyFunction<Key>;
	writeKey: WriteKeyFunction<Key>;

	/**
	 * Initializes the store with a new `NativeDatabase` instance.
	 *
	 * @param path - The path to the database.
	 * @param options - The options for the store.
	 */
	constructor(path: string, options?: StoreOptions) {
		this.db = new NativeDatabase();
		this.encoder = options?.encoder ?? null;
		this.encoding = options?.encoding ?? 'msgpack';
		this.keyBuffer = Buffer.allocUnsafeSlow(0x1000); // 4KB
		this.keyEncoder = options?.keyEncoder;
		this.keyEncoding = options?.keyEncoding ?? 'ordered-binary';
		this.name = options?.name ?? 'default';
		this.parallelism = options?.parallelism ?? 1;
		this.path = path;
		this.pessimistic = options?.pessimistic ?? false;
		this.readKey = orderedBinary.readKey;
		this.writeKey = orderedBinary.writeKey;
	}

	/**
	 * Closes the database.
	 */
	close() {
		this.db.close();
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
	async open(): Promise<void> {
		if (this.db.opened) {
			return;
		}

		this.db.open(this.path, {
			name: this.name,
			parallelism: this.parallelism,
			mode: this.pessimistic ? 'pessimistic' : 'optimistic',
		});

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
				this.encoder = new EncoderClass({
					...this.encoder
				});
			} else if (!encoderFn && this.encoding === 'json') {
				this.encoder = {
					encode: (value: any) => JSON.stringify(value),
				};
			}
		}

		if (this.encoder?.writeKey && !this.encoder.encode) {
			this.encoder.encode = (_value: any, _mode?: number): Buffer => {
				// TODO: Implement
				return Buffer.from('');
			};
			this.encoder.copyBuffers = true;
		}

		if (this.keyEncoding === 'uint32') {
			this.readKey = readUint32Key;
			this.writeKey = writeUint32Key as WriteKeyFunction<Key>;
		} else if (this.keyEncoding === 'binary') {
			this.readKey = readBufferKey;
			this.writeKey = writeBufferKey as WriteKeyFunction<Key>;
		} else if (this.keyEncoder) {
			const { readKey, writeKey } = this.keyEncoder;
			if (!readKey || !writeKey) {
				throw new Error('Custom key encoder must provide both readKey and writeKey');
			}
			this.readKey = readKey;
			this.writeKey = writeKey as WriteKeyFunction<Key>;
		}
	}
}