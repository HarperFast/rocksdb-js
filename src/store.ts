import {
	NativeDatabase,
	type NativeDatabaseOptions,
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

const KEY_BUFFER_SIZE = 4096;
const REUSE_BUFFER_MODE = 512;
const RESET_BUFFER_MODE = 1024;
const WRITE_BUFFER_SIZE = 65536;

// class KeyBuffer {
// 	buffer: Buffer;
// 	uint32: Uint32Array;
// 	float64: Float64Array;
// 	view: DataView;

// 	constructor() {
// 		this.buffer = Buffer.allocUnsafeSlow(KEY_BUFFER_SIZE);
// 		const { buffer: internalBuffer } = this.buffer;
// 		this.view = new DataView(internalBuffer, 0, KEY_BUFFER_SIZE);
// 		this.uint32 = new Uint32Array(internalBuffer, 0, KEY_BUFFER_SIZE >> 2);
// 		this.float64 = new Float64Array(internalBuffer, 0, KEY_BUFFER_SIZE >> 3);
// 	}
// }

/**
 * Options for the `Store` class.
 */
export interface StoreOptions extends Omit<NativeDatabaseOptions, 'mode'> {
	decoder?: Decoder | null;
	encoder?: Encoder;
	encoding?: Encoding;
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
	// keyBuffer: KeyBuffer;
	keyEncoding: KeyEncoding;
	keyEncoder?: {
		readKey?: ReadKeyFunction<Key>;
		writeKey?: WriteKeyFunction<Buffer | number>;
	};
	name: string;
	noBlockCache?: boolean;
	parallelismThreads: number;
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
		// this.keyBuffer = new KeyBuffer();
		this.keyEncoder = options?.keyEncoder;
		this.keyEncoding = options?.keyEncoding ?? 'ordered-binary';
		this.name = options?.name ?? 'default';
		this.noBlockCache = options?.noBlockCache;
		this.parallelismThreads = options?.parallelismThreads ?? 1;
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

	decodeValue(value: Buffer) {
		if (this.decoder?.decode) {
			return this.decoder.decode(value);
		}

		if (value && this.encoding === 'json') {
			return JSON.parse(value.toString());
		}

		return value;
	}

	encodeKey(key: Key) {
		const buffer = Buffer.from(
			new SharedArrayBuffer(WRITE_BUFFER_SIZE)
		);
		const _bytesWritten = this.writeKey(key, buffer, 0);
		return buffer;
	}

	/**
	 * Encodes a value for the database.
	 *
	 * @param value - The value to encode.
	 * @returns The encoded value.
	 */
	encodeValue(value: any) {
		if (value && value['\x10binary-data\x02']) {
			return value['\x10binary-data\x02'];
		}

		if (this.encoder?.encode) {
			if (this.encoder.copyBuffers) {
				return this.encoder.encode(
					value,
					REUSE_BUFFER_MODE // | (writeTxn ? RESET_BUFFER_MODE : 0),
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
	async open(): Promise<void> {
		if (this.db.opened) {
			return;
		}

		this.db.open(this.path, {
			name: this.name,
			noBlockCache: this.noBlockCache,
			parallelismThreads: this.parallelismThreads,
			mode: this.pessimistic ? 'pessimistic' : 'optimistic',
		});

		if (this.encoding === 'ordered-binary') {
			this.encoder = {
				writeKey: orderedBinary.writeKey,
				readKey: orderedBinary.readKey,
			};
		} else {
			// custom encoder, binary, cbor, json, msgpack, or string
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
			} else if (this.encoding === 'json') {
				this.encoder = {
					encode: (value: any) => JSON.stringify(value),
				};
			} else if (this.encoder?.decode) {
				// binary or string
				this.decoder = {
					decode: this.encoder.decode,
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