import { DB } from './util/load-binding';
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

export interface StoreOptions {
	decoder?: Decoder | null;
	encoder?: Encoder;
	encoding?: Encoding;
	keyBuffer?: Buffer;
	keyEncoder?: {
		readKey?: ReadKeyFunction<Key>;
		writeKey?: WriteKeyFunction<Buffer | number>;
	};
	keyEncoding?: KeyEncoding;
	name?: string;
	parallelism?: number;
}

/**
 * A store wraps the native database binding and database settings so that a
 * single database instance can be shared between the main RocksDatabase class
 * and the Transaction class.
 */
export class Store {
	db: DB;
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
	readKey: ReadKeyFunction<Key>;
	writeKey: WriteKeyFunction<Key>;

	constructor(path: string, options?: StoreOptions) {
		this.db = new DB();
		this.encoder = options?.encoder ?? null;
		this.encoding = options?.encoding ?? 'msgpack';
		this.keyBuffer = Buffer.allocUnsafeSlow(0x1000); // 4KB
		this.keyEncoder = options?.keyEncoder;
		this.keyEncoding = options?.keyEncoding ?? 'ordered-binary';
		this.name = options?.name ?? 'default';
		this.parallelism = options?.parallelism ?? 1;
		this.path = path;
		this.readKey = orderedBinary.readKey;
		this.writeKey = orderedBinary.writeKey;
	}

	close() {
		this.db.close();
	}

	isOpen() {
		return this.db.opened;
	}

	async open() {
		if (this.db.opened) {
			return this;
		}

		this.db.open(this.path, {
			name: this.name,
			parallelism: this.parallelism,
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