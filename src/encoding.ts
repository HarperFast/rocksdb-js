import * as orderedBinary from 'ordered-binary';
import type { BufferWithDataView, Key } from './types.js';

export type Encoder = {
	copyBuffers?: boolean;
	decode?: (buffer: Buffer) => any;
	encode?: (value: any, mode?: number) => Buffer; // | string;
	Encoder?: new (options?: any) => Encoder;
	freezeData?: boolean;
	needsStableBuffer?: boolean;
	randomAccessStructure?: boolean;
	readKey?: (buffer: Buffer, start: number, end: number, inSequence?: boolean) => any;
	structuredClone?: boolean;
	useFloat32?: boolean;
	writeKey?: (key: any, target: Buffer, position: number, inSequence?: boolean) => number;
};

export type Encoding = 'binary' | 'ordered-binary' | 'msgpack';

export type KeyEncoder = {
	readKey?: ReadKeyFunction<Key>;
	writeKey?: WriteKeyFunction<Buffer | number>;
};

export type KeyEncoding = 'binary' | 'ordered-binary' | 'uint32';

export type ReadKeyFunction<T> = (source: BufferWithDataView, start: number, end?: number) => T;
export type WriteKeyFunction<T> = T extends Buffer
	? (key: Buffer, target: BufferWithDataView, start: number) => number
	: (key: Key, target: BufferWithDataView, start: number) => number;

export function readBufferKey(source: BufferWithDataView, start: number, end?: number): Uint8Array {
	return Uint8Array.prototype.slice.call(source, start, end);
}

export function writeBufferKey(key: Buffer, target: BufferWithDataView, start: number): number {
	target.set(key, start);
	return key.length + start;
}

export function readUint32Key(source: BufferWithDataView, start: number, _end?: number): number {
	if (!source.dataView) {
		source.dataView = new DataView(source.buffer);
	}
	return source.dataView.getUint32(start, true);
}

export function writeUint32Key(key: number, target: BufferWithDataView, start: number): number {
	if (!target.dataView) {
		target.dataView = new DataView(target.buffer);
	}
	if (isNaN(key)) {
		throw new TypeError('Key is not a number');
	}
	target.dataView.setUint32(start, key, true);
	return start + 4;
}

/**
 * Initializes the key encoder functions.
 *
 * @param keyEncoding - The key encoding to use.
 * @param keyEncoder - The key encoder to use.
 * @returns The key encoder.
 */
export function initKeyEncoder(keyEncoding?: KeyEncoding | undefined, keyEncoder?: KeyEncoder | undefined) {
	let readKey;
	let writeKey;
	
	if (keyEncoder) {
		({ readKey, writeKey } = keyEncoder);
		if (!readKey || !writeKey) {
			throw new Error('Custom key encoder must provide both readKey and writeKey');
		}
	} else if (keyEncoding === 'binary') {
		readKey = readBufferKey;
		writeKey = writeBufferKey;
	} else if (keyEncoding === 'uint32') {
		readKey = readUint32Key;
		writeKey = writeUint32Key;
	} else if (!keyEncoding || keyEncoding === 'ordered-binary') {
		keyEncoding = 'ordered-binary';
		readKey = orderedBinary.readKey;
		writeKey = orderedBinary.writeKey;
	} else {
		throw new Error(`Invalid key encoding: ${keyEncoding}`);
	}

	return { keyEncoding, readKey, writeKey };
}
