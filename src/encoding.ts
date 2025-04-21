import { BufferWithDataView, Key } from './types.js';

export type Decoder = {
	decode?: (buffer: Buffer) => any;
};

export type Encoder = {
	copyBuffers?: boolean;
	decode?: (buffer: Buffer) => any;
	encode?: (value: any, mode?: number) => Buffer | string;
	Encoder?: new (options: any) => Encoder;
	readKey?: (buffer: Buffer, start: number, end: number, inSequence?: boolean) => any;
	writeKey?: (key: any, target: Buffer, position: number, inSequence?: boolean) => number;
};

export type Encoding = 'binary' | 'cbor' | 'json' | 'msgpack' | 'ordered-binary' | 'string';

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
		source.dataView = new DataView(source.buffer, 0, source.length);
	}
	return source.dataView.getUint32(start, true);
}

export function writeUint32Key(key: number, target: BufferWithDataView, start: number): number {
	if (!target.dataView) {
		target.dataView = new DataView(target.buffer, 0, target.length);
	}
	target.dataView.setUint32(start, key, true);
	return start + 4;
}