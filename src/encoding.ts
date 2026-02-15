import * as orderedBinary from 'ordered-binary';

export type Key = Key[] | string | symbol | number | boolean | Uint8Array | Buffer | null;

export interface BufferWithDataView extends Buffer {
	dataView: DataView;
	start: number;
	end: number;
}

export type EncoderFunction = new (options?: any) => Encoder;

export interface Encoder {
	copyBuffers?: boolean;
	decode?: (buffer: BufferWithDataView, options?: { end: number }) => any;
	encode?: (value: any, mode?: number) => Buffer; // | string;
	Encoder?: EncoderFunction;
	freezeData?: boolean;
	needsStableBuffer?: boolean;
	randomAccessStructure?: boolean;
	readKey?: (buffer: Buffer, start: number, end: number, inSequence?: boolean) => any;
	structuredClone?: boolean;
	structures?: any[];
	useFloat32?: boolean;
	writeKey?: (key: any, target: Buffer, position: number, inSequence?: boolean) => number;
}

export type Encoding = 'binary' | 'ordered-binary' | 'msgpack' | false;

export interface KeyEncoder {
	readKey?: ReadKeyFunction<Key>;
	writeKey?: WriteKeyFunction;
}

export type KeyEncoding = 'binary' | 'ordered-binary' | 'uint32';

export type ReadKeyFunction<T> = (source: BufferWithDataView, start: number, end?: number) => T;
export type WriteKeyFunction = (key: Key, target: BufferWithDataView, start: number) => number;

/**
 * Initializes the key encoder functions.
 *
 * @param keyEncoding - The key encoding to use.
 * @param keyEncoder - The key encoder to use.
 * @returns The key encoder.
 */
export function initKeyEncoder(
	requestedKeyEncoding?: KeyEncoding | undefined,
	keyEncoder?: KeyEncoder | undefined
): { keyEncoding: KeyEncoding; readKey: ReadKeyFunction<Key>; writeKey: WriteKeyFunction } {
	const keyEncoding: KeyEncoding = requestedKeyEncoding ?? 'ordered-binary';

	if (keyEncoder) {
		const { readKey, writeKey } = keyEncoder;
		if (!readKey || !writeKey) {
			throw new Error('Custom key encoder must provide both readKey and writeKey');
		}
		return { keyEncoding, readKey, writeKey };
	}

	if (keyEncoding === 'binary') {
		return {
			keyEncoding,
			readKey(source: BufferWithDataView, start: number, end?: number): Uint8Array {
				return Uint8Array.prototype.slice.call(source, start, end);
			},
			writeKey(key: Key, target: BufferWithDataView, start: number): number {
				const keyBuffer = key instanceof Buffer ? key : Buffer.from(String(key));
				target.set(keyBuffer, start);
				return keyBuffer.length + start;
			},
		};
	}

	if (keyEncoding === 'uint32') {
		return {
			keyEncoding,
			readKey(source: BufferWithDataView, start: number, _end?: number): number {
				if (!source.dataView) {
					source.dataView = new DataView(source.buffer);
				}
				return source.dataView.getUint32(start, true);
			},
			writeKey(key: Key, target: BufferWithDataView, start: number): number {
				const keyNumber = Number(key);
				if (isNaN(keyNumber)) {
					throw new TypeError('Key is not a number');
				}
				target.dataView.setUint32(start, keyNumber, true);
				return start + 4;
			},
		};
	}

	if (keyEncoding === 'ordered-binary') {
		return {
			keyEncoding,
			readKey: orderedBinary.readKey,
			writeKey: orderedBinary.writeKey as WriteKeyFunction,
		};
	}

	throw new Error(`Invalid key encoding: ${keyEncoding}`);
}

/**
 * Creates a fixed-size buffer with a data view, start, and end properties.
 *
 * Note: It uses `Buffer.allocUnsafe()` because it's the fastest by using
 * Node.js's preallocated memory pool, though the memory is not zeroed out.
 *
 * @param size - The size of the buffer.
 * @returns The buffer with a data view.
 */
export function createFixedBuffer(size: number): BufferWithDataView {
	const buffer = Buffer.allocUnsafeSlow(size) as BufferWithDataView;
	buffer.dataView = new DataView(buffer.buffer);
	buffer.start = 0;
	buffer.end = 0;
	return buffer;
}
