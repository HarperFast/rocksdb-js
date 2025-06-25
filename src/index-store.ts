import { ExtendedIterable } from '@harperdb/extended-iterable';
import { NativeDatabase, NativeIterator, NativeTransaction } from './load-binding.js';
import { BOUNDARY, DBIterator, type DBIteratorValue, HEADER } from './dbi-iterator.js';
import { GetOptions, getTxnId, Store, type PutOptions } from './store.js';
import type { BufferWithDataView, Key } from './encoding.js';
import type { DBITransactional, IteratorOptions, RangeOptions } from './dbi.js';

const EMPTY = Buffer.alloc(0);

/**
 * A store that supports duplicate keys.
 */
export class IndexStore extends Store {
	/**
	 * Indicates to the `DBIterator` that the iterator is in dupSort mode.
	 */
	dupSort = true;

	decodeValue(value: Buffer): any {
		// noop because the values are already decoded with the key
		return value;
	}

	#encodeIndexedKey(key: Key, value?: any) {
		if (Array.isArray(key) && key.includes(null)) {
			throw new Error('Key cannot contain null');
		}

		console.log('encodeIndexedKey', value !== undefined ? [key, null, value] : [key, null]);

		return this.encodeKey(
			value !== undefined ? [key, null, value] : [key, null]
		);
	}

	#encodeIndexedKey2(key: Key, value?: any) {
		const encodedKey = this.encodeKey(key);
		const encodedKeyBuffer = Buffer.from(encodedKey.subarray(encodedKey.start, encodedKey.end));

		const headerBuffer = Buffer.alloc(8);
		headerBuffer.writeUInt32BE(HEADER, 0);
		headerBuffer.writeUInt32BE(encodedKeyBuffer.length, 4);
		console.log('  header', headerBuffer);
		console.log('     key', encodedKeyBuffer);

		// pad the encoded key buffer to 32 bytes
		let paddedKeyBuffer = encodedKeyBuffer;
		if (encodedKeyBuffer.length < 32) {
			paddedKeyBuffer = Buffer.concat([encodedKeyBuffer, Buffer.alloc(32 - encodedKeyBuffer.length)]);
		} else if (encodedKeyBuffer.length > 32) {
			paddedKeyBuffer = encodedKeyBuffer.subarray(0, 32);
		}

		const boundaryBuffer = Buffer.alloc(4);
		boundaryBuffer.writeUInt32BE(BOUNDARY, 0);

		if (value !== undefined) {
			const encodedValue = this.encodeKey(value);
			const encodedValueBuffer = encodedValue.subarray(encodedValue.start, encodedValue.end);
			console.log('boundary', boundaryBuffer);
			console.log('   value', encodedValueBuffer);
			const result = Buffer.concat([headerBuffer, paddedKeyBuffer, boundaryBuffer, encodedValueBuffer]);
			console.log('  result', result);
			return {
				key: result,
				value: encodedKeyBuffer
			};
		}

		const result = Buffer.concat([headerBuffer, paddedKeyBuffer, boundaryBuffer]);
		console.log(result);
		return {
			key: result,
			value: encodedKeyBuffer
		};
	}

	get(
		context: NativeDatabase | NativeTransaction,
		key: Key,
		resolve: (value: Buffer) => void,
		reject: (err: unknown) => void,
		_txnId?: number
	): number {
		try {
			for (const value of this.getRange(context, {
				key,
				valuesOnly: true
			})) {
				resolve(value);
				break;
			}
			return 0;
		} catch (err) {
			reject(err);
			return 1;
		}
	}

	getRange(context: NativeDatabase | NativeTransaction, options?: IteratorOptions & DBITransactional): ExtendedIterable<DBIteratorValue<any> | any> {
		if (!this.db.opened) {
			throw new Error('Database not open');
		}

		let start: Buffer | undefined;
		let end: Buffer | undefined;

		console.log('getRange', options);

		if (options?.key !== undefined) {
			// start = this.#encodeIndexedKey2(options.key).key;
			// console.log('range start', start);
			const encodedStartKey = this.#encodeIndexedKey(options.key);
			start = Buffer.from(encodedStartKey.subarray(encodedStartKey.start, encodedStartKey.end));
			end = Buffer.concat([start, Buffer.from([0xff])]);
		} else {
			if (options?.start !== undefined) {
				// start = this.#encodeIndexedKey2(options.start).key;
				const encodedStartKey = this.#encodeIndexedKey(options.start);
				start = Buffer.from(encodedStartKey.subarray(encodedStartKey.start, encodedStartKey.end));
			}

			if (options?.end !== undefined) {
				// const endKey = this.encodeKey(options.end);
				const encodedEndKey = this.#encodeIndexedKey(options.end);
				end = Buffer.from(encodedEndKey.subarray(encodedEndKey.start, encodedEndKey.end));
				// end = this.#encodeIndexedKey2(options.end).key;
			}
		}

		console.log('start', start);
		console.log('end', end);

		return new ExtendedIterable<DBIteratorValue<any> | any>(
			new DBIterator(
				new NativeIterator(context, {
					...options,
					inclusiveEnd: options?.inclusiveEnd || !!options?.key,
					start,
					end
				}),
				this,
				options
			)
		);
	}

	#getSortKeysOnly(context: NativeDatabase | NativeTransaction, key: Key, options?: DBITransactional): ExtendedIterable<DBIteratorValue<any> | any> {
		if (!this.db.opened) {
			throw new Error('Database not open');
		}

		const startKey = this.#encodeIndexedKey(key);
		const start = startKey.subarray(startKey.start, startKey.end);
		// const start = this.#encodeIndexedKey2(key).key;
		const end = Buffer.concat([start, Buffer.from([0xff])]);

		return new ExtendedIterable<DBIteratorValue<any> | any>(
			new DBIterator(
				new NativeIterator(context, {
					...options,
					start,
					end
				}),
				this,
				{ ...options, sortKeyOnly: true }
			)
		);
	}

	getSync(context: NativeDatabase | NativeTransaction, key: Key, _options?: GetOptions & DBITransactional) {
		if (!this.db.opened) {
			throw new Error('Database not open');
		}

		for (const value of this.getRange(context, {
			key,
			valuesOnly: true
		})) {
			return value;
		}
	}

	getValuesCount(context: NativeDatabase | NativeTransaction, key: Key, options?: RangeOptions & DBITransactional) {
		const startKey = this.#encodeIndexedKey(key);
		const start = startKey.subarray(startKey.start, startKey.end);
		// const start = this.#encodeIndexedKey2(key).key;
		const end = Buffer.concat([start, Buffer.from([0xff])]);

		return context.getCount({
			...options,
			start,
			end
		}, getTxnId(options));
	}

	putSync(context: NativeDatabase | NativeTransaction, key: Key, value: any, options?: PutOptions & DBITransactional) {
		if (!this.db.opened) {
			throw new Error('Database not open');
		}

		if (key === undefined) {
			throw new Error('Key is required');
		}

		// const keyBuffer = this.encodeKey(key);
		// console.log(key, keyBuffer.subarray(keyBuffer.start, keyBuffer.end));
		// const keyValueBuffer2 = this.#encodeIndexedKey(key);
		// console.log(key, keyValueBuffer2.subarray(keyValueBuffer2.start, keyValueBuffer2.end));
		// const keyValueBuffer = this.#encodeIndexedKey(key, value);
		// console.log(key, keyValueBuffer.subarray(keyValueBuffer.start, keyValueBuffer.end));
		// const { key: keyValueBuffer, value: valueBuffer } = this.#encodeIndexedKey2(key, value);
		// console.log(key, keyValueBuffer);
		// context.putSync(keyValueBuffer, valueBuffer, getTxnId(options));

		const keyValueBuffer = this.#encodeIndexedKey(key, value);
		const b = Buffer.from(keyValueBuffer.subarray(keyValueBuffer.start, keyValueBuffer.end));
		context.putSync(b, EMPTY, getTxnId(options));
		// console.log('putSync', key, b);
		// const c = this.decodeKey(b);
		// console.log('putSync', key, c);
	}

	removeSync(context: NativeDatabase | NativeTransaction, key: Key, value?: any, options?: DBITransactional) {
		if (!this.db.opened) {
			throw new Error('Database not open');
		}

		if (key === undefined) {
			throw new Error('Key is required');
		}

		if (options === undefined && value?.transaction !== undefined) {
			options = value;
			value = undefined;
		}

		if (value !== undefined) {
			// const keyValueBuffer = this.#encodeIndexedKey2(key, value).key;
			const keyValueBuffer = this.#encodeIndexedKey(key, value);
			context.removeSync(keyValueBuffer, getTxnId(options));
		} else {
			for (const { key: sortKey } of this.#getSortKeysOnly(context, key, options)) {
				context.removeSync(sortKey, getTxnId(options));
			}
		}
	}
}
