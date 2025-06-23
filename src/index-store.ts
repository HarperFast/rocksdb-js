import { ExtendedIterable } from '@harperdb/extended-iterable';
import { NativeDatabase, NativeIterator, NativeTransaction } from './load-binding.js';
import { BOUNDARY, DBIterator, type DBIteratorValue } from './dbi-iterator.js';
import { GetOptions, getTxnId, Store, type PutOptions } from './store.js';
import type { BufferWithDataView, Key } from './encoding.js';
import type { DBITransactional, IteratorOptions } from './dbi.js';

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

	getRange(context: NativeDatabase | NativeTransaction, options?: IteratorOptions & DBITransactional): ExtendedIterable<DBIteratorValue<any> | any> {
		if (!this.db.opened) {
			throw new Error('Database not open');
		}

		let start: Buffer | undefined;
		let end: Buffer | undefined;
		let encodedStartKey: BufferWithDataView | undefined;

		if (options?.key) {
			encodedStartKey = this.encodeKey([options.key, BOUNDARY]);
			start = Buffer.from(encodedStartKey.subarray(encodedStartKey.start, encodedStartKey.end));
			end = Buffer.concat([start, Buffer.from([0xff])]);
		} else {
			if (options?.start) {
				const startKey = this.encodeKey(options.start);
				start = Buffer.from(startKey.subarray(startKey.start, startKey.end));
			}

			if (options?.end) {
				const endKey = this.encodeKey(options.end);
				end = Buffer.from(endKey.subarray(endKey.start, endKey.end));
			}
		}

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

		const encodedStartKey = this.encodeKey([key, BOUNDARY]);
		const start = Buffer.from(encodedStartKey.subarray(encodedStartKey.start, encodedStartKey.end));
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

	putSync(context: NativeDatabase | NativeTransaction, key: Key, value: any, options?: PutOptions & DBITransactional) {
		if (!this.db.opened) {
			throw new Error('Database not open');
		}

		const keyValueBuffer = this.encodeKey([key, BOUNDARY, value]);
		context.putSync(keyValueBuffer, EMPTY, getTxnId(options));
	}

	removeSync(context: NativeDatabase | NativeTransaction, key: Key, value?: any, options?: DBITransactional) {
		if (!this.db.opened) {
			throw new Error('Database not open');
		}

		if (options === undefined && value?.transaction !== undefined) {
			options = value;
			value = undefined;
		}

		if (value !== undefined) {
			const keyValueBuffer = this.encodeKey([key, BOUNDARY, value]);
			context.removeSync(keyValueBuffer, getTxnId(options));
		} else {
			for (const { key: sortKey } of this.#getSortKeysOnly(context, key, options)) {
				context.removeSync(sortKey, getTxnId(options));
			}
		}

		context.removeSync(
			this.encodeKey(key),
			getTxnId(options)
		);
	}
}
