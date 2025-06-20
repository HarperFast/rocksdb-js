import { ExtendedIterable } from '@harperdb/extended-iterable';
import { NativeDatabase, NativeIterator, NativeTransaction } from './load-binding.js';
import { DBIterator, type DBIteratorValue, type DBIteratorSortKey } from './dbi-iterator.js';
import { getTxnId, Store, type PutOptions } from './store.js';
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

	getRange(context: NativeDatabase | NativeTransaction, options?: IteratorOptions & DBITransactional): ExtendedIterable<DBIteratorValue<any>> {
		if (!this.db.opened) {
			throw new Error('Database not open');
		}

		let start: Buffer | undefined;
		let end: Buffer | undefined;
		let encodedStartKey: BufferWithDataView | undefined;

		if (options?.key) {
			encodedStartKey = this.encodeKey([options.key]);
			start = Buffer.from(encodedStartKey.subarray(encodedStartKey.start, encodedStartKey.end));
			end = Buffer.concat([start.subarray(0, -1), Buffer.from([0xff])]);
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

		return new ExtendedIterable<DBIteratorValue<any>>(
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

	#getSortKeys(context: NativeDatabase | NativeTransaction, key: Key, options?: DBITransactional): ExtendedIterable<DBIteratorValue<any>> {
		if (!this.db.opened) {
			throw new Error('Database not open');
		}

		const encodedStartKey = this.encodeKey([key]);
		const start = Buffer.from(encodedStartKey.subarray(encodedStartKey.start, encodedStartKey.end));
		const end = Buffer.concat([start.subarray(0, -1), Buffer.from([0xff])]);

		return new ExtendedIterable<DBIteratorValue<any>>(
			new DBIterator(
				new NativeIterator(context, {
					...options,
					inclusiveEnd: true,
					start,
					end
				}),
				this,
				{ ...options, sortKey: true }
			)
		);
	}

	putSync(context: NativeDatabase | NativeTransaction, key: Key, value: any, options?: PutOptions & DBITransactional) {
		if (!this.db.opened) {
			throw new Error('Database not open');
		}

		const keyValueBuffer = this.encodeKey([key, value]);
		context.putSync(keyValueBuffer, EMPTY, getTxnId(options));

		const keyBuffer = this.encodeKey(key);
		const valueBuffer = this.encodeValue(value);
		context.putSync(keyBuffer, valueBuffer, getTxnId(options));
	}

	removeSync(context: NativeDatabase | NativeTransaction, key: Key, options?: DBITransactional) {
		if (!this.db.opened) {
			throw new Error('Database not open');
		}

		for (const { key: sortKey } of this.#getSortKeys(context, key, options)) {
			context.removeSync(sortKey, getTxnId(options));
		}

		context.removeSync(
			this.encodeKey(key),
			getTxnId(options)
		);
	}
}
