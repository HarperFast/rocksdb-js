import type { IteratorOptions } from './dbi.js';
import type { Key } from './encoding.js';
import type { Store } from './store.js';

export const BOUNDARY = 0xFFFFFFFF;

export type DBIteratorValue<T> = {
	key: Key;
	value: T;
};

/**
 * Wraps the `NativeIterator` class, and decodes the key and value.
 */
export class DBIterator<T> implements Iterator<DBIteratorValue<T> | T> {
	/**
	 * When true, encodes the key with both the primary key and the indexed value.
	 */
	#dupSortMode = false;

	/**
	 * When true, includes the decoded values in the result.
	 */
	#includeValues: boolean;

	/**
	 * The native iterator.
	 */
	#iterator: Iterator<DBIteratorValue<T>>;

	/**
	 * When true, the iterator will iterate return the raw undecoded key.
	 */
	#sortKeyOnly = false;

	/**
	 * The store instance used for decoding keys/values and determining if the
	 * iterator is in dupSort mode.
	 */
	#store: Store;

	/**
	 * When `true`, the iterator will return the values only.
	 */
	#valuesOnly?: boolean;

	constructor(
		iterator: Iterator<DBIteratorValue<T>>,
		store: Store,
		options?: IteratorOptions & T & { sortKeyOnly?: boolean }
	) {
		this.#dupSortMode = !!('dupSort' in store && store.dupSort);
		this.#includeValues = options?.values ?? true;
		this.#iterator = iterator;
		this.#sortKeyOnly = !!options?.sortKeyOnly;
		this.#store = store;
		this.#valuesOnly = options?.valuesOnly ?? false;

		if (!this.#includeValues && this.#valuesOnly) {
			throw new Error('valuesOnly cannot be true when values is false');
		}
	}

	next(...[_value]: [] | [any]): IteratorResult<DBIteratorValue<T> | T> {
		let result = this.#iterator.next();
		while (!result.done) {
			const resultValue = result.value;

			if (this.#dupSortMode && this.#sortKeyOnly) {
				// in dupSort mode with sortKey option, return the sortKey as the key
				return {
					done: false,
					value: {
						key: resultValue.key as Buffer,
						value: resultValue.value as T
					}
				};
			}

			let key = this.#store.decodeKey(resultValue.key as Buffer);
			let value = resultValue.value;

			if (this.#dupSortMode) {
				const boundary = Array.isArray(key) ? key.indexOf(BOUNDARY) : -1;
				if (!Array.isArray(key) || boundary === -1) {
					result = this.#iterator.next();
					continue;
				}

				const indexedValue = key[boundary + 1];
				key = boundary > 1 ? key.slice(0, boundary) : key[0];

				if (this.#includeValues) {
					value = indexedValue as T;
				}
			} else if (this.#includeValues) {
				value = this.#store.decodeValue(value as Buffer);
			}

			if (this.#valuesOnly) {
				return {
					done: false,
					value
				};
			}

			return {
				done: false,
				value: {
					key,
					value
				}
			};
		}

		return result;
	}

	return(value?: any): IteratorResult<DBIteratorValue<T>, any> {
		if (this.#iterator.return) {
			return this.#iterator.return(value);
		}
		return { done: true, value };
	}

	throw(err: unknown): IteratorResult<DBIteratorValue<T>, any> {
		if (this.#iterator.throw) {
			return this.#iterator.throw(err);
		}
		throw err;
	}
}
