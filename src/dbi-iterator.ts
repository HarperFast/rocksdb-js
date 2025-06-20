import type { IteratorOptions } from './dbi.js';
import type { Key } from './encoding.js';
import type { Store } from './store.js';

export type DBIteratorValue<T> = {
	key: Key;
	value: T;
};

export type DBIteratorSortKey = {
	sortKey: Buffer;
};

/**
 * Wraps the `NativeIterator` class, and decodes the key and value.
 */
export class DBIterator<T> implements Iterator<DBIteratorValue<T>> {
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
	 * ?
	 */
	#sortKey: Key | undefined;

	/**
	 * The store instance used for decoding keys/values and determining if the
	 * iterator is in dupSort mode.
	 */
	#store: Store;

	constructor(
		iterator: Iterator<DBIteratorValue<T>>,
		store: Store,
		options?: IteratorOptions & T
	) {
		this.#includeValues = options?.values ?? true;
		this.#iterator = iterator;
		this.#store = store;

		if ('dupSort' in store && store.dupSort) {
			this.#dupSortMode = true;
			this.#sortKey = options?.sortKey;
		}
	}

	next(...[_value]: [] | [any]): IteratorResult<DBIteratorValue<T>> {
		let result = this.#iterator.next();
		while (!result.done) {
			const resultValue = result.value;

			if (this.#dupSortMode && this.#sortKey !== undefined) {
				// In dupSort mode with sortKey option, return the sortKey as the key
				return {
					done: false,
					value: {
						key: resultValue.key as Buffer,
						value: undefined as T
					}
				};
			}

			let key = this.#store.decodeKey(resultValue.key as Buffer);
			let value = resultValue.value;
			let sortKey: Buffer | undefined;

			if (this.#dupSortMode) {
				if (!Array.isArray(key)) {
					result = this.#iterator.next();
					continue;
				}

				const [primaryKey, indexedValue] = key;

				// double check the key matches
				// if (this.#sortKey !== undefined && this.#sortKey !== primaryKey) {
				// 	result = this.#iterator.next();
				// 	continue;
				// }

				key = primaryKey;
				if (this.#includeValues) {
					value = indexedValue as T;
				}
			} else if (this.#includeValues) {
				value = this.#store.decodeValue(value as Buffer);
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
