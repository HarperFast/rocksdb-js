import type { IteratorOptions } from './dbi.js';
import type { Key } from './encoding.js';
import type { Store } from './store.js';

export interface DBIteratorValue<T> {
	key: Key;
	value: T;
};

/**
 * Wraps an iterator, namely the `NativeIterator` class, and decodes the key
 * and value.
 */
export class DBIterator<T> implements Iterator<DBIteratorValue<T>> {
	iterator: Iterator<DBIteratorValue<T>>;
	store: Store;
	#includeValues: boolean;

	constructor(
		iterator: Iterator<DBIteratorValue<T>>,
		store: Store,
		options?: IteratorOptions & T
	) {
		this.iterator = iterator;
		this.store = store;
		this.#includeValues = options?.values ?? true;
	}

	[Symbol.iterator](): Iterator<DBIteratorValue<T>> {
		return this;
	}

	next(...[_value]: [] | [any]): IteratorResult<DBIteratorValue<T>> {
		const result = this.iterator.next();
		if (result.done) {
			return result;
		}

		const value: Partial<DBIteratorValue<T>> = {};
		value.key = this.store.decodeKey(result.value.key as Buffer);
		if (this.#includeValues) {
			value.value = this.store.decodeValue(result.value.value as Buffer);
		}

		return {
			done: false,
			value: value as DBIteratorValue<T>
		};
	}

	return(value?: any): IteratorResult<DBIteratorValue<T>, any> {
		if (this.iterator.return) {
			return this.iterator.return(value);
		}
		return { done: true, value };
	}

	throw(err: unknown): IteratorResult<DBIteratorValue<T>, any> {
		if (this.iterator.throw) {
			return this.iterator.throw(err);
		}
		throw err;
	}
}
