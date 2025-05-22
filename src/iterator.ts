import type { Key } from './encoding';
import { NativeDatabase, NativeIterator, NativeTransaction } from './load-binding';

export interface IteratorOptions {
	decoder?: (value: any) => any,
	end?: Key | Uint8Array;
	exactMatch?: boolean;
	exclusiveStart?: boolean;
	inclusiveEnd?: boolean;
	limit?: number;
	key?: Key;
	offset?: number;
	onlyCount?: boolean;
	reverse?: boolean;
	snapshot?: boolean;
	start?: Key | Uint8Array;
	values?: boolean;
	valuesForKey?: boolean;
	versions?: boolean;
};

export interface NativeIteratorOptions extends IteratorOptions {
	context: NativeDatabase | NativeTransaction;
};

export type IteratorYieldResult<T> = {
	done?: false;
	value: {
		key: Key;
		value: T;
	};
};

export type IteratorReturnResult<T> = {
	done: true;
	value: {
		key: Key;
		value: T;
	};
};

export type IteratorResult<T, TReturn = any> = IteratorYieldResult<T> | IteratorReturnResult<TReturn>;

export class Iterator<T, TReturn = any, TNext = any> extends NativeIterator {
	async = false;

	constructor(options: NativeIteratorOptions) {
		super(options);
	}

	next(...[_value]: [] | [TNext]): IteratorResult<T, TReturn> {
		const result = super.next();
		if (result.done) {
			return result;
		}

		// TODO: decode the key and value

		return {
			value: {
				key: result.value.key,
				value: result.value.value,
			}
		};
	}
}

/**
 * An iterable that queries a range of keys.
 */
export class RangeIterable<T> {
	#iterator: Iterator<T>;

	constructor(iterator: Iterator<T>) {
		this.#iterator = iterator;
	}

	[Symbol.iterator]() {
		return this.#iterator;
	}

	[Symbol.asyncIterator]() {
		this.#iterator.async = true;
		return this.#iterator;
	}

	get asArray() {
		// TODO
		return [];
	}

	at(_index: number) {
		// TODO
	}

	concat() {
		return new RangeIterable(this.#iterator);
	}

	drop(_limit: number) {
		return new RangeIterable(this.#iterator);
	}

	every(_callback: (value: T, index: number) => boolean) {
		// TODO
	}

	filter(_callback: (value: T, index: number) => boolean) {
		// TODO
	}

	find(_callback: (value: T, index: number) => boolean) {
		// TODO
	}

	flatMap<U>(_callback: (value: T, index: number) => U) {
		return new RangeIterable(this.#iterator);
	}

	forEach(_callback: (value: T, index: number) => void) {
		// TODO
	}

	map<U>(_callback: (value: T, index: number) => U) {
		return new RangeIterable(this.#iterator);
	}

	mapError(_callback: (error: Error) => Error) {
		// TODO
	}

	reduce(_callback: (previousValue: T, currentValue: T, currentIndex: number) => T) {
		// TODO
	}

	slice(_start: number, _end: number) {
		// TODO
	}

	some(_callback: (value: T, index: number) => boolean) {
		// TODO
	}

	take(_limit: number) {
		return new RangeIterable(this.#iterator);
	}
}
