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

class IteratorWrapper extends NativeIterator {
	constructor(context: NativeDatabase | NativeTransaction, options?: IteratorOptions) {
		super(context, options);
	}

	next() {
		const result = super.next();
		if (result.done) {
			return result;
		}

		// TODO: decode the key and value?

		return {
			key: result.value.key,
			value: result.value.value,
		};
	}
}

/**
 * An iterable that queries a range of keys.
 */
export class RangeIterator<T> {
	#context: NativeDatabase | NativeTransaction;
	#options: IteratorOptions;

	constructor(context: NativeDatabase | NativeTransaction, options?: IteratorOptions) {
		this.#context = context;
		this.#options = options ?? {};
	}

	[Symbol.iterator]() {
		return new IteratorWrapper(this.#context, this.#options);
	}

	[Symbol.asyncIterator]() {
		return new IteratorWrapper(this.#context, this.#options);
	}

	get asArray() {
		// TODO
		return [];
	}

	at(_index: number) {
		// TODO
	}

	concat() {
		const iter = new RangeIterator(this.#context, this.#options);
		// TODO
		return iter;
	}

	drop(_limit: number) {
		const iter = new RangeIterator(this.#context, this.#options);
		// TODO
		return iter;
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

	flatMap(_callback: (value: T, index: number) => Iterator<T, unknown, undefined> | Iterable<T, unknown, undefined>) {
		const iter = new RangeIterator(this.#context, this.#options);
		// TODO
		return iter;
	}

	forEach(_callback: (value: T, index: number) => void) {
		// TODO
	}

	map<U>(_callback: (value: T, index: number) => U) {
		const iter = new RangeIterator(this.#context, this.#options);
		// TODO
		return iter;
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
		const iter = new RangeIterator(this.#context, this.#options);
		// TODO
		return iter;
	}
}
