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

	drop(_limit: number) {
		//
	}

	every(_callback: (value: T, index: number) => boolean) {
		//
	}

	filter(_callback: (value: T, index: number) => boolean) {
		//
	}

	find(_callback: (value: T, index: number) => boolean) {
		//
	}

	flatMap(_callback: (value: T, index: number) => Iterator<T, unknown, undefined> | Iterable<T, unknown, undefined>) {
		//
	}

	forEach(_callback: (value: T, index: number) => void) {
		//
	}

	map<U>(_callback: (value: T, index: number) => U) {
		//
	}

	reduce(_callback: (previousValue: T, currentValue: T, currentIndex: number) => T) {
		//
	}

	some(_callback: (value: T, index: number) => boolean) {
		//
	}

	take(_limit: number) {
		//
	}

	toArray() {
		//
	}
}
