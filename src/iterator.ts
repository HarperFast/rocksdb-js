import { NativeIterator } from './load-binding';
import { Store } from './store';
import type { Key } from './encoding';

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

export interface IteratorResultValue<T> {
	key: Key;
	value: T;
}

export class BaseIterator<T> implements Iterator<IteratorResultValue<T>> {
	async = false;
	iterator: NativeIterator<IteratorResultValue<T>>;
	store: Store;

	constructor(iterator: NativeIterator<IteratorResultValue<T>>, store: Store) {
		this.iterator = iterator;
		this.store = store;
	}

	next(...[_value]: [] | [any]): IteratorResult<IteratorResultValue<T>> {
		const result = this.iterator.next() as IteratorResult<IteratorResultValue<T>>;
		if (result.done) {
			return result;
		}

		const key = this.store.decodeKey(result.value.key as Buffer);
		const value = this.store.decodeValue(result.value.value as Buffer);

		return {
			value: { key, value }
		};
	}
}

/**
 * An iterable that queries a range of keys.
 */
export class RangeIterable<T> {
	#iterator: BaseIterator<T>;

	constructor(iterator: BaseIterator<T>) {
		this.#iterator = iterator;
	}

	[Symbol.iterator]() {
		return this.#iterator;
	}

	[Symbol.asyncIterator]() {
		this.#iterator.async = true;
		return this.#iterator;
	}

	/**
	 * Collects the iterator results in an array and returns it.
	 */
	get asArray() {
		let result: IteratorResultValue<T>[] | undefined;

		const promise = new Promise<IteratorResultValue<T>[]>((resolve, reject) => {
			const iterator = this.#iterator;

			const array: IteratorResultValue<T>[] = [];
			(function next(iResult) {
				while (iResult.done !== true) {
					if (iResult instanceof Promise) {
						return iResult.then(next);
					}
					array.push(iResult.value);
					iResult = iterator.next();
				}
				resolve(result = array);
			}(iterator.next()));
		});

		return result || promise;
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

	flatMap(_callback: (value: T, index: number) => T) {
		return new RangeIterable(this.#iterator);
	}

	forEach(_callback: (value: T, index: number) => void) {
		// TODO
	}

	/**
	 * Returns a new iterable with the results of calling a callback function.
	 */
	map(callback: (value: IteratorResultValue<T>, index: number) => IteratorResultValue<T>) {
		class MapIterator extends BaseIterator<T> {
			i = 0;

			next(...[_value]: [] | [any]): IteratorResult<IteratorResultValue<T>> {
				const iResult = super.next();
				if (iResult.done) {
					return iResult;
				}

				const result = callback(iResult.value, this.i++);
				// if (result instanceof Promise) {
				// 	return result.then(this.next.bind(this));
				// }
				return {
					value: result,
					done: false
				};
			}
		}

		return new RangeIterable(new MapIterator(this.#iterator.iterator, this.#iterator.store));
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
