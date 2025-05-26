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
