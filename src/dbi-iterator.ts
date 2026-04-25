import type { BufferWithDataView, Key } from './encoding.js';
import { constants, type NativeIteratorCls, type NativeIteratorResult } from './load-binding.js';
import { ITERATOR_STATE, KEY_BUFFER, type Store, VALUE_BUFFER } from './store.js';

const { ITERATOR_RESULT_DONE, ITERATOR_RESULT_FAST } = constants;

export interface DBIteratorValue<T> {
	key: Key;
	value: T;
}

const DONE_RESULT: IteratorResult<DBIteratorValue<unknown>> = Object.freeze({
	done: true,
	value: undefined as unknown,
}) as IteratorResult<DBIteratorValue<unknown>>;

/**
 * Wraps the `NativeIterator` C++ binding, decoding keys and values from the
 * shared key/value buffers (fast path) or from per-iteration buffers (slow
 * fallback path used for oversized data or stable-buffer decoders).
 *
 * The native `next()` returns a primitive signal (and writes lengths to the
 * shared `ITERATOR_STATE` buffer) instead of constructing a JS result object,
 * so this class is responsible for building the `IteratorResult`.
 */
export class DBIterator<T> implements Iterator<DBIteratorValue<T>> {
	iterator: InstanceType<typeof NativeIteratorCls>;
	store: Store;
	#includeValues: boolean;

	constructor(
		iterator: InstanceType<typeof NativeIteratorCls>,
		store: Store,
		includeValues: boolean
	) {
		this.iterator = iterator;
		this.store = store;
		this.#includeValues = includeValues;
	}

	[Symbol.iterator](): Iterator<DBIteratorValue<T>> {
		return this;
	}

	next(): IteratorResult<DBIteratorValue<T>> {
		const result: NativeIteratorResult = this.iterator.next();

		if (result === ITERATOR_RESULT_DONE) {
			return DONE_RESULT as IteratorResult<DBIteratorValue<T>>;
		}

		const includeValues = this.#includeValues;
		const value: Partial<DBIteratorValue<T>> = {};

		if (result === ITERATOR_RESULT_FAST) {
			// Fast path: key bytes are in the shared KEY_BUFFER, value bytes
			// are in the shared VALUE_BUFFER, and lengths are in ITERATOR_STATE.
			value.key = this.store.readKey(KEY_BUFFER, 0, ITERATOR_STATE[0]);
			if (includeValues) {
				VALUE_BUFFER.end = ITERATOR_STATE[1];
				value.value = this.store.decodeValue(VALUE_BUFFER) as T;
			}
		} else {
			// Slow path: data didn't fit in shared buffers (or decoder needs
			// stable buffer). The native side allocated fresh buffers for us.
			const slow = result as { key: Buffer; value?: Buffer };
			value.key = this.store.decodeKey(slow.key);
			if (includeValues && slow.value !== undefined) {
				value.value = this.store.decodeValue(slow.value as BufferWithDataView) as T;
			}
		}

		return { done: false, value: value as DBIteratorValue<T> };
	}

	return(value?: any): IteratorResult<DBIteratorValue<T>, any> {
		if (this.iterator.return) {
			this.iterator.return();
		}
		return { done: true, value };
	}

	throw(err: unknown): IteratorResult<DBIteratorValue<T>, any> {
		if (this.iterator.throw) {
			this.iterator.throw(err);
		}
		throw err;
	}
}
