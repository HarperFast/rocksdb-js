import { BaseIterator, DONE } from './base-iterator.js';

export class FlatMapIterator<T, U> extends BaseIterator<T> {
	#index = 0;
	#currentSubIterator: Iterator<U> | null = null;
	#callback: (value: T, index: number) => U | U[] | Iterable<U> | Promise<U | U[] | Iterable<U>>;

	constructor(
		iterator: Iterator<T> | AsyncIterator<T>,
		callback: (value: T, index: number) => U | U[] | Iterable<U> | Promise<U | U[] | Iterable<U>>
	) {
		super(iterator);
		if (typeof callback !== 'function') {
			super.throw(new TypeError('Callback is not a function'));
		}
		this.#callback = callback;
	}

	next(): IteratorResult<U> | Promise<IteratorResult<U>> | any {
		if (this.finished) {
			return DONE;
		}

		// try to get value from current sub-iterator first
		if (this.#currentSubIterator) {
			const subResult = this.#currentSubIterator.next();
			if (!subResult.done) {
				return subResult;
			}
			this.#currentSubIterator = null;
		}

		try {
			const result = this.#getNextFromMain();
			if (result instanceof Promise) {
				return result.catch(err => super.throw(err));
			}
			return result;
		} catch (err) {
			return super.throw(err);
		}
	}

	#getNextFromMain(): IteratorResult<U> | Promise<IteratorResult<U>> {
		const mainResult = super.next();
		if (mainResult instanceof Promise) {
			return mainResult.then(result => this.#processMainResult(result));
		}

		return this.#processMainResult(mainResult);
	}

	#processMainResult(result: IteratorResult<T>): IteratorResult<U> | Promise<IteratorResult<U>> {
		if (result.done) {
			return { done: true, value: undefined as any };
		}

		const callbackResult = this.#callback(result.value, this.#index++);

		// handle promise from callback
		if (callbackResult instanceof Promise) {
			return callbackResult
				.then(resolvedResult => this.#processCallbackResult(resolvedResult));
		}

		return this.#processCallbackResult(callbackResult);
	}

	#processCallbackResult(callbackResult: U | U[] | Iterable<U>): IteratorResult<U> | Promise<IteratorResult<U>> {
		// create sub-iterator if callback result is iterable
		if (
			Array.isArray(callbackResult) ||
			typeof callbackResult?.[Symbol.iterator] === 'function'
		) {
			this.#currentSubIterator = callbackResult[Symbol.iterator]();
			if (this.#currentSubIterator) {
				const subResult = this.#currentSubIterator.next();
				if (!subResult.done) {
					return subResult;
				}
				this.#currentSubIterator = null;
			}
			return this.#getNextFromMain();
		}

		// not iterable, return as single value
		return {
			done: false,
			value: callbackResult as U
		};
	}
}
