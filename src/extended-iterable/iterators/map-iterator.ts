import { BaseIterator, DONE } from './base-iterator.js';

export class MapIterator<T, U> extends BaseIterator<T> {
	#index = 0;
	#callback: (value: T, index: number) => U | Promise<U>;

	constructor(
		iterator: Iterator<T> | AsyncIterator<T>,
		callback: (value: T, index: number) => U | Promise<U>
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

		try {
			const result = super.next();

			// async handling
			if (result instanceof Promise) {
				return this.#asyncMap(result)
					.catch(err => super.throw(err));
			}

			// sync handling
			if (result.done) {
				return result;
			}

			const value = this.#callback(result.value, this.#index++);
			if (value instanceof Promise) {
				return value
					.then(value => ({
						done: false,
						value
					}))
					.catch(err => super.throw(err));
			}

			return {
				done: false,
				value
			};
		} catch (err) {
			return super.throw(err);
		}
	}

	async #asyncMap(result: Promise<IteratorResult<T>>): Promise<IteratorResult<U>> {
		const currentResult = await result;
		if (currentResult.done) {
			return currentResult;
		}

		const value = this.#callback(currentResult.value, this.#index++);
		if (value instanceof Promise) {
			return value.then(value => ({
				done: false,
				value
			}));
		}

		return {
			done: false,
			value
		};
	}
}
