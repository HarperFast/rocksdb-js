import { BaseIterator, DONE } from './base-iterator.js';

export class TakeIterator<T> extends BaseIterator<T> {
	#count = 0;
	#limit: number;

	constructor(
		limit: number,
		iterator: Iterator<T> | AsyncIterator<T>
	) {
		super(iterator);

		this.#limit = limit;

		if (typeof limit !== 'number') {
			super.throw(new TypeError('Limit is not a number'));
		}
		if (limit < 0) {
			super.throw(new RangeError('Limit must be a positive number'));
		}
	}

	next(): IteratorResult<T> | Promise<IteratorResult<T>> | any {
		if (this.finished) {
			return DONE;
		}

		if (this.#limit === 0 || this.#count >= this.#limit) {
			return super.return();
		}

		let result: IteratorResult<T> | Promise<IteratorResult<T>>;

		try {
			result = super.next();
		} catch (err) {
			return super.throw(err);
		}

		// async handling
		if (result instanceof Promise) {
			return this.#asyncNext(result)
				.catch(err => super.throw(err));
		}

		// sync handling
		if (result.done) {
			return result;
		}

		this.#count++;

		return {
			done: false,
			value: result.value
		};
	}

	async #asyncNext(result: Promise<IteratorResult<T>>): Promise<IteratorResult<T>> {
		const currentResult = await result;

		if (currentResult.done) {
			return currentResult;
		}

		this.#count++;

		return {
			done: false,
			value: currentResult.value
		};
	}
};
