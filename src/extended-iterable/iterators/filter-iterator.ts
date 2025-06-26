import { BaseIterator, DONE } from './base-iterator.js';

export class FilterIterator<T> extends BaseIterator<T> {
	#index = 0;
	#callback: (value: T, index: number) => boolean | Promise<boolean>;

	constructor(
		iterator: Iterator<T> | AsyncIterator<T>,
		callback: (value: T, index: number) => boolean | Promise<boolean>
	) {
		super(iterator);

		if (typeof callback !== 'function') {
			super.throw(new TypeError('Callback is not a function'));
		}

		this.#callback = callback;
	}

	next(): IteratorResult<T> | Promise<IteratorResult<T>> | any {
		if (this.finished) {
			return DONE;
		}

		try {
			let result = super.next();

			// async handling
			if (result instanceof Promise) {
				return this.#asyncFilter(result)
					.catch(err => super.throw(err));
			}

			// sync handling
			while (!result.done) {
				const keep = this.#callback(result.value, this.#index++);

				if (keep instanceof Promise) {
					return keep.then(keep => {
						if (keep) {
							return result;
						}
						return this.next();
					});
				}

				if (keep) {
					return result;
				}

				result = super.next();

				// handle sync-to-async transition
				if (result instanceof Promise) {
					return this.#asyncFilter(result);
				}
			}

			return result;
		} catch (err) {
			return super.throw(err);
		}
	}

	async #asyncFilter(result: Promise<IteratorResult<T>>): Promise<IteratorResult<T>> {
		let currentResult = await result;

		while (!currentResult.done) {
			const keep = await this.#callback(currentResult.value, this.#index++);
			if (keep) {
				return currentResult;
			}
			currentResult = await super.next();
		}

		return currentResult;
	}
}
