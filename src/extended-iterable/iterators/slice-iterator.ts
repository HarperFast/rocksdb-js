import { BaseIterator, DONE } from './base-iterator.js';

export class SliceIterator<T> extends BaseIterator<T> {
	#index = 0;
	#yielding = false;
	#startIndex: number;
	#endIndex?: number;

	constructor(
		iterator: Iterator<T> | AsyncIterator<T>,
		startIndex?: number,
		endIndex?: number
	) {
		super(iterator);

		if (startIndex !== undefined) {
			if (typeof startIndex !== 'number') {
				super.throw(new TypeError('Start is not a number'));
			}
			if (startIndex < 0) {
				super.throw(new RangeError('Start must be a positive number'));
			}
		}
		if (endIndex !== undefined) {
			if (typeof endIndex !== 'number') {
				super.throw(new TypeError('End is not a number'));
			}
			if (endIndex < 0) {
				super.throw(new RangeError('End must be a positive number'));
			}
		}

		this.#startIndex = startIndex ?? 0;
		this.#endIndex = endIndex;
	}

	next(): IteratorResult<T> | Promise<IteratorResult<T>> | any {
		if (this.finished) {
			return DONE;
		}

		// check if we can return early
		if (this.#endIndex !== undefined && this.#startIndex >= this.#endIndex) {
			return super.return();
		}

		// skip to start if not already yielding
		if (!this.#yielding) {
			return this.#skipToStart();
		}

		// check if we've reached the end
		if (this.#endIndex !== undefined && this.#index >= this.#endIndex) {
			return super.return();
		}

		return this.#getNextValue();
	}

	#skipToStart(): IteratorResult<T> | Promise<IteratorResult<T>> {
		// skip elements synchronously if possible
		while (this.#index < this.#startIndex) {
			let result: IteratorResult<T> | Promise<IteratorResult<T>>;
			try {
				result = this.iterator.next();
			} catch (err) {
				return super.throw(err);
			}

			if (result instanceof Promise) {
				return this.#asyncSkipAndGetFirst(result);
			}

			if (result.done) {
				return result;
			}
			this.#index++;
		}

		this.#yielding = true;
		return this.#getNextValue();
	}

	async #asyncSkipAndGetFirst(result: Promise<IteratorResult<T>>): Promise<IteratorResult<T>> {
		let currentResult = await result;

		// continue skipping elements
		while (!currentResult.done && this.#index < this.#startIndex) {
			this.#index++;
			currentResult = await super.next();
		}

		this.#yielding = true;

		// if iterator is done or we've reached the end index, return done
		if (currentResult.done) {
			return currentResult;
		}

		// process the current result
		this.#index++;
		return {
			done: false,
			value: currentResult.value
		};
	}

	#getNextValue(): IteratorResult<T> | Promise<IteratorResult<T>> {
		let result: IteratorResult<T> | Promise<IteratorResult<T>>;
		try {
			result = super.next();
		} catch (err) {
			return super.throw(err);
		}

		if (result instanceof Promise) {
			// async path
			return result.then(currentValue => {
				if (currentValue.done) {
					return currentValue;
				}
				this.#index++;
				return {
					done: false,
					value: currentValue.value
				};
			});
		}

		// sync path
		if (result.done) {
			return result;
		}
		this.#index++;
		return {
			done: false,
			value: result.value
		};
	}
}
