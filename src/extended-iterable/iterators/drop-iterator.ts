import { BaseIterator, DONE } from './base-iterator.js';

export class DropIterator<T> extends BaseIterator<T> {
	#count: number;
	#doneSkipping = false;
	#itemsDropped = 0;

	constructor(
		count: number,
		iterator: Iterator<T> | AsyncIterator<T>
	) {
		super(iterator);

		this.#count = count;

		if (typeof count !== 'number') {
			super.throw(new TypeError('Count is not a number'));
		}
		if (count < 0) {
			super.throw(new RangeError('Count must be a positive number'));
		}
	}

	next(): IteratorResult<T> | Promise<IteratorResult<T>> | any {
		if (this.finished) {
			return DONE;
		}

		if (this.#doneSkipping) {
			const result = super.next();
			if (result instanceof Promise) {
				return result
					.then(r => this.#processResult(r))
					.catch(err => super.throw(err));
			}
			return this.#processResult(result);
		}

		try {
			let result: IteratorResult<T> | Promise<IteratorResult<T>>;
			do {
				result = super.next();

				if (result instanceof Promise) {
					return this.#asyncSkip(result)
						.catch(err => super.throw(err));
				}

			} while (this.#itemsDropped++ < this.#count && !result.done);

			this.#doneSkipping = true;
			return this.#processResult(result);
		} catch (err) {
			return super.throw(err);
		}
	}

	async #asyncSkip(result: Promise<IteratorResult<T>>): Promise<IteratorResult<T>> {
		let currentResult = await result;

		// continue skipping if needed
		while (!currentResult.done && this.#itemsDropped < this.#count) {
			this.#itemsDropped++;
			currentResult = await super.next();
		}

		this.#doneSkipping = true;
		return this.#processResult(currentResult);
	}

	#processResult(result: IteratorResult<T>): IteratorResult<T> | Promise<IteratorResult<T>> {
		if (result.done) {
			return result;
		}
		return {
			done: false,
			value: result.value
		};
	}
};
