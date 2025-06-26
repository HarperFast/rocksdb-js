import { BaseIterator, DONE } from './base-iterator.js';
import { resolveIterator } from './resolve-iterator.js';
import type { IterableLike } from '../types.js';

export class ConcatIterator<T> extends BaseIterator<T> {
	#firstIteratorDone = false;
	#secondIterator!: Iterator<T> | AsyncIterator<T>;

	constructor(
		secondIterable: IterableLike<T>,
		iterator: Iterator<T> | AsyncIterator<T>
	) {
		super(iterator);
		try {
			this.#secondIterator = resolveIterator(secondIterable);
		} catch (err) {
			super.throw(err);
		}
	}

	next(): IteratorResult<T> | Promise<IteratorResult<T>> | any {
		if (this.finished) {
			return DONE;
		}

		// iterate through the original iterator first
		if (!this.#firstIteratorDone) {
			let result: IteratorResult<T> | Promise<IteratorResult<T>>;
			try {
				result = super.next();
			} catch (err) {
				return super.throw(err);
			}
			if (result instanceof Promise) {
				return result
					.then(result => this.#processFirstResult(result))
					.catch(err => {
						if (this.iterator.throw) {
							return this.iterator.throw(err);
						}
						throw err;
					});
			}
			return this.#processFirstResult(result);
		}

		// then iterate through the second iterator
		return this.#getNextFromOther();
	}

	return(): IteratorResult<T> | Promise<IteratorResult<T>> | any {
		if (this.#secondIterator?.return) {
			const result = this.#secondIterator.return();
			if (result instanceof Promise) {
				return result.then(() => super.return());
			}
		}
		return super.return();
	}

	#processFirstResult(result: IteratorResult<T>): IteratorResult<T> | Promise<IteratorResult<T>> {
		if (result.done) {
			this.#firstIteratorDone = true;
			return this.#getNextFromOther();
		}
		return this.#processResult(result);
	}

	#getNextFromOther(): IteratorResult<T> | Promise<IteratorResult<T>> {
		const result = this.#secondIterator.next();
		if (result instanceof Promise) {
			return result.then(result => this.#processResult(result));
		}
		return this.#processResult(result);
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
