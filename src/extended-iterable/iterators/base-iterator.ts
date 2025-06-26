export const DONE = { done: true, value: undefined };

const KEEP_ALIVE = Symbol('keepAlive');

/**
 * The base class for iterators.
 */
export class BaseIterator<T> implements Iterator<T>, AsyncIterator<T> {
	protected finished = false;
	protected iterator: Iterator<T> | AsyncIterator<T>;

	constructor(iterator: Iterator<T> | AsyncIterator<T>, keepAlive = false) {
		this.iterator = iterator;
		if (keepAlive) {
			this[KEEP_ALIVE] = true;
			iterator[KEEP_ALIVE] = true;
		}
	}

	next(): IteratorResult<T> | Promise<IteratorResult<T>> | any {
		return this.iterator.next();
	}

	return(value?: T): IteratorResult<T> | Promise<IteratorResult<T>> | any {
		if (!this[KEEP_ALIVE] && !this.iterator[KEEP_ALIVE]) {
			this.finished = true;
			if (this.iterator.return) {
				this.iterator.return(value);
			}
		}
		return { done: true, value };
	}

	throw(err: unknown): IteratorResult<T> | Promise<IteratorResult<T>> | any {
		if (!this[KEEP_ALIVE] && !this.iterator[KEEP_ALIVE] && this.iterator.throw) {
			return this.iterator.throw(err);
		}
		throw err;
	}
}
