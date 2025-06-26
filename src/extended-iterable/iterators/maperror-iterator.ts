import { BaseIterator } from './base-iterator.js';

export class MapErrorIterator<T> extends BaseIterator<T> {
	#catchCallback?: (error: Error | unknown) => Error | unknown | Promise<Error | unknown>;

	constructor(
		iterator: Iterator<T> | AsyncIterator<T>,
		catchCallback?: (error: Error | unknown) => Error | unknown | Promise<Error | unknown>
	) {
		super(iterator, true);
		if (catchCallback && typeof catchCallback !== 'function') {
			super.throw(new TypeError('Callback is not a function'));
		}
		this.#catchCallback = catchCallback;
	}

	next(): IteratorResult<T | Error> | Promise<IteratorResult<T | Error>> | any {
		try {
			const result = super.next();

			// async handling
			if (result instanceof Promise) {
				return this.#asyncNext(result);
			}

			// sync handling
			if (result.done) {
				return result;
			}

			return {
				done: false,
				value: result.value
			};
		} catch (error: unknown) {
			// handle sync errors - return error as value and continue on next call
			if (this.#catchCallback) {
				const err = this.#catchCallback(error);
				// if catchCallback returns a promise, switch to async handling
				if (err instanceof Promise) {
					return err.then(resolvedErr => ({
						done: false,
						value: resolvedErr
					}));
				}
				return {
					done: false,
					value: err ?? error
				};
			}
			return {
				done: false,
				value: error
			};
		}
	}

	async #asyncNext(result: Promise<IteratorResult<T>>): Promise<IteratorResult<T | Error>> {
		try {
			const currentResult = await result;

			if (currentResult.done) {
				return currentResult as IteratorResult<T | Error>;
			}

			return {
				done: false,
				value: currentResult.value
			};
		} catch (error) {
			// handle async errors - return error as value
			if (this.#catchCallback) {
				const err = this.#catchCallback(error);
				// await the result if it's a promise
				const resolvedErr = err instanceof Promise ? await err : err;
				return {
					done: false,
					value: resolvedErr ?? error
				};
			}
			return {
				done: false,
				value: error as any
			};
		}
	}
}
