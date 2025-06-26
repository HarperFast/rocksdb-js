import { ConcatIterator } from './iterators/concat-iterator.js';
import { DropIterator } from './iterators/drop-iterator.js';
import { FilterIterator } from './iterators/filter-iterator.js';
import { FlatMapIterator } from './iterators/flatmap-iterator.js';
import { MapErrorIterator } from './iterators/maperror-iterator.js';
import { MapIterator } from './iterators/map-iterator.js';
import { resolveIterator } from './iterators/resolve-iterator.js';
import { SliceIterator } from './iterators/slice-iterator.js';
import { TakeIterator } from './iterators/take-iterator.js';
import type { IterableLike } from './types.js';

/**
 * An iterable that provides a rich set of methods for working with ranges of
 * items.
 */
export class ExtendedIterable<T> {
	#iterator: (Iterator<T> | AsyncIterator<T>) & { async?: boolean };

	constructor(iterator: IterableLike<T>) {
		this.#iterator = resolveIterator(iterator);
	}

	/**
	 * Returns an iterator in synchronous mode.
	 *
	 * @returns The iterator.
	 *
	 * @example
	 * ```typescript
	 * const iterator = new ExtendedIterable([1, 2, 3]);
	 * for (const item of iterator) {
	 *   console.log(item);
	 * }
	 * ```
	 */
	[Symbol.iterator](): Iterator<T> {
		return this.#iterator as Iterator<T>;
	}

	/**
	 * Returns an iterator in asynchronous mode.
	 *
	 * @returns The iterator in async mode.
	 *
	 * @example
	 * ```typescript
	 * const iterator = new ExtendedIterable([1, 2, 3]);
	 * for await (const item of iterator) {
	 *   console.log(item);
	 * }
	 * ```
	 */
	[Symbol.asyncIterator](): AsyncIterator<T> {
		this.#iterator.async = true;
		return this.#iterator as AsyncIterator<T>;
	}

	/**
	 * Collects the iterator results in an array and returns it.
	 *
	 * @returns The iterator results as an array.
	 *
	 * @example
	 * ```typescript
	 * const iterator = new ExtendedIterable([1, 2, 3]);
	 * const array = iterator.asArray;
	 * ```
	 */
	get asArray(): Array<T> | Promise<Array<T>> {
		const iterator = this.#iterator;
		const array: T[] = [];

		const handleError = (err: unknown) => {
			iterator.throw?.(err);
			throw err;
		};

		let result: IteratorResult<T> | Promise<IteratorResult<T>>;

		try {
			result = iterator.next();
		} catch (err) {
			return handleError(err);
		}

		// if first result is async, switch to async mode immediately
		if (result instanceof Promise) {
			return this.#asyncAsArray(array, result);
		}

		// synchronous processing
		while (!result.done) {
			array.push(result.value);

			try {
				result = iterator.next();
			} catch (err) {
				return handleError(err);
			}

			// if iterator becomes async mid-iteration, switch to async mode
			if (result instanceof Promise) {
				return this.#asyncAsArray(array, result);
			}
		}

		return array;
	}

	/**
	 * Helper function to return the async iterator results as an array.
	 *
	 * @param array - The array to collect the results in.
	 * @param currentResult - The current result of the iterator.
	 * @returns The iterator results as an array.
	 */
	async #asyncAsArray(array: T[], currentResult: IteratorResult<T> | Promise<IteratorResult<T>>): Promise<Array<T>> {
		const iterator = this.#iterator;

		try {
			let result = await currentResult;

			while (!result.done) {
				array.push(result.value);
				result = await iterator.next();
			}

			return array;
		} catch (err) {
			if (iterator.throw) {
				const throwResult = iterator.throw(err);
				if (throwResult instanceof Promise) {
					return throwResult.then(() => { throw err; });
				}
			}
			throw err;
		}
	}

	/**
	 * Returns the item at the given index.
	 *
	 * @param index - The index of the item to return.
	 * @returns The item at the given index.
	 *
	 * @example
	 * ```typescript
	 * const iterator = new ExtendedIterable([1, 2, 3]);
	 * const item = iterator.at(0);
	 * ```
	 */
	at(index: number): T | Promise<T | undefined> | undefined {
		const iterator = this.#iterator;
		const handleError = (err: unknown) => {
			const throwResult = iterator.throw?.(err);
			if (throwResult instanceof Promise) {
				return throwResult.then(() => { throw err; });
			}
			throw err;
		};

		let currentIndex = 0;
		let result: IteratorResult<T> | Promise<IteratorResult<T>>;

		try {
			if (typeof index !== 'number') {
				throw new TypeError('index is not a number');
			}
			if (index < 0) {
				throw new RangeError('index must be a positive number');
			}

			result = iterator.next();
		} catch (err) {
			return handleError(err);
		}

		// if first result is async, handle everything async
		if (result instanceof Promise) {
			return this.#asyncAt(result, index, currentIndex).catch(handleError);
		}

		// synchronous path
		while (!result.done) {
			if (currentIndex === index) {
				if (iterator.return) {
					return (iterator.return(result.value) as IteratorReturnResult<T>).value;
				}
				return result.value;
			}

			currentIndex++;

			try {
				result = iterator.next();
			} catch (err) {
				return handleError(err);
			}

			// switch to async if needed
			if (result instanceof Promise) {
				return this.#asyncAt(result, index, currentIndex).catch(handleError);
			}
		}

		return result.value;
	}

	async #asyncAt(result: Promise<IteratorResult<T>>, index: number, currentIndex: number): Promise<T | undefined> {
		const iterator = this.#iterator;
		let currentResult = await result;

		while (!currentResult.done) {
			if (currentIndex === index) {
				if (iterator.return) {
					const returnResult = iterator.return(currentResult.value);
					if (returnResult instanceof Promise) {
						return returnResult.then(r => r.value);
					}
					return returnResult.value;
				}
				return currentResult.value;
			}
			currentIndex++;
			currentResult = await iterator.next();
		}
	}

	/**
	 * Concatenates the iterable with another iterable.
	 *
	 * @param other - The iterable to concatenate with.
	 * @returns The concatenated iterable.
	 *
	 * @example
	 * ```typescript
	 * const iterator = new ExtendedIterable([1, 2, 3]);
	 * const concatenated = iterator.concat([4, 5, 6]);
	 * ```
	 */
	concat(other: IterableLike<T>): ExtendedIterable<T> {
		return new ExtendedIterable<T>(
			new ConcatIterator<T>(other, this.#iterator)
		);
	}

	/**
	 * Returns a new iterable skipping the first `count` items.
	 *
	 * @param count - The number of items to skip.
	 * @returns The new iterable.
	 *
	 * @example
	 * ```typescript
	 * const iterator = new ExtendedIterable([1, 2, 3]);
	 * const items = iterator.drop(2);
	 * ```
	 */
	drop(count: number): ExtendedIterable<T> {
		return new ExtendedIterable(
			new DropIterator(count, this.#iterator)
		);
	}

	/**
	 * Returns `true` if the callback returns `true` for every item of the
	 * iterable.
	 *
	 * @param callback - The callback function to call for each result.
	 * @returns `true` if the callback returns `true` for every item of the
	 * iterable, `false` otherwise.
	 *
	 * @example
	 * ```typescript
	 * const iterator = new ExtendedIterable([1, 2, 3]);
	 * const isAllValid = iterator.every(item => item < 5);
	 * ```
	 */
	every(callback: (value: T, index: number) => boolean | Promise<boolean>): boolean | Promise<boolean> {
		const iterator = this.#iterator;
		const handleError = (err: unknown) => {
			const throwResult = iterator.throw?.(err);
			if (throwResult instanceof Promise) {
				return throwResult.then(() => { throw err; });
			}
			throw err;
		};

		try {
			if (typeof callback !== 'function') {
				throw new TypeError('Callback is not a function');
			}

			let result = iterator.next();
			let index = 0;

			// if first result is a promise, we know this is async
			if (result instanceof Promise) {
				return this.#asyncEvery(result, callback, index)
					.catch(handleError);
			}

			// sync path
			while (!result.done) {
				const rval = callback(result.value, index++);
				if (rval instanceof Promise) {
					return rval
						.then(rval => {
							if (!rval) {
								iterator.return?.();
								return false;
							}
							return this.#asyncEvery(iterator.next(), callback, index);
						})
						.catch(handleError);
				}

				if (!rval) {
					iterator.return?.();
					return false;
				}
				result = iterator.next() as IteratorResult<T>;

				// if we encounter a Promise mid-iteration, switch to async
				if (result instanceof Promise) {
					return this.#asyncEvery(result, callback, index)
						.catch(handleError);
				}
			}
		} catch (err) {
			return handleError(err);
		}

		return true;
	}

	/**
	 * Helper function to process the remaining async iterator results.
	 *
	 * @param result - The result of the iterator as a promise.
	 * @param callback - The callback function to call for each result.
	 * @param index - The current index of the iterator.
	 * @returns `true` if the callback returns `true` for every item of the
	 * iterable, `false` otherwise.
	 */
	async #asyncEvery(
		result: IteratorResult<T> | Promise<IteratorResult<T>>,
		callback: (value: T, index: number) => boolean | Promise<boolean>,
		index: number
	): Promise<boolean> {
		const iterator = this.#iterator;
		let currentResult = await result;

		while (!currentResult.done) {
			const rval = callback(currentResult.value, index++);

			if (rval instanceof Promise) {
				return rval.then(rval => {
					if (!rval) {
						iterator.return?.();
						return false;
					}
					return this.#asyncEvery(iterator.next(), callback, index);
				});
			}

			if (!rval) {
				iterator.return?.();
				return false;
			}
			currentResult = await iterator.next();
		}

		return true;
	}

	/**
	 * Returns a new iterable containing only the values for which the callback
	 * returns `true`.
	 *
	 * @param callback - The callback function to call for each result.
	 * @returns The new iterable.
	 *
	 * @example
	 * ```typescript
	 * const iterator = new ExtendedIterable([1, 2, 3]);
	 * const filtered = iterator.filter(item => item < 3);
	 * ```
	 */
	filter(callback: (value: T, index: number) => boolean | Promise<boolean>): ExtendedIterable<T> {
		return new ExtendedIterable(
			new FilterIterator(this.#iterator, callback)
		);
	}

	/**
	 * Returns the first item of the iterable for which the callback returns
	 * `true`.
	 *
	 * @param callback - The callback function to call for each result.
	 * @returns The first item of the iterable for which the callback returns
	 * `true`, or `undefined` if no such item is found.
	 *
	 * @example
	 * ```typescript
	 * const iterator = new ExtendedIterable([1, 2, 3]);
	 * const found = iterator.find(item => item === 2);
	 * ```
	 */
	find(callback: (value: T, index: number) => boolean | Promise<boolean>): T | Promise<T | undefined> | undefined {
		const iterator = this.#iterator;
		const handleError = (err: unknown) => {
			if (iterator.throw) {
				const throwResult = iterator.throw(err);
				if (throwResult instanceof Promise) {
					return throwResult.then(() => undefined);
				}
			}
			throw err;
		};

		try {
			if (typeof callback !== 'function') {
				throw new TypeError('Callback is not a function');
			}

			let index = 0;
			let result = iterator.next();

			// if first result is a promise, we know this is async
			if (result instanceof Promise) {
				return this.#asyncFind(result, callback, index)
					.catch(err => handleError(err));
			}

			// sync path
			while (!result.done) {
				const { value } = result;
				const callbackResult = callback(value, index++);
				if (callbackResult instanceof Promise) {
					return callbackResult
						.then(callbackResult => {
							if (callbackResult) {
								iterator.return?.();
								return value;
							}
							return this.#asyncFind(iterator.next(), callback, index);
						})
						.catch(err => handleError(err));
				}

				if (callbackResult) {
					iterator.return?.();
					return value;
				}
				result = iterator.next();

				// if we encounter a Promise mid-iteration, switch to async
				if (result instanceof Promise) {
					return this.#asyncFind(result, callback, index)
						.catch(err => handleError(err));
				}
			}
		} catch (err) {
			return handleError(err);
		}
	}

	async #asyncFind(
		result: IteratorResult<T> | Promise<IteratorResult<T>>,
		callback: (value: T, index: number) => boolean | Promise<boolean>,
		index: number
	): Promise<T | undefined> {
		const iterator = this.#iterator;
		let currentResult = await result;

		while (!currentResult.done) {
			const { value } = currentResult;

			if (await callback(value, index++)) {
				iterator.return?.();
				return value;
			}

			currentResult = await iterator.next();
		}
	}

	/**
	 * Returns a new iterable with the flattened results of a callback function.
	 *
	 * @param callback - The callback function to call for each result.
	 * @returns The new iterable with the values flattened.
	 *
	 * @example
	 * ```typescript
	 * const iterator = new ExtendedIterable([1, 2, 3]);
	 * const flattened = iterator.flatMap(item => [item, item]);
	 * ```
	 */
	flatMap<U>(callback: (value: T, index: number) => U | U[] | Iterable<U> | Promise<U | U[] | Iterable<U>>): ExtendedIterable<U> {
		return new ExtendedIterable(
			new FlatMapIterator<T, U>(this.#iterator, callback)
		);
	}

	/**
	 * Calls a function for each item of the iterable.
	 *
	 * @param callback - The callback function to call for each result.
	 *
	 * @example
	 * ```typescript
	 * const iterator = new ExtendedIterable([1, 2, 3]);
	 * iterator.forEach(item => console.log(item));
	 * ```
	 */
	forEach(callback: (value: T, index: number) => any | Promise<any>): void | Promise<void> {
		const iterator = this.#iterator;
		const handleError = (err: unknown) => {
			if (iterator.throw) {
				const throwResult = iterator.throw(err);
				if (throwResult instanceof Promise) {
					return throwResult.then(() => undefined);
				}
			}
			throw err;
		};

		try {
			if (typeof callback !== 'function') {
				throw new TypeError('Callback is not a function');
			}

			let index = 0;
			let result = iterator.next();

			// if first result is a promise, we know this is async
			if (result instanceof Promise) {
				return this.#asyncForEach(result, callback, index)
					.catch(err => handleError(err));
			}

			// sync path
			while (!result.done) {
				const rval = callback(result.value, index++);
				if (rval instanceof Promise) {
					return rval
						.then(() => this.#asyncForEach(iterator.next(), callback, index))
						.catch(err => handleError(err));
				}

				result = iterator.next();

				// if we encounter a Promise mid-iteration, switch to async
				if (result instanceof Promise) {
					return this.#asyncForEach(result, callback, index)
						.catch(err => handleError(err));
				}
			}
		} catch (err) {
			return handleError(err);
		}
	}

	/**
	 * Helper function to process the remaining async iterator results.
	 *
	 * @param result - The result of the iterator as a promise.
	 * @param callback - The callback function to call for each result.
	 * @param index - The current index of the iterator.
	 */
	async #asyncForEach(
		result: IteratorResult<T> | Promise<IteratorResult<T>>,
		callback: (value: T, index: number
	) => any | Promise<any>, index: number): Promise<void> {
		const iterator = this.#iterator;
		let currentResult = await result;

		while (!currentResult.done) {
			const rval = callback(currentResult.value, index++);
			if (rval instanceof Promise) {
				await rval;
			}
			currentResult = await iterator.next();
		}
	}

	/**
	 * Returns a new iterable with the results of calling a callback function.
	 *
	 * @param callback - The callback function to call for each result.
	 * @returns The new iterable.
	 *
	 * @example
	 * ```typescript
	 * const iterator = new ExtendedIterable([1, 2, 3]);
	 * const mapped = iterator.map(item => item * 2);
	 * ```
	 */
	map<U>(callback: (value: T, index: number) => U | Promise<U>): ExtendedIterable<U> {
		return new ExtendedIterable(
			new MapIterator(this.#iterator, callback)
		);
	}

	/**
	 * Catch errors thrown during iteration and allow iteration to continue.
	 *
	 * @param catchCallback - The callback to handle errors. The returned error is logged/handled but iteration continues.
	 * @returns The new iterable.
	 *
	 * @example
	 * ```typescript
	 * const iterator = new ExtendedIterable([1, 2, 3]);
	 * const mapped = iterator
	 *     .map(item => item * 2)
	 *     .mapError(error => new Error('Error: ' + error.message));
	 * ```
	 */
	mapError(catchCallback?: (error: Error | unknown) => Error | unknown | Promise<Error | unknown>): ExtendedIterable<T | Error> {
		return new ExtendedIterable(
			new MapErrorIterator(this.#iterator, catchCallback)
		);
	}

	/**
	 * Reduces the iterable to a single value.
	 *
	 * @param callback - The callback function to call for each result.
	 * @param initialValue - The initial value to use for the accumulator.
	 * @returns The reduced value.
	 *
	 * @example
	 * ```typescript
	 * const iterator = new ExtendedIterable([1, 2, 3]);
	 * const sum = iterator.reduce((acc, item) => acc + item, 0);
	 * ````
	 */
	reduce<U>(callback: (previousValue: U, currentValue: T, currentIndex: number) => U | Promise<U>, initialValue: U): U | Promise<U>;
	reduce(callback: (previousValue: T, currentValue: T, currentIndex: number) => T | Promise<T>): T | Promise<T>;
	reduce<U>(callback: (previousValue: U, currentValue: T, currentIndex: number) => U | Promise<U>, initialValue?: U): U | Promise<U> {
		const iterator = this.#iterator;
		let index = 0;
		let accumulator: U;
		const handleError = (err: unknown): U | Promise<U> => {
			if (iterator.throw) {
				const throwResult = iterator.throw(err);
				if (throwResult instanceof Promise) {
					return throwResult.then(() => { throw err; });
				}
			}
			throw err;
		};

		try {
			if (typeof callback !== 'function') {
				throw new TypeError('Callback is not a function');
			}

			// handle initial value setup
			if (arguments.length < 2) {
				const firstResult = iterator.next();

				// if first result is a promise, we know this is async
				if (firstResult instanceof Promise) {
					return this.#asyncReduce(firstResult, callback, undefined as any, index, false)
						.catch(err => handleError(err));
				}

				if (firstResult.done) {
					throw new TypeError('Reduce of empty iterable with no initial value');
				}

				accumulator = firstResult.value as unknown as U;
				index = 1;
			} else {
				accumulator = initialValue!;
			}

			// process remaining elements synchronously
			let result = iterator.next();

			// if we encounter a Promise, switch to async
			if (result instanceof Promise) {
				return this.#asyncReduce(result, callback, accumulator, index, true)
					.catch(err => handleError(err));
			}

			// continue with synchronous iteration
			while (!result.done) {
				const callbackResult = callback(accumulator, result.value, index++);

				// if callback returns a promise, switch to async
				if (callbackResult instanceof Promise) {
					return callbackResult
						.then(resolvedAccumulator => {
							accumulator = resolvedAccumulator;
							return this.#asyncReduce(iterator.next(), callback, accumulator, index, true);
						})
						.catch(err => handleError(err));
				}

				accumulator = callbackResult;
				result = iterator.next();

				// if we encounter a Promise mid-iteration, switch to async
				if (result instanceof Promise) {
					return this.#asyncReduce(result, callback, accumulator, index, true)
						.catch(err => handleError(err));
				}
			}
		} catch (err) {
			return handleError(err);
		}

		return accumulator;
	}

	/**
	 * Helper function to handle async reduce operations.
	 *
	 * @param result - The result as a promise.
	 * @param callback - The callback function to call for each result.
	 * @param accumulator - The current accumulator value.
	 * @param index - The current index.
	 * @param hasAccumulator - Whether we already have an accumulator value.
	 * @returns The reduced value.
	 */
	async #asyncReduce<U>(
		result: IteratorResult<T> | Promise<IteratorResult<T>>,
		callback: (previousValue: U, currentValue: T, currentIndex: number) => U | Promise<U>,
		accumulator: U,
		index: number,
		hasAccumulator: boolean
	): Promise<U> {
		const iterator = this.#iterator;
		let currentResult = await result;

		// handle case where we need to get initial value from first result
		if (!hasAccumulator) {
			const firstValue = currentResult.value;
			accumulator = firstValue as unknown as U;
			index = 1;
			currentResult = await iterator.next();
		}

		// process remaining elements
		while (!currentResult.done) {
			const callbackResult = callback(accumulator, currentResult.value, index++);

			// await the callback result if it's a promise
			accumulator = callbackResult instanceof Promise ? await callbackResult : callbackResult;

			currentResult = await iterator.next();
		}

		return accumulator;
	}

	/**
	 * Returns a new iterable with the items between the start and end indices.
	 *
	 * @param start - The index to start at.
	 * @param end - The index to end at.
	 * @returns The new iterable.
	 *
	 * @example
	 * ```typescript
	 * const iterator = new ExtendedIterable([1, 2, 3]);
	 * const sliced = iterator.slice(1, 2);
	 * ```
	 */
	slice(start?: number, end?: number): ExtendedIterable<T> {
		return new ExtendedIterable(
			new SliceIterator(this.#iterator, start, end)
		);
	}

	/**
	 * Returns `true` if the callback returns `true` for any item of the
	 * iterable.
	 *
	 * @param callback - The callback function to call for each result.
	 * @returns `true` if the callback returns `true` for any item of the
	 * iterable, `false` otherwise.
	 *
	 * @example
	 * ```typescript
	 * const iterator = new ExtendedIterable([1, 2, 3]);
	 * const hasEven = iterator.some(item => item % 2 === 0);
	 * ```
	 */
	some(callback: (value: T, index: number) => boolean | Promise<boolean>): boolean | Promise<boolean> {
		const iterator = this.#iterator;
		const handleError = (err: unknown) => {
			const throwResult = iterator.throw?.(err);
			if (throwResult instanceof Promise) {
				return throwResult.then(() => { throw err; });
			}
			throw err;
		};

		try {
			if (typeof callback !== 'function') {
				throw new TypeError('Callback is not a function');
			}

			let result = iterator.next();
			let index = 0;

			// if first result is a promise, we know this is async
			if (result instanceof Promise) {
				return this.#asyncSome(result, callback, index)
					.catch(handleError);
			}

			// sync path
			while (!result.done) {
				const rval = callback(result.value, index++);

				if (rval instanceof Promise) {
					return rval
						.then(rval => {
							if (rval) {
								iterator.return?.();
								return true;
							}
							return this.#asyncSome(iterator.next(), callback, index);
						})
						.catch(handleError);
				}

				if (rval) {
					iterator.return?.();
					return true;
				}

				result = iterator.next();

				// if we encounter a Promise mid-iteration, switch to async
				if (result instanceof Promise) {
					return this.#asyncSome(result, callback, index)
						.catch(handleError);
				}
			}
		} catch (err) {
			return handleError(err);
		}

		return false;
	}

	/**
	 * Helper function to process the remaining async iterator results.
	 *
	 * @param result - The result of the iterator as a promise.
	 * @param callback - The callback function to call for each result.
	 * @param index - The current index of the iterator.
	 * @returns `true` if the callback returns `true` for any item of the
	 * iterable, `false` otherwise.
	 */
	async #asyncSome(
		result: IteratorResult<T> | Promise<IteratorResult<T>>,
		callback: (value: T, index: number) => boolean | Promise<boolean>,
		index: number
	): Promise<boolean> {
		const iterator = this.#iterator;
		let currentResult = await result;

		while (!currentResult.done) {
			const rval = callback(currentResult.value, index++);

			if (rval instanceof Promise) {
				return rval.then(rval => {
					if (rval) {
						iterator.return?.();
						return true;
					}
					return this.#asyncSome(iterator.next(), callback, index);
				});
			}

			if (rval) {
				iterator.return?.();
				return true;
			}

			currentResult = await iterator.next();
		}

		return false;
	}

	/**
	 * Returns a new iterable with the first `limit` items.
	 *
	 * @param limit - The number of items to take.
	 * @returns The new iterable.
	 *
	 * @example
	 * ```typescript
	 * const iterator = new ExtendedIterable([1, 2, 3]);
	 * const taken = iterator.take(2);
	 * console.log(taken.asArray); // [1, 2]
	 * ```
	 */
	take(limit: number): ExtendedIterable<T> {
		return new ExtendedIterable(
			new TakeIterator(limit, this.#iterator)
		);
	}

	/**
	 * Returns the iterator results as an array.
	 *
	 * @returns The iterator results as an array.
	 *
	 * @example
	 * ```typescript
	 * const iterator = new ExtendedIterable([1, 2, 3]);
	 * const array = iterator.toArray();
	 * console.log(array); // [1, 2, 3]
	 * ```
	 */
	toArray(): T[] | Promise<T[]> {
		return this.asArray;
	}
}

export default ExtendedIterable;
