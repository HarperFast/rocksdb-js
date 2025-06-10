export type ValueTransformer<TInput, TOutput> = (value: TInput) => TOutput;

export type IterableLike<T> =
	| Iterator<T>
	| AsyncIterator<T>
	| Iterable<T>
	| (() => Generator<T>)
	| (() => AsyncGenerator<T>);

/**
 * An iterable that provides a rich set of methods for working with ranges of
 * items.
 */
export class ExtendedIterable<T> {
	#iterator: (Iterator<T> | AsyncIterator<T>) & { async?: boolean };
	#transformer?: ValueTransformer<any, T>;

	constructor(
		iterator: IterableLike<T>,
		transformer?: ValueTransformer<any, T>
	) {
		this.#iterator = resolveIterator(iterator);

		if (transformer && typeof transformer !== 'function') {
			throw new TypeError('Transformer must be a function');
		}
		this.#transformer = transformer;
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
	 * Creates an array from an iterable or async iterable.
	 *
	 * @param iterator - The iterable or async iterable to create an array from.
	 * @returns The array or promise of the array.
	 *
	 * @example
	 * ```typescript
	 * const array = ExtendedIterable.from([1, 2, 3]);
	 * ```
	 */
	static from<T>(iterator: IterableLike<T>, transformer?: ValueTransformer<any, T>): Array<T> | Promise<Array<T>> {
		return new ExtendedIterable(iterator, transformer).asArray;
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
		const transformer = this.#transformer;
		const array: T[] = [];
		let result = iterator.next();

		// if first result is a Promise, we know it's async
		if (result instanceof Promise) {
			return this.#asyncAsArray(array, result);
		}

		// continue with synchronous iteration
		while (!result.done) {
			const value = transformer ? transformer(result.value) : result.value;
			array.push(value);

			result = iterator.next();

			// if we encounter a Promise mid-iteration, switch to async
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
	async #asyncAsArray(array: T[], currentResult: Promise<IteratorResult<T>>): Promise<Array<T>> {
		const iterator = this.#iterator;
		const transformer = this.#transformer;
		let result = await currentResult;

		while (!result.done) {
			const value = transformer ? transformer(result.value) : result.value;
			array.push(value);
			result = await iterator.next();
		}

		return array;
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
		if (typeof index !== 'number') {
			throw new TypeError('index is not a number');
		}
		if (index < 0) {
			throw new RangeError('index must be a positive number');
		}

		const iterator = this.#iterator;
		const transformer = this.#transformer;
		let currentIndex = 0;
		let result = iterator.next();

		// if first result is a promise, we know this is async
		if (result instanceof Promise) {
			return this.#asyncAt(result, index, currentIndex);
		}

		// synchronous path
		while (!result.done) {
			if (currentIndex === index) {
				return transformer ? transformer(result.value) : result.value;
			}
			currentIndex++;
			result = iterator.next() as IteratorResult<T>;

			// if we encounter a Promise mid-iteration, switch to async
			if (result instanceof Promise) {
				return this.#asyncAt(result, index, currentIndex);
			}
		}

		return undefined;
	}

	/**
	 * Helper function to return the async iterator result at the given index.
	 *
	 * @param result - The result of the iterator as a promise.
	 * @param index - The index of the item to return.
	 * @param currentIndex - The current index of the iterator.
	 * @returns The item at the given index.
	 */
	async #asyncAt(result: Promise<IteratorResult<T>>, index: number, currentIndex: number): Promise<T | undefined> {
		const iterator = this.#iterator;
		const transformer = this.#transformer;
		let currentResult = await result;

		while (!currentResult.done) {
			if (currentIndex === index) {
				return transformer ? transformer(currentResult.value) : currentResult.value;
			}
			currentIndex++;
			currentResult = await iterator.next();
		}

		return undefined;
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
		const iterator = this.#iterator;
		const transformer = this.#transformer;
		const secondIterator = resolveIterator(other);

		class ConcatIterator implements Iterator<T>, AsyncIterator<T> {
			#firstIteratorDone = false;

			next(): IteratorResult<T> | Promise<IteratorResult<T>> | any {
				// iterate through the original iterator first
				if (!this.#firstIteratorDone) {
					const result = iterator.next();
					if (result instanceof Promise) {
						return result.then((result) => this.#processFirstResult(result));
					}
					return this.#processFirstResult(result);
				}

				// then iterate through the second iterator
				return this.#getNextFromOther();
			}

			#processFirstResult(result: IteratorResult<T>): IteratorResult<T> | Promise<IteratorResult<T>> {
				if (result.done) {
					this.#firstIteratorDone = true;
					return this.#getNextFromOther();
				}
				return this.#processResult(result, transformer);
			}

			#getNextFromOther(): IteratorResult<T> | Promise<IteratorResult<T>> {
				const result = secondIterator.next();
				if (result instanceof Promise) {
					return result.then(this.#processResult);
				}
				return this.#processResult(result);
			}

			#processResult(result: IteratorResult<T>, transformer?: ValueTransformer<T, T>): IteratorResult<T> {
				if (result.done) {
					return result;
				}
				return {
					value: transformer ? transformer(result.value) : result.value,
					done: false
				};
			}
		}

		return new ExtendedIterable(new ConcatIterator());
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
		if (typeof count !== 'number') {
			throw new TypeError('Count is not a number');
		}
		if (count < 0) {
			throw new RangeError('Count must be a positive number');
		}
		if (count === 0) {
			return this;
		}

		const iterator = this.#iterator;
		const transformer = this.#transformer;

		class DropIterator implements Iterator<T>, AsyncIterator<T> {
			#itemsDropped = 0;
			#doneSkipping = false;

			next(): IteratorResult<T> | Promise<IteratorResult<T>> | any {
				if (this.#doneSkipping) {
					const result = iterator.next();
					if (result instanceof Promise) {
						return result.then(r => this.#transformResult(r));
					}
					return this.#transformResult(result);
				}

				let result = iterator.next();

				if (result instanceof Promise) {
					return this.#asyncSkip(result);
				}

				while (!result.done && this.#itemsDropped < count) {
					this.#itemsDropped++;
					result = iterator.next();

					// handle case where iterator becomes async mid-iteration
					if (result instanceof Promise) {
						return this.#asyncSkip(result);
					}
				}

				this.#doneSkipping = true;
				return this.#transformResult(result);
			}

			async #asyncSkip(result: Promise<IteratorResult<T>>): Promise<IteratorResult<T>> {
				let currentResult = await result;

				// continue skipping if needed
				while (!currentResult.done && this.#itemsDropped < count) {
					this.#itemsDropped++;
					currentResult = await iterator.next();
				}

				this.#doneSkipping = true;
				return this.#transformResult(currentResult);
			}

			#transformResult(result: IteratorResult<T>): IteratorResult<T> {
				if (result.done) {
					return result;
				}
				return {
					value: transformer ? transformer(result.value) : result.value,
					done: false
				};
			}
		}

		return new ExtendedIterable(new DropIterator());
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
		if (typeof callback !== 'function') {
			throw new TypeError('Callback is not a function');
		}

		const iterator = this.#iterator;
		const transformer = this.#transformer;
		let result = iterator.next();
		let index = 0;

		// if first result is a promise, we know this is async
		if (result instanceof Promise) {
			return this.#asyncEvery(result, callback, index);
		}

		// sync path
		while (!result.done) {
			const value = transformer ? transformer(result.value) : result.value;
			const rval = callback(value, index++);

			if (rval instanceof Promise) {
				return rval.then(rval => {
					if (!rval) {
						return false;
					}
					return this.#asyncEvery(iterator.next(), callback, index);
				});
			}

			if (!rval) {
				return false;
			}
			result = iterator.next() as IteratorResult<T>;

			// if we encounter a Promise mid-iteration, switch to async
			if (result instanceof Promise) {
				return this.#asyncEvery(result, callback, index);
			}
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
		callback: (value: T, index: number) => boolean | Promise<boolean>, index: number
	): Promise<boolean> {
		const iterator = this.#iterator;
		const transformer = this.#transformer;
		let currentResult = await result;

		while (!currentResult.done) {
			const value = transformer ? transformer(currentResult.value) : currentResult.value;
			const ok = callback(value, index++);

			if (ok instanceof Promise) {
				return ok.then(ok => {
					if (!ok) {
						return false;
					}
					return this.#asyncEvery(iterator.next(), callback, index);
				});
			}

			if (!ok) {
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
		if (typeof callback !== 'function') {
			throw new TypeError('Callback is not a function');
		}

		const iterator = this.#iterator;
		const transformer = this.#transformer;

		class FilterIterator implements Iterator<T>, AsyncIterator<T> {
			#index = 0;

			next(): IteratorResult<T> | Promise<IteratorResult<T>> | any {
				let result = iterator.next();

				// async handling
				if (result instanceof Promise) {
					return this.#asyncFilter(result);
				}

				// sync handling
				while (!result.done) {
					const value = transformer ? transformer(result.value) : result.value;
					const keep = callback(value, this.#index++);

					if (keep instanceof Promise) {
						return keep.then(keep => {
							if (keep) {
								return {
									value,
									done: false
								};
							}
							return this.next();
						});
					}

					if (keep) {
						return {
							value,
							done: false
						};
					}

					result = iterator.next();

					// handle sync-to-async transition
					if (result instanceof Promise) {
						return this.#asyncFilter(result);
					}
				}

				return result;
			}

			async #asyncFilter(result: Promise<IteratorResult<T>>): Promise<IteratorResult<T>> {
				let currentResult = await result;

				while (!currentResult.done) {
					const value = transformer ? transformer(currentResult.value) : currentResult.value;
					const keep = callback(value, this.#index++);

					if (keep instanceof Promise) {
						return keep.then(keep => {
							if (keep) {
								return {
									value,
									done: false
								};
							}
							return this.next();
						});
					}

					if (keep) {
						return {
							value,
							done: false
						};
					}
					currentResult = await this.next();
				}

				return currentResult;
			}
		}

		return new ExtendedIterable(new FilterIterator());
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
		if (typeof callback !== 'function') {
			throw new TypeError('Callback is not a function');
		}

		const iterator = this.#iterator;
		const transformer = this.#transformer;
		let result = iterator.next();
		let index = 0;

		// if first result is a promise, we know this is async
		if (result instanceof Promise) {
			return this.#asyncFind(result, callback, index);
		}

		// sync path
		while (!result.done) {
			const value = transformer ? transformer(result.value) : result.value;
			if (callback(value, index++)) {
				return value;
			}
			result = iterator.next() as IteratorResult<T>;

			// if we encounter a Promise mid-iteration, switch to async
			if (result instanceof Promise) {
				return this.#asyncFind(result, callback, index);
			}
		}

		return undefined;
	}

	async #asyncFind(result: IteratorResult<T> | Promise<IteratorResult<T>>, callback: (value: T, index: number) => boolean | Promise<boolean>, index: number): Promise<T | undefined> {
		const iterator = this.#iterator;
		const transformer = this.#transformer;
		let currentResult = await result;

		while (!currentResult.done) {
			const value = transformer ? transformer(currentResult.value) : currentResult.value;
			const isMatch = callback(value, index++);
			if (isMatch instanceof Promise) {
				return isMatch.then(isMatch => {
					if (isMatch) {
						return value;
					}
					return this.#asyncFind(iterator.next(), callback, index);
				});
			}
			if (isMatch) {
				return value;
			}
			currentResult = await iterator.next();
		}

		return undefined;
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
		if (typeof callback !== 'function') {
			throw new TypeError('Callback is not a function');
		}

		const iterator = this.#iterator;
		const transformer = this.#transformer;

		class FlatMapIterator implements Iterator<U>, AsyncIterator<U> {
			#index = 0;
			#currentSubIterator: Iterator<U> | null = null;

			next(): IteratorResult<U> | Promise<IteratorResult<U>> | any {
				return this.#getNext();
			}

			#getNext(): IteratorResult<U> | Promise<IteratorResult<U>> {
				// try to get value from current sub-iterator first
				if (this.#currentSubIterator) {
					const subResult = this.#currentSubIterator.next();
					if (!subResult.done) {
						return subResult;
					}
					this.#currentSubIterator = null;
				}

				return this.#getNextFromMain();
			}

			#getNextFromMain(): IteratorResult<U> | Promise<IteratorResult<U>> {
				const mainResult = iterator.next();

				if (mainResult instanceof Promise) {
					return mainResult.then(result => this.#processMainResult(result));
				}

				return this.#processMainResult(mainResult);
			}

			#processMainResult(result: IteratorResult<T>): IteratorResult<U> | Promise<IteratorResult<U>> {
				if (result.done) {
					return { value: undefined as any, done: true };
				}

				const value = transformer ? transformer(result.value) : result.value;
				const callbackResult = callback(value, this.#index++);

				// handle promise from callback
				if (callbackResult instanceof Promise) {
					return callbackResult.then(resolvedResult => this.#processCallbackResult(resolvedResult));
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
					// Get first value from new sub-iterator
					return this.#getNext();
				}

				// not iterable, return as single value
				return {
					value: callbackResult as U,
					done: false
				};
			}
		}

		return new ExtendedIterable(new FlatMapIterator());
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
		if (typeof callback !== 'function') {
			throw new TypeError('Callback is not a function');
		}

		const iterator = this.#iterator;
		const transformer = this.#transformer;
		let result = iterator.next();
		let index = 0;

		// if first result is a promise, we know this is async
		if (result instanceof Promise) {
			return this.#asyncForEach(result, callback, index);
		}

		// sync path
		while (!result.done) {
			const value = transformer ? transformer(result.value) : result.value;
			const rval = callback(value, index++);
			if (rval instanceof Promise) {
				return rval.then(() => this.#asyncForEach(iterator.next(), callback, index));
			}

			result = iterator.next();

			// if we encounter a Promise mid-iteration, switch to async
			if (result instanceof Promise) {
				return this.#asyncForEach(result, callback, index);
			}
		}
	}

	/**
	 * Helper function to process the remaining async iterator results.
	 *
	 * @param result - The result of the iterator as a promise.
	 * @param callback - The callback function to call for each result.
	 * @param index - The current index of the iterator.
	 */
	async #asyncForEach(result: IteratorResult<T> | Promise<IteratorResult<T>>, callback: (value: T, index: number) => any | Promise<any>, index: number): Promise<void> {
		const iterator = this.#iterator;
		const transformer = this.#transformer;
		let currentResult = await result;

		while (!currentResult.done) {
			const value = transformer ? transformer(currentResult.value) : currentResult.value;
			const rval = callback(value, index++);
			if (rval instanceof Promise) {
				return rval.then(() => this.#asyncForEach(iterator.next(), callback, index));
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
		if (typeof callback !== 'function') {
			throw new TypeError('Callback is not a function');
		}

		const iterator = this.#iterator;
		const transformer = this.#transformer;

		class MapIterator implements Iterator<U>, AsyncIterator<U> {
			#index = 0;

			next(): IteratorResult<U> | Promise<IteratorResult<U>> | any {
				const result = iterator.next();

				// async handling
				if (result instanceof Promise) {
					return this.#asyncMap(result);
				}

				// sync handling
				if (result.done) {
					return result;
				}

				const value = transformer ? transformer(result.value) : result.value;
				const mappedValue = callback(value, this.#index++);

				if (mappedValue instanceof Promise) {
					return mappedValue.then(value => ({
						value,
						done: false
					}));
				}

				return {
					value: mappedValue,
					done: false
				};
			}

			async #asyncMap(result: Promise<IteratorResult<T>>): Promise<IteratorResult<U>> {
				const currentResult = await result;
				if (currentResult.done) {
					return currentResult as IteratorResult<U>;
				}

				const value = transformer ? transformer(currentResult.value) : currentResult.value;
				const mappedValue = callback(value, this.#index++);

				if (mappedValue instanceof Promise) {
					return mappedValue.then(value => ({
						value,
						done: false
					}));
				}

				return {
					value: mappedValue,
					done: false
				};
			}
		}

		return new ExtendedIterable(new MapIterator());
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
		if (catchCallback && typeof catchCallback !== 'function') {
			throw new TypeError('Callback is not a function');
		}

		const iterator = this.#iterator;
		const transformer = this.#transformer;

		class ErrorMappingIterator implements Iterator<T | Error>, AsyncIterator<T | Error> {
			next(): IteratorResult<T | Error> | Promise<IteratorResult<T | Error>> | any {
				try {
					const result = iterator.next();

					// async handling
					if (result instanceof Promise) {
						return this.#asyncNext(result);
					}

					// sync handling
					if (result.done) {
						return result;
					}

					const value = transformer ? transformer(result.value) : result.value;
					return {
						value,
						done: false
					};
				} catch (error: unknown) {
					// handle sync errors - return error as value and continue on next call
					if (catchCallback) {
						const err = catchCallback(error);
						// if catchCallback returns a promise, switch to async handling
						if (err instanceof Promise) {
							return err.then(resolvedErr => ({
								value: resolvedErr,
								done: false
							}));
						}
						return {
							value: err,
							done: false
						};
					}
					return {
						value: error,
						done: false
					};
				}
			}

			async #asyncNext(result: Promise<IteratorResult<T>>): Promise<IteratorResult<T | Error>> {
				try {
					const currentResult = await result;

					if (currentResult.done) {
						return currentResult as IteratorResult<T | Error>;
					}

					// apply transformer if present
					const value = transformer ? transformer(currentResult.value) : currentResult.value;
					return {
						value,
						done: false
					};
				} catch (error) {
					// handle async errors - return error as value
					if (catchCallback) {
						const err = catchCallback(error);
						// await the result if it's a promise
						const resolvedErr = err instanceof Promise ? await err : err;
						return {
							value: resolvedErr as any,
							done: false
						};
					}
					return {
						value: error as any,
						done: false
					};
				}
			}
		}

		return new ExtendedIterable(new ErrorMappingIterator());
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
		if (typeof callback !== 'function') {
			throw new TypeError('Callback is not a function');
		}

		const iterator = this.#iterator;
		const transformer = this.#transformer;
		const hasInitialValue = arguments.length >= 2;
		let index = 0;
		let accumulator: U;

		// handle initial value setup
		if (!hasInitialValue) {
			const firstResult = iterator.next();

			// if first result is a promise, we know this is async
			if (firstResult instanceof Promise) {
				return this.#asyncReduce(firstResult, callback, undefined as any, index, false);
			}

			if (firstResult.done) {
				throw new TypeError('Reduce of empty iterable with no initial value');
			}

			const firstValue = transformer ? transformer(firstResult.value) : firstResult.value;
			accumulator = firstValue as unknown as U;
			index = 1;
		} else {
			accumulator = initialValue!;
		}

		// process remaining elements synchronously
		let result = iterator.next();

		// if we encounter a Promise, switch to async
		if (result instanceof Promise) {
			return this.#asyncReduce(result, callback, accumulator, index, true);
		}

		// continue with synchronous iteration
		while (!result.done) {
			const value = transformer ? transformer(result.value) : result.value;
			const callbackResult = callback(accumulator, value, index++);

			// if callback returns a promise, switch to async
			if (callbackResult instanceof Promise) {
				return callbackResult.then(resolvedAccumulator => {
					accumulator = resolvedAccumulator;
					return this.#asyncReduce(iterator.next(), callback, accumulator, index, true);
				});
			}

			accumulator = callbackResult;
			result = iterator.next();

			// if we encounter a Promise mid-iteration, switch to async
			if (result instanceof Promise) {
				return this.#asyncReduce(result, callback, accumulator, index, true);
			}
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
		const transformer = this.#transformer;
		let currentResult = await result;

		// handle case where we need to get initial value from first result
		if (!hasAccumulator) {
			if (currentResult.done) {
				throw new TypeError('Reduce of empty iterable with no initial value');
			}

			const firstValue = transformer ? transformer(currentResult.value) : currentResult.value;
			accumulator = firstValue as unknown as U;
			index = 1;
			currentResult = await iterator.next();
		}

		// process remaining elements
		while (!currentResult.done) {
			const value = transformer ? transformer(currentResult.value) : currentResult.value;
			const callbackResult = callback(accumulator, value, index++);

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
		if (start !== undefined) {
			if (typeof start !== 'number') {
				throw new TypeError('Start is not a number');
			}
			if (start < 0) {
				throw new RangeError('Start must be a positive number');
			}
		}
		if (end !== undefined) {
			if (typeof end !== 'number') {
				throw new TypeError('End is not a number');
			}
			if (end < 0) {
				throw new RangeError('End must be a positive number');
			}
		}

		const startIndex = start ?? 0;
		const endIndex = end;

		// Early termination for empty slice
		if (endIndex !== undefined && startIndex >= endIndex) {
			return new ExtendedIterable(function*() {}());
		}

		const iterator = this.#iterator;
		const transformer = this.#transformer;

		class SliceIterator implements Iterator<T>, AsyncIterator<T> {
			#index = 0;
			#yielding = false;

			next(): IteratorResult<T> | Promise<IteratorResult<T>> | any {
				// skip to start if not already yielding
				if (!this.#yielding) {
					return this.#skipToStart();
				}

				// Check if we've reached the end
				if (endIndex !== undefined && this.#index >= endIndex) {
					return { value: undefined, done: true };
				}

				return this.#getNextValue();
			}

			#skipToStart(): IteratorResult<T> | Promise<IteratorResult<T>> {
				// skip elements synchronously if possible
				while (this.#index < startIndex) {
					const result = iterator.next();

					// Handle async transition during skipping
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
				while (!currentResult.done && this.#index < startIndex) {
					this.#index++;
					currentResult = await iterator.next();
				}

				this.#yielding = true;

				// if iterator is done or we've reached the end index, return done
				if (currentResult.done || (endIndex !== undefined && this.#index >= endIndex)) {
					return currentResult;
				}

				// process the current result
				this.#index++;
				const value = transformer ? transformer(currentResult.value) : currentResult.value;
				return { value, done: false };
			}

			#getNextValue(): IteratorResult<T> | Promise<IteratorResult<T>> {
				const result = iterator.next();

				if (result instanceof Promise) {
					return this.#asyncGetNextValue(result);
				}

				if (result.done) {
					return result;
				}

				this.#index++;
				const value = transformer ? transformer(result.value) : result.value;
				return { value, done: false };
			}

			async #asyncGetNextValue(result: Promise<IteratorResult<T>>): Promise<IteratorResult<T>> {
				const currentResult = await result;

				if (currentResult.done) {
					return currentResult;
				}

				this.#index++;
				const value = transformer ? transformer(currentResult.value) : currentResult.value;
				return { value, done: false };
			}
		}

		return new ExtendedIterable(new SliceIterator());
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
		if (typeof callback !== 'function') {
			throw new TypeError('Callback is not a function');
		}

		const iterator = this.#iterator;
		const transformer = this.#transformer;
		let result = iterator.next();
		let index = 0;

		// if first result is a promise, we know this is async
		if (result instanceof Promise) {
			return this.#asyncSome(result, callback, index);
		}

		// sync path
		while (!result.done) {
			const value = transformer ? transformer(result.value) : result.value;
			const rval = callback(value, index++);

			if (rval instanceof Promise) {
				return rval.then(rval => {
					if (rval) {
						return true;
					}
					return this.#asyncSome(iterator.next(), callback, index);
				});
			}

			if (rval) {
				return true;
			}

			result = iterator.next();

			// if we encounter a Promise mid-iteration, switch to async
			if (result instanceof Promise) {
				return this.#asyncSome(result, callback, index);
			}
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
		const transformer = this.#transformer;
		let currentResult = await result;

		while (!currentResult.done) {
			const value = transformer ? transformer(currentResult.value) : currentResult.value;
			const rval = callback(value, index++);

			if (rval instanceof Promise) {
				return rval.then(rval => {
					if (rval) {
						return true;
					}
					return this.#asyncSome(iterator.next(), callback, index);
				});
			}

			if (rval) {
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
		if (typeof limit !== 'number') {
			throw new TypeError('limit is not a number');
		}
		if (limit < 0) {
			throw new RangeError('limit must be a positive number');
		}
		const iterator = this.#iterator;
		const transformer = this.#transformer;

		class TakeIterator implements Iterator<T>, AsyncIterator<T> {
			#count = 0;

			next(): IteratorResult<T> | Promise<IteratorResult<T>> | any {
				const result = iterator.next();

				// async handling
				if (result instanceof Promise) {
					return this.#asyncNext(result);
				}

				// sync handling
				if (result.done) {
					return result;
				}

				if (this.#count >= limit) {
					return { value: undefined, done: true };
				}

				this.#count++;
				const value = transformer ? transformer(result.value) : result.value;
				return {
					value,
					done: false
				};
			}

			async #asyncNext(result: Promise<IteratorResult<T>>): Promise<IteratorResult<T>> {
				const currentResult = await result;
				if (currentResult.done) {
					return currentResult;
				}

				if (this.#count >= limit) {
					return { value: undefined, done: true };
				}

				this.#count++;
				const value = transformer ? transformer(currentResult.value) : currentResult.value;
				return {
					value,
					done: false
				};
			}
		}

		return new ExtendedIterable(new TakeIterator());
	}
}

export default ExtendedIterable;

const GeneratorFunction = Object.getPrototypeOf(function*(){}).constructor;
const AsyncGeneratorFunction = Object.getPrototypeOf(async function*(){}).constructor;

/**
 * Returns an iterator for the given argument or throws an error.
 *
 * @param iterator - The iterator to resolve.
 * @returns The iterator.
 */
function resolveIterator<T>(iterator: IterableLike<T>): Iterator<T> | AsyncIterator<T> {
	if (iterator && typeof iterator[Symbol.iterator] === 'function') { // Iterable
		return iterator[Symbol.iterator]();
	} else if (
		typeof iterator === 'function' &&
		(iterator instanceof GeneratorFunction || iterator instanceof AsyncGeneratorFunction)
	) { // Generator or AsyncGenerator
		return iterator();
	} else if (
		iterator &&
		typeof iterator === 'object' &&
		'next' in iterator &&
		typeof iterator.next === 'function'
	) { // Iterator
		return iterator;
	} else {
		throw new TypeError('Argument is not iterable');
	}
}
