export type ValueTransformer<TInput, TOutput> = (value: TInput) => TOutput;

/**
 * An iterable that provides a rich set of methods for working with ranges of
 * items.
 */
export class RangeIterable<T> {
	#iterator: Iterator<T> & { async?: boolean };
	#transformer?: ValueTransformer<any, T>;

	constructor(
		iterator: Iterator<T> | Array<T>,
		transformer?: ValueTransformer<any, T>
	) {
		this.#transformer = transformer;
		this.#iterator = Array.isArray(iterator) ? iterator[Symbol.iterator]() : iterator;
	}

	/**
	 * Returns an iterator in synchronous mode.
	 */
	[Symbol.iterator]() {
		return this.#iterator;
	}

	/**
	 * Returns an iterator in asynchronous mode.
	 */
	[Symbol.asyncIterator]() {
		this.#iterator.async = true;
		return this.#iterator;
	}

	/**
	 * Collects the iterator results in an array and returns it.
	 *
	 * @returns The iterator results as an array.
	 *
	 * @example
	 * ```typescript
	 * const array = db.getRange().asArray;
	 * ```
	 */
	get asArray(): Array<T> | Promise<Array<T>> {
		let result: Array<T> | undefined;

		const promise = new Promise<Array<T>>(resolve => {
			const iterator = this.#iterator;

			const array: T[] = [];
			(function next(iResult) {
				while (iResult.done !== true) {
					if (iResult instanceof Promise) {
						return iResult.then(next);
					}
					array.push(iResult.value);
					iResult = iterator.next();
				}
				resolve(result = array);
			}(iterator.next()));
		});

		return result || promise;
	}

	/**
	 * Returns the item at the given index.
	 *
	 * @param index - The index of the item to return.
	 * @returns The item at the given index.
	 *
	 * @example
	 * ```typescript
	 * const item = db.getRange().at(0);
	 * ```
	 */
	at(index: number): T | undefined {
		for (const entry of this) {
			if (index-- === 0) {
				return entry;
			}
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
	 * const concatenated = db.getRange().concat(db.getRange());
	 * ```
	 */
	concat(other: Iterable<T>): RangeIterable<T> {
		const iterator = this.#iterator;
		const transformer = this.#transformer;

		class ConcatIterator implements Iterator<T> {
			#firstIteratorDone = false;
			#secondIterator: Iterator<T> | null = null;

			next(): IteratorResult<T> {
				// iterate through the original iterator
				if (!this.#firstIteratorDone) {
					const result = iterator.next();
					if (!result.done) {
						const value = typeof transformer === 'function' ? transformer(result.value) : result.value;
						return {
							value: value,
							done: false
						};
					} else {
						this.#firstIteratorDone = true;
						this.#secondIterator = other[Symbol.iterator]();
					}
				}

				// then iterate through the second iterable
				if (this.#secondIterator) {
					const result = this.#secondIterator.next();
					if (!result.done) {
						// no transformer applied here since iterable is already Iterable<T>
						return {
							value: result.value,
							done: false
						};
					}
				}

				return { value: undefined, done: true };
			}
		}

		return new RangeIterable(new ConcatIterator());
	}

	/**
	 * Returns a new iterable with the first `limit` items removed.
	 *
	 * @param limit - The number of items to remove.
	 * @returns The new iterable.
	 *
	 * @example
	 * ```typescript
	 * for (const { key, value } of db.getRange().drop(10)) {
	 *   console.log({ key, value });
	 * }
	 * ```
	 */
	drop(limit: number): RangeIterable<T> {
		const iterator = this.#iterator;
		const transformer = this.#transformer;

		class DropIterator implements Iterator<T> {
			#index = 0;

			next(): IteratorResult<T> {
				// skip the first `limit` items
				while (this.#index < limit) {
					const result = iterator.next();
					if (result.done) {
						return result;
					}
					this.#index++;
				}

				const result = iterator.next();
				if (result.done) {
					return result;
				}

				const value = typeof transformer === 'function' ? transformer(result.value) : result.value;
				return {
					value: value,
					done: false
				};
			}
		}

		return new RangeIterable(new DropIterator());
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
	 * const isAllValid = db.getRange().every(item => item.value.length > 0);
	 * ```
	 */
	every(callback: (value: T, index: number) => boolean): boolean {
		const iterator = this.#iterator;
		const transformer = this.#transformer;
		let index = 0;
		let result;
		while ((result = iterator.next()).done !== true) {
			const value = typeof transformer === 'function' ? transformer(result.value) : result.value;
			if (!callback(value, index++)) {
				return false;
			}
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
	 * const filtered = db.getRange().filter(item => item.value.length > 0);
	 * ```
	 */
	filter(callback: (value: T, index: number) => boolean): RangeIterable<T> {
		const iterator = this.#iterator;
		const transformer = this.#transformer;

		class FilterIterator implements Iterator<T> {
			#index = 0;

			next(): IteratorResult<T> {
				let result = iterator.next();

				while (!result.done) {
					const value = typeof transformer === 'function' ? transformer(result.value) : result.value;

					if (callback(value, this.#index++)) {
						return {
							value: value,
							done: false
						};
					}

					result = iterator.next();
				}

				return result;
			}
		}

		return new RangeIterable(new FilterIterator());
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
	 * const found = db.getRange().find(item => item.value.length > 0);
	 * ```
	 */
	find(callback: (value: T, index: number) => boolean): T | undefined {
		const iterator = this.#iterator;
		const transformer = this.#transformer;
		let index = 0;
		let result;
		while ((result = iterator.next()).done !== true) {
			const value = typeof transformer === 'function' ? transformer(result.value) : result.value;
			if (callback(value, index++)) {
				return value;
			}
		}
		return undefined;
	}

	/**
	 * Returns a new iterable with the results of a callback function, then
	 * flattens the results.
	 *
	 * @param callback - The callback function to call for each result.
	 * @returns The new iterable.
	 *
	 * @example
	 * ```typescript
	 * const flattened = db.getRange().flatMap(item => [item, item]);
	 * ```
	 */
	flatMap<U>(callback: (value: T, index: number) => U[] | Iterable<U>): RangeIterable<U> {
		const iterator = this.#iterator;
		const transformer = this.#transformer;

		class FlatMapIterator implements Iterator<U> {
			#index = 0;
			#currentSubIterator: Iterator<U> | null = null;
			#mainIteratorDone = false;

			next(): IteratorResult<U> {
				while (true) {
					// If we have a current sub-iterator, try to get the next value from it
					if (this.#currentSubIterator) {
						const subResult = this.#currentSubIterator.next();
						if (!subResult.done) {
							return subResult;
						}
						// Sub-iterator is done, clear it and continue
						this.#currentSubIterator = null;
					}

					// If main iterator is done, we're completely done
					if (this.#mainIteratorDone) {
						return { value: undefined as any, done: true };
					}

					// Get next value from main iterator
					const mainResult = iterator.next();
					if (mainResult.done) {
						this.#mainIteratorDone = true;
						return { value: undefined as any, done: true };
					}

					// Transform the value and apply callback
					const value = typeof transformer === 'function' ? transformer(mainResult.value) : mainResult.value;
					const callbackResult = callback(value, this.#index++);

					// Create sub-iterator from callback result
					if (Array.isArray(callbackResult)) {
						this.#currentSubIterator = callbackResult[Symbol.iterator]();
					} else if (callbackResult && typeof callbackResult[Symbol.iterator] === 'function') {
						this.#currentSubIterator = callbackResult[Symbol.iterator]();
					} else {
						// If callback result is not iterable, skip this iteration
						continue;
					}
				}
			}
		}

		return new RangeIterable(new FlatMapIterator());
	}

	/**
	 * Calls a function for each item of the iterable.
	 *
	 * @param callback - The callback function to call for each result.
	 *
	 * @example
	 * ```typescript
	 * db.getRange().forEach(item => console.log(item));
	 * ```
	 */
	forEach(callback: (value: T, index: number) => void): void {
		const iterator = this.#iterator;
		const transformer = this.#transformer;
		let index = 0;
		let result;
		while ((result = iterator.next()).done !== true) {
			const value = typeof transformer === 'function' ? transformer(result.value) : result.value;
			callback(value, index++);
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
	 * const mapped = db.getRange().map(item => item.value.length);
	 * ```
	 */
	map<U>(callback: (value: T, index: number) => U): RangeIterable<U> {
		const iterator = this.#iterator;
		const transformer = this.#transformer;

		class MapIterator implements Iterator<U> {
			#index = 0;

			next(): IteratorResult<U> {
				const result = iterator.next();
				if (result.done) {
					return result;
				}

				const value = typeof transformer === 'function' ? transformer(result.value) : result.value;
				const mappedValue = callback(value, this.#index++);

				return {
					value: mappedValue,
					done: false
				};
			}
		}

		return new RangeIterable(new MapIterator());
	}

	/**
	 * Catch errors thrown during iteration and allow iteration to continue.
	 *
	 * @param catchCallback - The callback to handle errors. The returned error is logged/handled but iteration continues.
	 * @returns The new iterable.
	 *
	 * @example
	 * ```typescript
	 * const mapped = db.getRange().mapError(error => new Error('Error: ' + error.message));
	 * ```
	 */
	mapError(catchCallback: (error: Error) => Error): RangeIterable<T> {
		const iterator = this.#iterator;
		const transformer = this.#transformer;

		class ErrorMappingIterator implements Iterator<T> {
			next(): IteratorResult<T> {
				while (true) {
					try {
						const result = iterator.next();
						if (result.done) {
							return result;
						}

						// apply transformer if present
						const value = typeof transformer === 'function' ? transformer(result.value) : result.value;
						return {
							value: value,
							done: false
						};
					} catch (error) {
						// call the catch callback with the error to allow transformation/logging
						const err = catchCallback(error instanceof Error ? error : new Error(String(error)));
						return {
							value: err as any
						};
					}
				}
			}
		}

		return new RangeIterable(new ErrorMappingIterator());
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
	 * const sum = db.getRange().reduce((acc, item) => acc + item.value.length, 0);
	 * ````
	 */
	reduce<U>(callback: (previousValue: U, currentValue: T, currentIndex: number) => U, initialValue: U): U;
	reduce(callback: (previousValue: T, currentValue: T, currentIndex: number) => T): T;
	reduce<U>(callback: (previousValue: U, currentValue: T, currentIndex: number) => U, initialValue?: U): U {
		const iterator = this.#iterator;
		const transformer = this.#transformer;
		let index = 0;
		let accumulator: U;
		let hasInitialValue = arguments.length >= 2;

		// if no initial value provided, use first element as accumulator
		if (!hasInitialValue) {
			const firstResult = iterator.next();
			if (firstResult.done) {
				throw new TypeError('Reduce of empty iterable with no initial value');
			}
			const firstValue = typeof transformer === 'function' ? transformer(firstResult.value) : firstResult.value;
			accumulator = firstValue as unknown as U;
			index = 1;
		} else {
			accumulator = initialValue!;
		}

		// process remaining elements
		let result;
		while ((result = iterator.next()).done !== true) {
			const value = typeof transformer === 'function' ? transformer(result.value) : result.value;
			accumulator = callback(accumulator, value, index++);
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
	 * const sliced = db.getRange().slice(10, 20);
	 * ```
	 */
	slice(start?: number, end?: number): RangeIterable<T> {
		const iterator = this.#iterator;
		const transformer = this.#transformer;

		class SliceIterator implements Iterator<T> {
			#index = 0;
			#startIndex = start ?? 0;
			#endIndex = end;

			next(): IteratorResult<T> {
				// skip elements before start index
				while (this.#index < this.#startIndex) {
					const result = iterator.next();
					if (result.done) {
						return result;
					}
					this.#index++;
				}

				if (this.#endIndex !== undefined && this.#index >= this.#endIndex) {
					return { value: undefined, done: true };
				}

				// get the next element
				const result = iterator.next();
				if (result.done) {
					return result;
				}

				this.#index++;
				const value = typeof transformer === 'function' ? transformer(result.value) : result.value;
				return {
					value: value,
					done: false
				};
			}
		}

		return new RangeIterable(new SliceIterator());
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
	 * const hasEven = db.getRange().some(item => item.value.length % 2 === 0);
	 * ```
	 */
	some(callback: (value: T, index: number) => boolean): boolean {
		const iterator = this.#iterator;
		const transformer = this.#transformer;
		let index = 0;
		let result;
		while ((result = iterator.next()).done !== true) {
			const value = typeof transformer === 'function' ? transformer(result.value) : result.value;
			if (callback(value, index++)) {
				return true;
			}
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
	 * for (const { key, value } of db.getRange().take(10)) {
	 *   console.log({ key, value });
	 * }
	 * ```
	 */
	take(limit: number): RangeIterable<T> {
		const iterator = this.#iterator;
		const transformer = this.#transformer;

		class TakeIterator implements Iterator<T> {
			#count = 0;

			next(): IteratorResult<T> {
				if (this.#count >= limit) {
					return { value: undefined, done: true };
				}

				const result = iterator.next();
				if (result.done) {
					return result;
				}

				this.#count++;
				const value = typeof transformer === 'function' ? transformer(result.value) : result.value;
				return {
					value: value,
					done: false
				};
			}
		}

		return new RangeIterable(new TakeIterator());
	}
}
