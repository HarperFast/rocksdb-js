import type { IterableLike } from '../types.js';

const GeneratorFunction = (function* () {}).constructor;
const AsyncGeneratorFunction = (async function* () {}).constructor;

/**
 * Returns an iterator for the given argument or throws an error.
 *
 * @param iterator - The iterator to resolve.
 * @returns The iterator.
 */
export function resolveIterator<T>(iterator: IterableLike<T>): Iterator<T> | AsyncIterator<T> {
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
