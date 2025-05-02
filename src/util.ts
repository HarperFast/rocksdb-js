export type MaybePromise<T> = T | Promise<T>;

export type MaybePromiseFunction<T> = () => MaybePromise<T>;

/**
 * Helper function handling `MaybePromise` results.
 *
 * If the result originates from a function that could throw an error, wrap it
 * in a function so this function can catch any errors and use a unified error
 * handling mechanism.
 */
export function when<T>(
	subject: MaybePromise<T> | MaybePromiseFunction<T>,
	callback?: (value: T) => MaybePromise<T>,
	errback?: (reason: any) => T,
): MaybePromise<T> {
	try {
		let result: MaybePromise<T>;

		if (typeof subject === 'function') {
			result = (subject as MaybePromiseFunction<T>)();
		} else {
			result = subject;
		}

		if (result instanceof Promise) {
			return result.then(callback, errback) as T;
		}

		return callback ? callback(result as T) : result as T;
	} catch (error) {
		return errback ? errback(error) : Promise.reject(error);
	}
}
