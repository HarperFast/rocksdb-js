export type MaybePromise<T> = T | Promise<T>;

export type MaybePromiseFunction<T> = () => MaybePromise<T>;

/**
 * Parses a duration string into milliseconds.
 *
 * @param duration - The duration string to parse.
 * @returns The duration in milliseconds.
 *
 * @example
 * ```typescript
 * parseDuration('1s'); // 1000
 * parseDuration('1m'); // 60000
 * parseDuration('1h'); // 3600000
 * parseDuration('1d'); // 86400000
 * parseDuration('1ms'); // 1
 * parseDuration('1s 1ms'); // 1001
 * parseDuration('1m 1s'); // 61000
 * parseDuration('foo'); // throws error
 *
 * parseDuration(1000); // 1000
 * parseDuration(60000); // 60000
 * parseDuration(3600000); // 3600000
 * parseDuration(86400000); // 86400000
 * parseDuration(NaN); // throws error
 * ```
 */
export function parseDuration(duration: number | string): number {
	if (typeof duration === 'number') {
		if (isNaN(duration) || !isFinite(duration)) {
			throw new Error(`Invalid duration: ${duration}`);
		}
		return duration;
	}

	let result = 0;
	for (const part of duration.split(' ')) {
		const m = part.match(/^(\d+)\s*(ms|s|m|h|d)?$/);
		if (!m) {
			throw new Error(`Invalid duration: ${duration}`);
		}

		const [, value, unit] = m;
		let num = parseInt(value, 10);
		switch (unit) {
			case 's':
				num *= 1000;
				break;
			case 'm':
				num *= 1000 * 60;
				break;
			case 'h':
				num *= 1000 * 60 * 60;
				break;
			case 'd':
				num *= 1000 * 60 * 60 * 24;
				break;
		}
		result += num;
	}
	return result;
}

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
	errback?: (reason: any) => T
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

/**
 * Polyfill for `Promise.withResolvers`.
 *
 * Note: This can be removed once Node.js 18 and 20 are no longer supported.
 *
 * @returns A tuple of `resolve`, `reject`, and `promise`.
 */
export function withResolvers<T>(): {
	resolve: (value: T) => void;
	reject: (reason: any) => void;
	promise: Promise<T>;
} {
	let resolve, reject;
	const promise = new Promise<T>((res, rej) => {
		resolve = res;
		reject = rej;
	});
	return { resolve, reject, promise };
}
