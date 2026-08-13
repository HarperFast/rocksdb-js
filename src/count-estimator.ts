import type { RangeOptions } from './dbi.ts';
import type { Key } from './encoding.ts';
import type { Store } from './store.ts';

export interface CountEstimatorOptions extends RangeOptions {
	/**
	 * When `true`, iteration proceeds from `end` toward `start`, so keys
	 * passed to `advance()` are treated as the new lower edge of the
	 * untraversed remainder.
	 */
	reverse?: boolean;
}

/**
 * Below this many traversed entries the observed density is too noisy to
 * calibrate with, so `estimate()` returns the raw statistical estimate.
 */
const CALIBRATION_MIN_TRAVERSED = 16;

/**
 * Bounds on how far the observed-vs-estimated ratio may scale the remainder;
 * the statistical estimate can be arbitrarily wrong at data-block granularity
 * and an unclamped ratio would let one bad sample dominate.
 */
const CALIBRATION_MAX = 8;

/**
 * Progressively refines a range key-count estimate as an iterator traverses
 * the range. The estimate starts as the pure statistical estimate
 * (`estimateCount`) and converges toward the exact count: the traversed
 * portion is exact, and the statistical estimate of the untraversed remainder
 * is calibrated by the observed ratio of actual-to-estimated entries over the
 * portion already traversed.
 *
 * The estimator never touches the iterator itself — the caller reports
 * progress with `advance(lastKey, count)` whenever it wants a checkpoint
 * (e.g. once per page), then reads `estimate()`.
 */
export class CountEstimator {
	#store: Store;
	#start: Key | Uint8Array | undefined;
	#end: Key | Uint8Array | undefined;
	#reverse: boolean;
	#cursor: Key | Uint8Array | undefined;
	#traversed = 0;

	constructor(store: Store, options?: CountEstimatorOptions) {
		this.#store = store;
		this.#start = options?.start;
		this.#end = options?.end;
		this.#reverse = options?.reverse ?? false;
	}

	/**
	 * The number of entries reported traversed so far.
	 */
	get traversed(): number {
		return this.#traversed;
	}

	/**
	 * Records that iteration has advanced through `count` more entries, ending
	 * at `lastKey`.
	 */
	advance(lastKey: Key | Uint8Array, count = 1): void {
		this.#cursor = lastKey;
		this.#traversed += count;
	}

	/**
	 * Estimates the total number of entries in the full range: the exact
	 * traversed count plus a calibrated statistical estimate of the remainder.
	 */
	estimate(): number {
		if (this.#cursor === undefined) {
			return this.#store.estimateCount({ start: this.#start, end: this.#end });
		}

		const traversedRange = this.#reverse
			? { start: this.#cursor, end: this.#end }
			: { start: this.#start, end: this.#cursor };
		const remainingRange = this.#reverse
			? { start: this.#start, end: this.#cursor }
			: { start: this.#cursor, end: this.#end };

		let remaining = this.#store.estimateCount(remainingRange);
		if (this.#traversed >= CALIBRATION_MIN_TRAVERSED) {
			const traversedEstimate = this.#store.estimateCount(traversedRange);
			const calibration = Math.min(
				CALIBRATION_MAX,
				Math.max(1 / CALIBRATION_MAX, this.#traversed / Math.max(traversedEstimate, 1))
			);
			remaining *= calibration;
		}

		return Math.round(this.#traversed + remaining);
	}
}
