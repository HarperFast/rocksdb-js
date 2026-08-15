import type { CountEstimate, CountEstimateOptions } from './dbi.ts';
import type { Key } from './encoding.ts';
import type { Store } from './store.ts';

export interface CountEstimatorOptions extends CountEstimateOptions {}

/**
 * Below this many traversed entries the observed density is too noisy to
 * calibrate with, so `estimate()` returns the raw statistical estimate.
 */
const CALIBRATION_MIN_TRAVERSED = 16;
const CALIBRATION_MIN_CONFIDENCE = 0.8;

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
 * (e.g. once per page), then reads `estimate()`. The caller owns the
 * progress contract: cursors must move monotonically through the range and
 * each entry must be reported once (a re-reported page inflates the count
 * undetectably). When traversal completes, call `finish()` so `estimate()`
 * returns the exact total instead of adding a block-granular remainder.
 *
 * Each `estimate()` checkpoint queries RocksDB statistics for the two range
 * segments (cost scales with the SSTs overlapping them, not with key count);
 * results are memoized per checkpoint, so repeated reads between `advance()`
 * calls are free.
 */
export class CountEstimator {
	#store: Store;
	#start: Key | Uint8Array | undefined;
	#end: Key | Uint8Array | undefined;
	#exclusiveStart: boolean;
	#inclusiveEnd: boolean;
	#reverse: boolean;
	#cursor: Key | Uint8Array | undefined;
	#traversed = 0;
	#finished = false;
	#memoized: CountEstimate | undefined;

	constructor(store: Store, options?: CountEstimatorOptions) {
		this.#store = store;
		this.#reverse = options?.reverse ?? false;
		this.#start = this.#reverse ? options?.end : options?.start;
		this.#end = this.#reverse ? options?.start : options?.end;
		this.#exclusiveStart = options?.exclusiveStart ?? this.#reverse;
		this.#inclusiveEnd = options?.inclusiveEnd ?? this.#reverse;
	}

	/**
	 * The number of entries reported traversed so far.
	 */
	get traversed(): number {
		return this.#traversed;
	}

	/**
	 * Records that iteration has advanced through `count` more entries, ending
	 * at `lastKey`. Pass `count: 0` when an empty page has no last key.
	 */
	advance(lastKey: Key | Uint8Array | undefined, count = 1): void {
		if (lastKey === undefined) {
			if (count !== 0) {
				throw new Error('CountEstimator.advance requires lastKey when count is nonzero');
			}
			return;
		}
		this.#cursor = lastKey;
		this.#traversed += count;
		this.#memoized = undefined;
	}

	/**
	 * Marks traversal of the range as complete: `estimate()` becomes the exact
	 * traversed count.
	 */
	finish(): void {
		this.#finished = true;
	}

	/**
	 * Estimates the total number of entries in the full range: the exact
	 * traversed count plus a calibrated statistical estimate of the remainder.
	 * `confidence` is the exactness-weighted blend of the traversed portion
	 * (exact) and the remainder's statistical confidence, so it converges to 1
	 * as traversal proceeds (and is exactly 1 after `finish()`).
	 */
	estimate(): CountEstimate {
		if (this.#finished) {
			return { count: this.#traversed, confidence: 1 };
		}
		if (this.#memoized !== undefined) {
			return { ...this.#memoized };
		}
		if (this.#cursor === undefined) {
			this.#memoized = this.#store.estimateCount({
				start: this.#start,
				end: this.#end,
				exclusiveStart: this.#exclusiveStart,
				inclusiveEnd: this.#inclusiveEnd,
			});
			return { ...this.#memoized };
		}

		// The cursor entry itself belongs to the traversed side, so the
		// remainder excludes it in both directions (an inclusive lower bound
		// forward would count it twice and block convergence). The range's own
		// bound flags stay with their original edge of the full range.
		const traversedRange = this.#reverse
			? { start: this.#cursor, end: this.#end, inclusiveEnd: this.#inclusiveEnd }
			: {
					start: this.#start,
					exclusiveStart: this.#exclusiveStart,
					end: this.#cursor,
					inclusiveEnd: true,
				};
		const remainingRange = this.#reverse
			? { start: this.#start, exclusiveStart: this.#exclusiveStart, end: this.#cursor }
			: {
					start: this.#cursor,
					exclusiveStart: true,
					end: this.#end,
					inclusiveEnd: this.#inclusiveEnd,
				};

		const remainingEstimate = this.#store.estimateCount(remainingRange);
		let remaining = remainingEstimate.count;
		let calibrationConfidence = 1;
		if (this.#traversed >= CALIBRATION_MIN_TRAVERSED) {
			const traversedEstimate = this.#store.estimateCount(traversedRange);
			if (
				traversedEstimate.count >= CALIBRATION_MIN_TRAVERSED &&
				traversedEstimate.confidence >= CALIBRATION_MIN_CONFIDENCE
			) {
				const calibration = Math.min(
					CALIBRATION_MAX,
					Math.max(1 / CALIBRATION_MAX, this.#traversed / traversedEstimate.count)
				);
				remaining *= calibration;
				calibrationConfidence = Math.min(calibration, 1 / calibration);
			}
		}

		const count = Math.round(this.#traversed + remaining);
		// Cap below 1: only finish() (or an exact-by-construction range) may
		// claim exactness, even when rounding makes the remainder vanish.
		const confidence =
			count > 0
				? Math.min(
						0.999,
						(this.#traversed + calibrationConfidence * remainingEstimate.confidence * remaining) /
							count
					)
				: Math.min(0.999, calibrationConfidence * remainingEstimate.confidence);
		this.#memoized = { count, confidence };
		return { ...this.#memoized };
	}
}
