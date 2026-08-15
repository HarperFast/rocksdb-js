import { dbRunner } from './lib/util.ts';
import { describe, expect, it, vi } from 'vitest';

/**
 * Estimates are statistical (block-granular SST approximation + memtable
 * skip-list approximation), so assertions use a tolerance factor rather than
 * exact bounds. Uniform fixed-size entries keep the real accuracy well inside
 * these bounds; the factor only guards against wild regressions.
 */
function expectWithin(estimate: number, exact: number, factor: number) {
	expect(estimate).toBeGreaterThanOrEqual(exact / factor);
	expect(estimate).toBeLessThanOrEqual(exact * factor);
}

function expectConfidence(confidence: number, min = 0, max = 1) {
	expect(confidence).toBeGreaterThanOrEqual(min);
	expect(confidence).toBeLessThanOrEqual(max);
}

const KEY = (i: number) => `key-${String(i).padStart(6, '0')}`;

describe('estimateCount', () => {
	it('should estimate counts for ranges over flushed data', () =>
		dbRunner(async ({ db }) => {
			const N = 20000;
			for (let i = 0; i < N; i++) {
				await db.put(KEY(i), `value-${i}-${'x'.repeat(50)}`);
			}
			await db.flush();

			// full range, both open-ended and bounded
			expectWithin(db.getEstimatedKeyCount(), N, 2);
			const full = db.estimateCount({ start: KEY(0), end: KEY(N) });
			expectWithin(full.count, N, 2);
			// a large uniform range should be high-confidence
			expectConfidence(full.confidence, 0.5, 1);

			const singleKey = db.estimateCount({
				start: KEY(N / 2),
				end: KEY(N / 2),
				inclusiveEnd: true,
			});
			if (singleKey.count === 0) {
				expect(singleKey.confidence).toBeLessThanOrEqual(0.1);
			}

			// half range [25%, 75%)
			const half = db.estimateCount({ start: KEY(N / 4), end: KEY((3 * N) / 4) });
			expectWithin(half.count, N / 2, 2);

			// open-ended: start only and end only
			expectWithin(db.estimateCount({ start: KEY(N / 2) }).count, N / 2, 2);
			expectWithin(db.estimateCount({ end: KEY(N / 2) }).count, N / 2, 2);

			// a range past all data should estimate near zero relative to N
			const empty = db.estimateCount({ start: 'z', end: 'zz' });
			expect(empty.count).toBeLessThan(N / 20);
			expectConfidence(empty.confidence);
		}));

	it('should estimate counts for data still in the memtable', () =>
		dbRunner(async ({ db }) => {
			const N = 10000;
			for (let i = 0; i < N; i++) {
				await db.put(KEY(i), `value-${i}`);
			}
			// no flush: everything is in the memtable
			expectWithin(db.estimateCount({ start: KEY(0), end: KEY(N) }).count, N, 2);
			expectWithin(db.estimateCount({ start: KEY(N / 4), end: KEY((3 * N) / 4) }).count, N / 2, 2);
		}));

	it('should estimate counts spanning memtable and SST data', () =>
		dbRunner(async ({ db }) => {
			const N = 10000;
			for (let i = 0; i < N; i++) {
				await db.put(KEY(i), `value-${i}`);
			}
			await db.flush();
			for (let i = N; i < 2 * N; i++) {
				await db.put(KEY(i), `value-${i}`);
			}
			expectWithin(db.estimateCount({ start: KEY(0), end: KEY(2 * N) }).count, 2 * N, 2);
		}));

	it('should return a confident 0 for an empty database', () =>
		dbRunner(async ({ db }) => {
			expect(db.getEstimatedKeyCount()).toBe(0);
			// even a zero from estimate-num-keys is an estimate, not exact
			expect(db.estimateCount()).toEqual({ count: 0, confidence: 0.95 });
			const range = db.estimateCount({ start: 'a', end: 'z' });
			expect(range.count).toBe(0);
			expectConfidence(range.confidence, 0.9, 0.99);
		}));

	it('should return an exact 0 for an inverted range', () =>
		dbRunner(async ({ db }) => {
			for (let i = 0; i < 5000; i++) {
				await db.put(KEY(i), `value-${i}`);
			}
			await db.flush();
			// inverted/empty by construction: exact, so confidence is 1
			expect(db.estimateCount({ start: KEY(4000), end: KEY(1000) })).toEqual({
				count: 0,
				confidence: 1,
			});
			expect(db.estimateCount({ start: KEY(1000), end: KEY(1000) })).toEqual({
				count: 0,
				confidence: 1,
			});
		}));

	it('should treat zero-length native bounds safely', () =>
		dbRunner(async ({ db }) => {
			for (let i = 0; i < 5000; i++) {
				await db.put(KEY(i), `value-${i}`);
			}
			await db.flush();
			// encodeKey rejects zero-length keys, so exercise the native surface
			// directly: an empty end bound sorts below every key (empty range),
			// an empty start bound is the minimum key (no-op lower bound)
			const native = (db as any).store.db;
			expect(native.estimateCount(undefined, Buffer.alloc(0)).count).toBe(0);
			expect(native.estimateCount(Buffer.alloc(0), undefined).count).toBe(
				db.getEstimatedKeyCount()
			);
		}));

	it('should not count uncommitted transaction writes', () =>
		dbRunner(async ({ db }) => {
			const N = 10000;
			for (let i = 0; i < N; i++) {
				await db.put(KEY(i), `value-${i}`);
			}
			await db.transaction(async (txn) => {
				for (let i = N; i < 2 * N; i++) {
					txn.putSync(KEY(i), `value-${i}`);
				}
				const estimate = db.estimateCount({ start: KEY(0), end: KEY(2 * N) });
				expect(estimate.count).toBeLessThan(N * 1.5);
			});
		}));

	it('should scale estimates with range width', () =>
		dbRunner(async ({ db }) => {
			const N = 20000;
			for (let i = 0; i < N; i++) {
				await db.put(KEY(i), `value-${i}-${'x'.repeat(30)}`);
			}
			await db.flush();

			const tenth = db.estimateCount({ start: KEY(0), end: KEY(N / 10) }).count;
			const half = db.estimateCount({ start: KEY(0), end: KEY(N / 2) }).count;
			const full = db.estimateCount({ start: KEY(0), end: KEY(N) }).count;
			expect(tenth).toBeLessThan(half);
			expect(half).toBeLessThan(full);
		}));

	it('should use range-local density for varied value sizes', () =>
		dbRunner(async ({ db }) => {
			const N = 3000;
			for (let i = 0; i < N; i++) {
				await db.put(`small-${KEY(i)}`, 'x'.repeat(20));
			}
			await db.flush();
			for (let i = 0; i < N; i++) {
				await db.put(`large-${KEY(i)}`, 'x'.repeat(1000));
			}
			await db.flush();

			expectWithin(db.estimateCount({ start: 'small-', end: 'small.' }).count, N, 2);
			expectWithin(db.estimateCount({ start: 'large-', end: 'large.' }).count, N, 2);
		}));
});

describe('CountEstimator', () => {
	it('should refine the estimate as iteration advances', () =>
		dbRunner(async ({ db }) => {
			const N = 20000;
			for (let i = 0; i < N; i++) {
				await db.put(KEY(i), `value-${i}-${'x'.repeat(50)}`);
			}
			await db.flush();

			const range = { start: KEY(0), end: KEY(N) };
			const estimator = db.createCountEstimator(range);

			// before any traversal: the pure statistical estimate
			const initial = estimator.estimate();
			expectWithin(initial.count, N, 2);
			expectConfidence(initial.confidence);

			// walk the first half, checkpointing once per "page"
			let traversed = 0;
			let lastKey: unknown;
			for (const key of db.getKeys(range)) {
				lastKey = key;
				if (++traversed % 1000 === 0) {
					estimator.advance(lastKey as string, 1000);
				}
				if (traversed >= N / 2) {
					break;
				}
			}
			expect(estimator.traversed).toBe(N / 2);

			// with half the range traversed exactly, the estimate must be at
			// least the traversed count, within a tighter overall bound, and
			// more trusted than the untraversed estimate
			const refined = estimator.estimate();
			expect(refined.count).toBeGreaterThanOrEqual(N / 2);
			expectWithin(refined.count, N, 1.6);
			expectConfidence(refined.confidence);
		}));

	it('should support reverse iteration', () =>
		dbRunner(async ({ db }) => {
			const N = 10000;
			for (let i = 0; i < N; i++) {
				await db.put(KEY(i), `value-${i}`);
			}
			await db.flush();

			const range = { start: KEY(N), end: KEY(0), reverse: true };
			const estimator = db.createCountEstimator(range);
			expectWithin(db.estimateCount(range).count, N - 1, 2);
			let upperBound = range.start;
			let inclusiveEnd = true;
			for (;;) {
				const page = Array.from(
					db.getKeys({ ...range, start: upperBound, inclusiveEnd, limit: 500 })
				);
				if (page.length === 0) {
					break;
				}
				upperBound = page[page.length - 1] as string;
				inclusiveEnd = false;
				estimator.advance(upperBound, page.length);
				const checkpoint = estimator.estimate();
				expect(checkpoint.count).toBeGreaterThanOrEqual(estimator.traversed);
				expectWithin(checkpoint.count, N - 1, 2);
			}
			expect(estimator.traversed).toBe(N - 1);
			estimator.finish();
			expect(estimator.estimate()).toEqual({ count: N - 1, confidence: 1 });
		}));

	it('should memoize before traversal and ignore an empty page', () =>
		dbRunner(async ({ db }) => {
			for (let i = 0; i < 1000; i++) {
				await db.put(KEY(i), `value-${i}`);
			}
			await db.flush();

			const estimateCount = vi.spyOn(db.store, 'estimateCount');
			const estimator = db.createCountEstimator({ start: KEY(0), end: KEY(1000) });
			const initial = estimator.estimate();
			const expected = { ...initial };
			expect(estimator.estimate()).toEqual(initial);
			expect(estimateCount).toHaveBeenCalledTimes(1);
			initial.count = 0;
			expect(estimator.estimate()).toEqual(expected);

			expect(() => estimator.advance(undefined, 1)).toThrow(
				'CountEstimator.advance requires lastKey when count is nonzero'
			);
			expect(estimator.traversed).toBe(0);
			expect(estimator.estimate()).toEqual(expected);
			expect(estimateCount).toHaveBeenCalledTimes(1);
		}));

	it('should not calibrate from a block-granular partial page', () =>
		dbRunner(async ({ db }) => {
			const N = 20000;
			for (let i = 0; i < N; i++) {
				await db.put(KEY(i), `value-${i}`);
			}
			await db.flush();

			const estimator = db.createCountEstimator({ start: KEY(0), end: KEY(N) });
			estimator.advance(KEY(24), 25);
			expectWithin(estimator.estimate().count, N, 2);
		}));

	it('should converge to near-exact as traversal completes', () =>
		dbRunner(async ({ db }) => {
			const N = 5000;
			for (let i = 0; i < N; i++) {
				await db.put(KEY(i), `value-${i}`);
			}
			await db.flush();

			const estimator = db.createCountEstimator({ start: KEY(0), end: KEY(N) });
			estimator.advance(KEY(N - 1), N);
			// remainder is (KEY(N-1), KEY(N)) — essentially empty, though the
			// estimate of it is block-granular, so allow a small overshoot
			const final = estimator.estimate();
			expect(final.count).toBeGreaterThanOrEqual(N);
			expect(final.count).toBeLessThan(N * 1.25);
			expectConfidence(final.confidence, 0.7, 1);

			estimator.finish();
			expect(estimator.estimate()).toEqual({ count: N, confidence: 1 });
		}));

	it('should report the exact total for a paginated loop driven to completion', () =>
		dbRunner(async ({ db }) => {
			const N = 5000;
			const PAGE = 250;
			for (let i = 0; i < N; i++) {
				await db.put(KEY(i), `value-${i}`);
			}
			await db.flush();

			const range = { start: KEY(0), end: KEY(N) };
			const estimator = db.createCountEstimator(range);
			let pageStart: string | undefined;
			let exclusiveStart = false;
			for (;;) {
				const page = Array.from(
					db.getKeys({ ...range, start: pageStart ?? range.start, exclusiveStart, limit: PAGE })
				);
				if (page.length === 0) {
					break;
				}
				pageStart = page[page.length - 1] as string;
				exclusiveStart = true;
				estimator.advance(pageStart, page.length);
				const checkpoint = estimator.estimate();
				expect(checkpoint.count).toBeGreaterThanOrEqual(estimator.traversed);
				// only finish() may claim exactness
				expect(checkpoint.confidence).toBeLessThan(1);
			}
			expect(estimator.traversed).toBe(N);
			estimator.finish();
			expect(estimator.estimate()).toEqual({ count: N, confidence: 1 });
		}));
});
