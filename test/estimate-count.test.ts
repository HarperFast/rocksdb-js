import { dbRunner } from './lib/util.ts';
import { describe, expect, it } from 'vitest';

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
			expectWithin(db.getEstimatedKeyCount({ start: KEY(0), end: KEY(N) }), N, 2);

			// half range [25%, 75%)
			const half = db.getEstimatedKeyCount({ start: KEY(N / 4), end: KEY((3 * N) / 4) });
			expectWithin(half, N / 2, 2);

			// open-ended: start only and end only
			expectWithin(db.getEstimatedKeyCount({ start: KEY(N / 2) }), N / 2, 2);
			expectWithin(db.getEstimatedKeyCount({ end: KEY(N / 2) }), N / 2, 2);

			// a range past all data should estimate near zero relative to N
			const empty = db.getEstimatedKeyCount({ start: 'z', end: 'zz' });
			expect(empty).toBeLessThan(N / 20);
		}));

	it('should estimate counts for data still in the memtable', () =>
		dbRunner(async ({ db }) => {
			const N = 10000;
			for (let i = 0; i < N; i++) {
				await db.put(KEY(i), `value-${i}`);
			}
			// no flush: everything is in the memtable
			expectWithin(db.getEstimatedKeyCount({ start: KEY(0), end: KEY(N) }), N, 2);
			expectWithin(db.getEstimatedKeyCount({ start: KEY(N / 4), end: KEY((3 * N) / 4) }), N / 2, 2);
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
			expectWithin(db.getEstimatedKeyCount({ start: KEY(0), end: KEY(2 * N) }), 2 * N, 2);
		}));

	it('should return 0 for an empty database', () =>
		dbRunner(async ({ db }) => {
			expect(db.getEstimatedKeyCount()).toBe(0);
			expect(db.getEstimatedKeyCount({ start: 'a', end: 'z' })).toBe(0);
		}));

	it('should return 0 for an inverted range', () =>
		dbRunner(async ({ db }) => {
			for (let i = 0; i < 5000; i++) {
				await db.put(KEY(i), `value-${i}`);
			}
			await db.flush();
			expect(db.getEstimatedKeyCount({ start: KEY(4000), end: KEY(1000) })).toBe(0);
			expect(db.getEstimatedKeyCount({ start: KEY(1000), end: KEY(1000) })).toBe(0);
		}));

	it('should scale estimates with range width', () =>
		dbRunner(async ({ db }) => {
			const N = 20000;
			for (let i = 0; i < N; i++) {
				await db.put(KEY(i), `value-${i}-${'x'.repeat(30)}`);
			}
			await db.flush();

			const tenth = db.getEstimatedKeyCount({ start: KEY(0), end: KEY(N / 10) });
			const half = db.getEstimatedKeyCount({ start: KEY(0), end: KEY(N / 2) });
			const full = db.getEstimatedKeyCount({ start: KEY(0), end: KEY(N) });
			expect(tenth).toBeLessThan(half);
			expect(half).toBeLessThan(full);
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
			expectWithin(estimator.estimate(), N, 2);

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
			// least the traversed count and within a tighter overall bound
			const refined = estimator.estimate();
			expect(refined).toBeGreaterThanOrEqual(N / 2);
			expectWithin(refined, N, 1.6);
		}));

	it('should support reverse iteration', () =>
		dbRunner(async ({ db }) => {
			const N = 10000;
			for (let i = 0; i < N; i++) {
				await db.put(KEY(i), `value-${i}`);
			}
			await db.flush();

			const estimator = db.createCountEstimator({ start: KEY(0), end: KEY(N), reverse: true });
			// walk the last quarter in reverse
			estimator.advance(KEY((3 * N) / 4), N / 4);
			const refined = estimator.estimate();
			expect(refined).toBeGreaterThanOrEqual(N / 4);
			expectWithin(refined, N, 2);
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
			expect(final).toBeGreaterThanOrEqual(N);
			expect(final).toBeLessThan(N * 1.25);

			estimator.finish();
			expect(estimator.estimate()).toBe(N);
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
				expect(estimator.estimate()).toBeGreaterThanOrEqual(estimator.traversed);
			}
			expect(estimator.traversed).toBe(N);
			estimator.finish();
			expect(estimator.estimate()).toBe(N);
		}));
});
