import { dbRunner } from './lib/util.js';
import { describe, expect, it } from 'vitest';

describe('keys count', () => {
	it('should get the estimated number of all keys', () =>
		dbRunner(async ({ db }) => {
			// note: this test does not do enough updates make the estimated count inaccurate
			for (let i = 0; i < 1000; ++i) {
				await db.put(`foo-${i}`, `bar-${i}`);
			}

			let estimatedCount = db.getKeysCount();
			expect(estimatedCount).toBeLessThanOrEqual(1000);

			for (let i = 100; i < 600; ++i) {
				await db.remove(`foo-${i}`);
			}

			estimatedCount = db.getKeysCount();
			expect(estimatedCount).toBeGreaterThanOrEqual(500);
			expect(estimatedCount).toBeLessThanOrEqual(1000);
		}));

	it('should get the number of all keys', () =>
		dbRunner(async ({ db }) => {
			for (let i = 0; i < 1000; ++i) {
				await db.put(`foo-${i}`, `bar-${i}`);
			}

			let actualCount = db.getKeysCount({ start: null });
			expect(actualCount).toBe(1000);

			for (let i = 100; i < 600; ++i) {
				await db.remove(`foo-${i}`);
			}

			actualCount = db.getKeysCount({ start: null });
			expect(actualCount).toBe(500);
		}));
});
