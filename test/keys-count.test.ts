import { dbRunner } from './lib/util.js';
import { describe, expect, it } from 'vitest';

describe('keys count', () => {
	it('should get the count of all keys', () =>
		dbRunner(async ({ db }) => {
			// note: this test does not do enough updates make the estimated count inaccurate
			for (let i = 0; i < 1000; ++i) {
				await db.put(`foo-${i}`, `bar-${i}`);
			}

			let exactCount = db.getKeysCount();
			expect(exactCount).toBe(1000);

			let estimatedCount = db.getEstimatedKeyCount();
			expect(estimatedCount).toBeLessThanOrEqual(1000);

			for (let i = 100; i < 600; ++i) {
				await db.remove(`foo-${i}`);
			}

			exactCount = db.getKeysCount();
			expect(exactCount).toBe(500);

			estimatedCount = db.getEstimatedKeyCount();
			expect(estimatedCount).toBeGreaterThanOrEqual(500);
			expect(estimatedCount).toBeLessThanOrEqual(1000);
		}));

	it('should get the count of keys in a range', () =>
		dbRunner(async ({ db }) => {
			for (let i = 0; i < 1000; ++i) {
				await db.put(`foo-${String(i).padStart(4, '0')}`, `bar-${i}`);
			}

			let exactCount = db.getKeysCount({ start: '' });
			expect(exactCount).toBe(1000);

			exactCount = db.getKeysCount({ start: 'foo-0100' });
			expect(exactCount).toBe(900);

			for (let i = 100; i < 600; ++i) {
				await db.remove(`foo-${String(i).padStart(4, '0')}`);
			}

			exactCount = db.getKeysCount({ start: 'foo-0100' });
			expect(exactCount).toBe(400); // 600 - 1000

			exactCount = db.getKeysCount({ start: 'foo-0100', end: 'foo-0600' });
			expect(exactCount).toBe(0);
		}));
});
