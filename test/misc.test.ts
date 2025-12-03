import { describe, expect, it } from 'vitest';
import { dbRunner } from './lib/util.js';

describe('Misc', () => {
	it('should get monotonic timestamp from database', () => dbRunner(async ({ db }) => {
		const now = Date.now();
		const ts1 = db.getMonotonicTimestamp();
		const ts2 = db.getMonotonicTimestamp();
		expect(ts1).toBeGreaterThan(now - 1000);
		expect(ts1).toBeLessThan(now + 1000);
		expect(ts2).toBeGreaterThan(ts1);
		expect(ts2).toBeLessThan(now + 1000);
	}));
});
