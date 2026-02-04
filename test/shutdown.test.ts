import { describe, expect, it } from 'vitest';
import { dbRunner } from './lib/util.js';
import { registryStatus, shutdown } from '../src/index.js';

describe('Shutdown', () => {
	it('should shutdown rocksdb-js', () =>
		dbRunner({
			dbOptions: [{}, { name: 'test' }],
		}, async ({ db }, { db: db2 }) => {
			expect(db.isOpen()).toBe(true);
			expect(db2.isOpen()).toBe(true);
			let status = registryStatus();
			expect(status.length).toBe(1);
			expect(status[0].columnFamilies.length).toBe(2);
			shutdown();
			expect(db.isOpen()).toBe(false);
			expect(db2.isOpen()).toBe(false);
			status = registryStatus();
			expect(status.length).toBe(0);
		}));

	it('should handle multiple shutdowns', () =>
		dbRunner(async ({ db }) => {
			expect(db.isOpen()).toBe(true);
			let status = registryStatus();
			expect(status.length).toBe(1);
			expect(status[0].columnFamilies.length).toBe(1);
			shutdown();
			expect(db.isOpen()).toBe(false);
			status = registryStatus();
			expect(status.length).toBe(0);
			shutdown();
			expect(db.isOpen()).toBe(false);
			status = registryStatus();
			expect(status.length).toBe(0);
		}));
});
