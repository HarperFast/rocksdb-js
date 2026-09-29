import { getRegistryStatus, registryStatus } from '../src/index.ts';
import { dbRunner } from './lib/util.ts';
import { describe, expect, it } from 'vitest';

describe('getRegistryStatus()', () => {
	it('should keep registryStatus() as an alias of getRegistryStatus()', () => {
		expect(typeof getRegistryStatus).toBe('function');
		expect(registryStatus).toBe(getRegistryStatus);
	});

	it('should report open databases under both names', () =>
		dbRunner({ dbOptions: [{}, { name: 'test' }] }, async ({ db }, { db: db2 }) => {
			expect(db2.isOpen()).toBe(true);
			const status = getRegistryStatus();
			const entry = status.find((e) => e.path === db.path);
			expect(entry).toBeDefined();
			expect(Object.keys(entry!.columnFamilies).sort()).toEqual(['default', 'test']);
			expect(registryStatus()).toEqual(status);
		}));
});
