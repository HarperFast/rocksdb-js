import * as rocksdb from '../src/index.ts';
import { getRegistryStatus } from '../src/index.ts';
import { dbRunner } from './lib/util.ts';
import { describe, expect, it } from 'vitest';

describe('getRegistryStatus()', () => {
	it('should no longer export the removed registryStatus() alias', () => {
		expect(typeof getRegistryStatus).toBe('function');
		expect('registryStatus' in rocksdb).toBe(false);
	});

	it('should report open databases', () =>
		dbRunner({ dbOptions: [{}, { name: 'test' }] }, async ({ db }, { db: db2 }) => {
			expect(db2.isOpen()).toBe(true);
			const status = getRegistryStatus();
			const entry = status.find((e) => e.path === db.path);
			expect(entry).toBeDefined();
			expect(Object.keys(entry!.columnFamilies).sort()).toEqual(['default', 'test']);
		}));
});
