import { describe, expect, it } from 'vitest';
import { RocksDatabase } from '../src/rocks-database.js';

describe('basic functions', () => {
	it('should get the versions', async () => {
		const db = new RocksDatabase('test');
		await db.put('test', 'test');
		const value = await db.get('test');
		expect(value).toBe('test');
	});
});
