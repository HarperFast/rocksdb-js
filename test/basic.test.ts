import { describe, expect, it } from 'vitest';
import { RocksDatabase } from '../src/index.js';

describe('basic functions', () => {
	it('should get the versions', async () => {
		let db = new RocksDatabase('/tmp/testdb');
		await db.put('test', 'test');
		const value = await db.get('test');
		expect(value).toBe('test');
	});
});
