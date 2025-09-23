import { describe, expect, it } from 'vitest';
import { dbRunner } from './lib/util.js';

describe('Column Families', () => {
	it('should open multiple column families', () => dbRunner({
		dbOptions: [ {}, { name: 'foo' } ],
	}, async ({ db }, { db: db2 }) => {
		await db.put('foo', 'bar');
		await db2.put('foo', 'bar2');
		expect(db.get('foo')).toBe('bar');
		expect(db2.get('foo')).toBe('bar2');
	}));

	it('should reuse same instance for same column family', () => dbRunner({
		dbOptions: [ { name: 'foo' }, { name: 'foo' } ],
	}, async ({ db }, { db: db2 }) => {
		await db.put('foo', 'bar');
		expect(db.get('foo')).toBe('bar');
		expect(db2.get('foo')).toBe('bar');
	}));
});
