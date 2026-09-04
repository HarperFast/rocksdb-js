import { RocksDatabase, Store } from '../src/index.ts';
import { dbRunner, generateDBPath } from './lib/util.ts';
import { rmSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

describe('Column Families', () => {
	it('should open multiple column families', () =>
		dbRunner({ dbOptions: [{}, { name: 'foo' }] }, async ({ db }, { db: db2 }) => {
			await db.put('foo', 'bar');
			await db2.put('foo', 'bar2');
			expect(db.get('foo')).toBe('bar');
			expect(db2.get('foo')).toBe('bar2');
			expect(db.name).toBe('default');
			expect(db2.name).toBe('foo');
			expect(db.columns).toEqual(['default', 'foo']);
			expect(db2.columns).toEqual(['default', 'foo']);
		}));

	it('should reuse same instance for same column family', () =>
		dbRunner({ dbOptions: [{ name: 'foo' }, { name: 'foo' }] }, async ({ db }, { db: db2 }) => {
			await db.put('foo', 'bar');
			expect(db.get('foo')).toBe('bar');
			expect(db2.get('foo')).toBe('bar');
			expect(db.name).toBe('foo');
			expect(db2.name).toBe('foo');
			expect(db.columns).toEqual(['default', 'foo']);
			expect(db2.columns).toEqual(['default', 'foo']);
		}));

	it('should get column families', () =>
		dbRunner({ skipOpen: true, dbOptions: [{}, { name: 'foo' }] }, async ({ db }, { db: db2 }) => {
			db.open();
			expect(db.columns).toEqual(['default']);
			db2.open();
			expect(db.columns).toEqual(['default', 'foo']);
			expect(db2.columns).toEqual(['default', 'foo']);
		}));
});

describe('Column Family Views (use)', () => {
	it('should bind a view to a column family with isolated data', () =>
		dbRunner(async ({ db }) => {
			const events = db.use('events');
			expect(events).toBeInstanceOf(RocksDatabase);
			expect(events.name).toBe('events');
			expect(db.name).toBe('default');

			await db.put('k', 'default-value');
			await events.put('k', 'events-value');

			// Same key, different column families -> different values.
			expect(db.get('k')).toBe('default-value');
			expect(events.get('k')).toBe('events-value');

			// The view auto-created the column family.
			expect(db.columns).toEqual(['default', 'events']);
		}));

	it('should cache views by name', () =>
		dbRunner(async ({ db }) => {
			const a = db.use('events');
			const b = db.use('events');
			expect(a).toBe(b);
		}));

	it('should return this for the database own column family', () =>
		dbRunner(async ({ db }) => {
			expect(db.use('default')).toBe(db);
			// The own-name shortcut ignores options rather than forking a handle.
			expect(db.use('default', { compression: 'zstd' })).toBe(db);
		}));

	it('should return this for a non-default bound column family', () =>
		dbRunner({ dbOptions: [{ name: 'foo' }] }, async ({ db }) => {
			expect(db.name).toBe('foo');
			expect(db.use('foo')).toBe(db);
		}));

	it('should open this when use() names the own column family on an unopened db', () =>
		dbRunner({ skipOpen: true }, async ({ db }) => {
			expect(db.isOpen()).toBe(false);
			const self = db.use('default');
			expect(self).toBe(db);
			expect(db.isOpen()).toBe(true); // shortcut honored the create-and-open contract
			await db.put('k', 'v');
			expect(db.get('k')).toBe('v');
		}));

	it('should derive the bound name from a custom Store', () => {
		const path = generateDBPath();
		const db = new RocksDatabase(new Store(path, { name: 'audit' })).open();
		try {
			expect(db.name).toBe('audit');
			// use() with the store's own name returns this, not a duplicate handle.
			expect(db.use('audit')).toBe(db);
			// ...and 'default' is a distinct view, not this audit-bound handle.
			const def = db.use('default');
			expect(def).not.toBe(db);
			expect(def.name).toBe('default');
			def.close();
		} finally {
			db.close();
			rmSync(path, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
		}
	});

	it('should share underlying data with a separately-opened handle', () => {
		const path = generateDBPath();
		const db = RocksDatabase.open(path);
		const other = RocksDatabase.open(path, { name: 'events' });
		const events = db.use('events');
		try {
			other.put('k', 'written-via-other');
			// Both handles pin the same shared descriptor/column family.
			expect(events.get('k')).toBe('written-via-other');
		} finally {
			events.close();
			other.close();
			db.close();
			rmSync(path, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
		}
	});

	it('should keep views open independently of the parent', () =>
		dbRunner({ skipOpen: true }, async ({ db }) => {
			db.open();
			const events = db.use('events');
			await events.put('k', 'v');

			// Views are independent handles sharing the same descriptor: closing
			// the parent does not close them (and vice versa).
			db.close();
			expect(db.isOpen()).toBe(false);
			expect(events.isOpen()).toBe(true);
			expect(events.get('k')).toBe('v');

			events.close();
			expect(events.isOpen()).toBe(false);
		}));

	// The weak cache's recreate-on-stale branch is exercised deterministically via
	// an explicitly-closed view below. A GC-collection assertion is intentionally
	// NOT tested here: forcing collection of the napi-wrapped view is unreliable
	// under the test runner (see AGENTS.md on GC-dependent tests) even though the
	// view is genuinely collectible — the cache holds only a WeakRef and a
	// FinalizationRegistry, neither of which pins it.
	it('should recreate a view that was explicitly closed', () =>
		dbRunner(async ({ db }) => {
			const events = db.use('events');
			await events.put('k', 'v');
			events.close();

			// A closed-but-referenced view is not handed back; use() recreates it.
			const reopened = db.use('events');
			expect(reopened).not.toBe(events);
			expect(reopened.isOpen()).toBe(true);
			expect(reopened.get('k')).toBe('v');
		}));

	it('should reject an invalid column family name', () =>
		dbRunner(async ({ db }) => {
			expect(() => db.use('')).toThrow('Column family name must be a non-empty string');
			// @ts-expect-error - intentionally invalid
			expect(() => db.use(123)).toThrow('Column family name must be a non-empty string');
		}));

	it('should open a view from an unopened parent', () =>
		dbRunner({ skipOpen: true }, async ({ db }) => {
			expect(db.isOpen()).toBe(false);
			const events = db.use('events');
			expect(events.isOpen()).toBe(true);
			await events.put('foo', 'bar');
			expect(events.get('foo')).toBe('bar');
		}));

	it('should yield a valid view when the parent is discarded', async () => {
		const path = generateDBPath();
		// The parent is a throwaway; the returned view opens its own handle and
		// remains valid after the parent is collected.
		const events = (() => new RocksDatabase(path).use('events'))();
		try {
			await events.put('foo', 'bar');
			expect(events.get('foo')).toBe('bar');
		} finally {
			events.close();
			rmSync(path, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
		}
	});

	it('should transact independently per view', () =>
		dbRunner(async ({ db }) => {
			const events = db.use('events');
			await db.transaction(async (txn) => {
				txn.putSync('t', 'default-txn');
			});
			await events.transaction(async (txn) => {
				txn.putSync('t', 'events-txn');
			});
			expect(db.get('t')).toBe('default-txn');
			expect(events.get('t')).toBe('events-txn');
		}));
});
