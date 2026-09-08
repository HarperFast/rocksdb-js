import { dbRunner } from './lib/util.ts';
import { describe, expect, it } from 'vitest';

describe('Lifecycle', () => {
	it('should open and close database', () =>
		dbRunner({ skipOpen: true }, async ({ db }) => {
			expect(db.isOpen()).toBe(false);
			expect(db.status).toBe('closed');

			db.close(); // noop

			db.open();
			db.open(); // noop

			expect(db.isOpen()).toBe(true);
			expect(db.status).toBe('open');
			expect(db.get('foo')).toBeUndefined();

			db.close();

			expect(db.isOpen()).toBe(false);
			expect(db.status).toBe('closed');

			await expect(db.get('foo')).rejects.toThrow('Database not open');
		}));

	it('should open and close multiple databases', () =>
		dbRunner(
			{ dbOptions: [{}, { name: 'foo' }, { name: 'foo' }], skipOpen: true },
			async ({ db }, { db: db2 }, { db: db3 }) => {
				expect(db.isOpen()).toBe(false);
				expect(db2.isOpen()).toBe(false);
				expect(db3.isOpen()).toBe(false);

				db.close(); // noop
				db2.close(); // noop
				db3.close(); // noop

				db.open();
				db.open(); // noop

				db2.open();
				db2.open(); // noop

				db3.open();
				db3.open(); // noop

				expect(db.isOpen()).toBe(true);
				expect(db.get('foo')).toBeUndefined();

				expect(db2.isOpen()).toBe(true);
				expect(db2.get('foo')).toBeUndefined();

				expect(db3.isOpen()).toBe(true);
				expect(db3.get('foo')).toBeUndefined();

				db.close();
				db2.close();
				db3.close();

				expect(db.isOpen()).toBe(false);
				expect(db2.isOpen()).toBe(false);
				expect(db3.isOpen()).toBe(false);

				await expect(db.get('foo')).rejects.toThrow('Database not open');
				await expect(db2.get('foo')).rejects.toThrow('Database not open');
				await expect(db3.get('foo')).rejects.toThrow('Database not open');
			}
		));

	// close() publishes async-work cancellation on the handle, and every async
	// admission (get, flush, compact, clear, backup, checkpoint) is refused while
	// it stands. Reopening the same instance has to clear it, or a documented
	// reopen would come back with every async operation permanently rejected as
	// "Database is closing".
	it('accepts async work again after a close/open cycle on the same instance', () =>
		dbRunner(async ({ db }) => {
			db.putSync('before', 'close');
			await db.flush();
			db.close();

			db.open();
			expect(db.isOpen()).toBe(true);
			db.putSync('after', 'reopen');
			await expect(db.flush()).resolves.toBeUndefined();
			await expect(db.compact()).resolves.toBeUndefined();
			expect(await db.get('before')).toBe('close');
			expect(await db.get('after')).toBe('reopen');
		}));
});
