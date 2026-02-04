import { existsSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { dbRunner } from './lib/util.js';

describe('Destroy', () => {
	it('should destroy a closed database', () =>
		dbRunner(async ({ db, dbPath }) => {
			expect(db.isOpen()).toBe(true);
			db.close();
			expect(existsSync(dbPath)).toBe(true);
			expect(db.isOpen()).toBe(false);
			db.destroy();
			expect(existsSync(dbPath)).toBe(false);
			expect(db.isOpen()).toBe(false);
		}));

	it('should destroy an open database', () =>
		dbRunner(({ db, dbPath }) => {
			db.putSync('key', 'value');
			expect(db.getSync('key')).toBe('value');
			db.destroy();
			expect(existsSync(dbPath)).toBe(false);
			expect(db.isOpen()).toBe(false);
		}));

	it('should destroy all related instances', () =>
		dbRunner(
			{ dbOptions: [{}, { name: 'test' }] },
			async ({ db: db1, dbPath: dbPath1 }, { db: db2, dbPath: dbPath2 }) => {
				expect(existsSync(dbPath1)).toBe(true);
				expect(existsSync(dbPath2)).toBe(true);
				expect(db1.isOpen()).toBe(true);
				expect(db2.isOpen()).toBe(true);

				db1.destroy();

				expect(existsSync(dbPath1)).toBe(false);
				expect(existsSync(dbPath2)).toBe(false);
				expect(db1.isOpen()).toBe(false);
				expect(db2.isOpen()).toBe(false);
			}
		));
});
