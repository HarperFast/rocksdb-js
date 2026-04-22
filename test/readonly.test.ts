import { TransactionLog } from '../src/load-binding.js';
import { dbRunner } from './lib/util.js';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

describe('Readonly Operations', () => {
	it('should error opening a readonly database that does not exist', () =>
		dbRunner({ skipOpen: true, dbOptions: [{ readOnly: true }] }, async ({ db }) => {
			expect(() => db.open()).toThrow('Database does not exist');
		}));

	it('should error write operations and transactions in readonly mode', () =>
		dbRunner(
			{ skipOpen: true, dbOptions: [{}, { readOnly: true }] },
			async ({ db }, { db: db2 }) => {
				db.open();
				db2.open();

				// clear
				await expect(db2.clear()).rejects.toThrow('Database is opened in readonly mode');
				expect(() => db2.clearSync()).toThrow('Database is opened in readonly mode');

				// destroy
				expect(() => db2.destroy()).toThrow('Database is opened in readonly mode');

				// drop
				await expect(db2.drop()).rejects.toThrow('Database is opened in readonly mode');
				expect(() => db2.dropSync()).toThrow('Database is opened in readonly mode');

				// flush
				await expect(db2.flush()).rejects.toThrow('Database is opened in readonly mode');
				expect(() => db2.flushSync()).toThrow('Database is opened in readonly mode');

				// purgeLogs
				expect(() => db2.purgeLogs()).toThrow('Database is opened in readonly mode');

				// put
				await expect(db2.put('foo', 'bar')).rejects.toThrow('Database is opened in readonly mode');
				expect(() => db2.putSync('foo', 'bar')).toThrow('Database is opened in readonly mode');

				// remove
				await expect(db2.remove('foo')).rejects.toThrow('Database is opened in readonly mode');
				expect(() => db2.removeSync('foo')).toThrow('Database is opened in readonly mode');

				// transaction
				await expect(db2.transaction(() => {})).rejects.toThrow(
					'Database is opened in readonly mode'
				);
				expect(() => db2.transactionSync(() => {})).toThrow('Database is opened in readonly mode');

				// useLog
				expect(() => db2.useLog('foo')).toThrow('Database is opened in readonly mode');
			}
		));

	it('should not see changes in readonly mode', () =>
		dbRunner(
			{ skipOpen: true, dbOptions: [{}, { readOnly: true }] },
			async ({ db }, { db: db2 }) => {
				// create the database
				db.open();
				expect(db.readOnly).toBe(false);
				db.putSync('foo', 'bar');

				db2.open();
				expect(db2.readOnly).toBe(true);
				expect(db2.getSync('foo')).toBe('bar');

				// change the value
				db.putSync('foo', 'baz');
				db.close(); // flush on close

				// db2 is still referencing an old snapshot
				expect(db2.getSync('foo')).toBe('bar');
			}
		));

	it('should throw if trying to create a transaction log in readonly mode', () =>
		dbRunner(
			{ skipOpen: true, dbOptions: [{}, { readOnly: true }] },
			async ({ db }, { db: db2 }) => {
				// create the database
				db.open();
				db2.open();
				expect(() => new TransactionLog(db2.store.db, 'foo')).toThrow(
					'Database is opened in readonly mode'
				);
			}
		));

	it('should open a db in readonly mode in separate process', () =>
		dbRunner(async ({ db, dbPath }) => {
			db.putSync('foo', 'bar');

			await new Promise<void>((resolve, reject) => {
				const args =
					process.versions.bun || process.versions.deno
						? [join(__dirname, 'fixtures', 'fork-open-readonly.mts'), dbPath]
						: [
								'node_modules/tsx/dist/cli.mjs',
								join(__dirname, 'fixtures', 'fork-open-readonly.mts'),
								dbPath,
							];

				const child = spawn(process.execPath, args, {
					env: { ...process.env, DO_FORK: '1' },
					stdio: 'inherit',
				});
				child.on('close', (code) => {
					try {
						expect(code).toBe(0);
						resolve();
					} catch (error) {
						reject(error);
					}
				});
				child.on('error', reject);
			});
		}));
});
