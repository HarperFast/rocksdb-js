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
			{
				skipOpen: true,
				dbOptions: [
					{}, // default column family, read/write
					{ readOnly: true }, // default column family, read-only
					{ name: 'baz' }, // named column family, read/write
					{ name: 'baz', readOnly: true }, // named column family, read-only
				],
			},
			async ({ db }, { db: db2 }, { db: db3 }, { db: db4 }) => {
				db.open();
				expect(db.readOnly).toBe(false);
				db2.open();
				expect(db2.readOnly).toBe(true);
				db3.open();
				expect(db3.readOnly).toBe(false);

				// db3 created the named column family AFTER db2 took a snapshot of the column
				// families, thus "baz" won't exist in the snapshot
				expect(() => db4.open()).toThrow(
					'Column family "baz" not found: cannot create column family in read-only mode'
				);

				// we close db2 to delete read-only DBDescriptor, then re-open it discover the "baz"
				// column family
				db2.close();
				db2.open();
				expect(db2.readOnly).toBe(true);
				db4.open();
				expect(db4.readOnly).toBe(true);

				// clear
				await expect(db2.clear()).rejects.toThrow(
					'Clear failed: Not implemented: Not supported operation in read only mode'
				);
				expect(() => db2.clearSync()).toThrow(
					'Clear failed: Not implemented: Not supported operation in read only mode'
				);

				// destroy
				expect(() => db2.destroy()).toThrow(
					'Destroy failed: Unsupported operation in read-only mode'
				);

				// drop
				await expect(db2.drop()).rejects.toThrow(
					'Drop failed: Not implemented: Not supported operation in read only mode'
				);
				expect(() => db2.dropSync()).toThrow(
					'Drop failed: Not implemented: Not supported operation in read only mode'
				);

				// purgeLogs
				expect(() => db2.purgeLogs()).toThrow(
					'Purge logs failed: Unsupported operation in read-only mode'
				);

				// put
				await expect(db2.put('foo', 'bar')).rejects.toThrow(
					'Put failed: Not implemented: Not supported operation in read only mode'
				);
				expect(() => db2.putSync('foo', 'bar')).toThrow(
					'Put failed: Not implemented: Not supported operation in read only mode'
				);

				// remove
				await expect(db2.remove('foo')).rejects.toThrow(
					'Remove failed: Not implemented: Not supported operation in read only mode'
				);
				expect(() => db2.removeSync('foo')).toThrow(
					'Remove failed: Not implemented: Not supported operation in read only mode'
				);
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

	it('should allow read operations in transactions', async () =>
		dbRunner(
			{ skipOpen: true, dbOptions: [{}, { readOnly: true }] },
			async ({ db }, { db: db2 }) => {
				db.open();
				db.putSync('foo', 'bar');

				db2.open();

				// read operations
				await db2.transaction(async (txn) => {
					expect(await txn.get('foo')).toBe('bar');
				});
				await db2.transaction(async (txn) => {
					expect(await db2.get('foo', { transaction: txn })).toBe('bar');
				});
				db2.transactionSync(async (txn) => {
					expect(txn.getSync('foo')).toBe('bar');
				});
				db2.transactionSync(async (txn) => {
					expect(db2.getSync('foo', { transaction: txn })).toBe('bar');
				});

				// write operations
				await expect(
					db2.transaction(async (txn) => {
						txn.putSync('foo', 'baz');
					})
				).rejects.toThrow('Put failed: Not implemented: Not supported operation in read only mode');
				await expect(
					db2.transaction(async (txn) => {
						db2.putSync('foo', 'baz', { transaction: txn });
					})
				).rejects.toThrow('Put failed: Not implemented: Not supported operation in read only mode');
				expect(() =>
					db2.transactionSync((txn) => {
						txn.putSync('foo', 'baz');
					})
				).toThrow('Put failed: Not implemented: Not supported operation in read only mode');
				expect(() =>
					db2.transactionSync((txn) => {
						db2.putSync('foo', 'baz', { transaction: txn });
					})
				).toThrow('Put failed: Not implemented: Not supported operation in read only mode');
			}
		));

	it('should allow read access to transaction logs', async () =>
		dbRunner({ dbOptions: [{}, { readOnly: true }] }, async ({ db: db1 }, { db: db2 }) => {
			const log1 = db1.useLog('foo');
			await db1.transaction(async (txn) => {
				await txn.put('foo', 'bar');
				log1.addEntry(Buffer.from('hello'), txn.id);
			});

			expect(db1.listLogs()).toEqual(['foo']);
			expect(db2.listLogs()).toEqual(['foo']);

			const log2 = db2.useLog('foo');
			await db2.transaction(async (txn) => {
				expect(() => log2.addEntry(Buffer.from('world'), txn.id)).toThrow(
					'Unsupported operation in read-only mode'
				);

				const txnLog = txn.useLog('foo');
				expect(() => txnLog.addEntry(Buffer.from('world'), txn.id)).toThrow(
					'Unsupported operation in read-only mode'
				);
			});

			const value1 = Array.from(log1.query({ start: 0 }));
			const value2 = Array.from(log2.query({ start: 0 }));
			expect(value1).toEqual(value2);

			// manually construct a transaction log instance
			const txnLog1 = new TransactionLog(db1.store.db, 'foo');
			const txnLog2 = new TransactionLog(db2.store.db, 'foo');
			const value3 = Array.from(txnLog1.query({ start: 0 }));
			const value4 = Array.from(txnLog2.query({ start: 0 }));
			expect(value3).toEqual(value4);
		}));

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
					// stdio: 'inherit',
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
