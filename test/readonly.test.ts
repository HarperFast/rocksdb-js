import { RocksDatabase } from '../src/index.ts';
import { TransactionLog } from '../src/load-binding.ts';
import { dbRunner } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { appendFileSync, cpSync, readFileSync, readdirSync, rmSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

describe('Readonly Operations', () => {
	it('should error opening a readonly database that does not exist', () =>
		dbRunner({ skipOpen: true, dbOptions: [{ readOnly: true }] }, async ({ db }) => {
			expect(() => db.open()).toThrow('Database does not exist');
		}));

	it('should report a missing SST as the concurrent-compaction race, not corruption', () =>
		dbRunner(
			{ skipOpen: true, dbOptions: [{}, { readOnly: true }] },
			async ({ db }, { db: db2, dbPath }) => {
				// Produce an SST, then remove it out from under the MANIFEST — the
				// same shape a live writer's compaction produces mid-open.
				db.open();
				db.putSync('foo', 'bar');
				db.flushSync();
				db.close();

				const sst = readdirSync(dbPath).find((file) => file.endsWith('.sst'));
				expect(sst).toBeDefined();
				rmSync(join(dbPath, sst!));

				let error: Error & { code?: string };
				try {
					db2.open();
					expect.fail('expected the read-only open to throw');
				} catch (err) {
					error = err as Error;
				}
				expect(error!.code).toBe('ERR_CONCURRENT_COMPACTION');
				expect(error!.message).toMatch(/concurrent compaction/);
				expect(error!.message).not.toMatch(/Database does not exist/);
				// The original RocksDB status text is preserved for diagnosis.
				expect(error!.message).toContain(sst);
			}
		));

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

	it('should not recover (truncate) transaction logs on a readonly open', () =>
		dbRunner(
			{ skipOpen: true, dbOptions: [{}, { readOnly: true }, {}] },
			async ({ db, dbPath }, { db: readOnly }, { db: writer }) => {
				// Write real log entries, then fully close so the registry entry is
				// released and the next open re-discovers the store from disk.
				db.open();
				const log = db.useLog('foo');
				await db.transaction(async (txn) => {
					await txn.put('foo', 'bar');
					log.addEntry(Buffer.from('hello'), txn.id);
				});
				db.close();

				// Simulate a torn append: garbage past the last complete entry. A
				// writer's recovery truncates this; a read-only open must not — the
				// file may belong to a live writer in another process, and reader
				// truncation is how acknowledged writes vanish (invariant 5).
				const logDir = join(dbPath, 'transaction_logs', 'foo');
				const logFile = readdirSync(logDir).find((file) => file.endsWith('.txnlog'));
				expect(logFile).toBeDefined();
				const logPath = join(logDir, logFile!);
				appendFileSync(logPath, Buffer.alloc(64, 0xff));
				const tornSize = statSync(logPath).size;

				readOnly.open();
				// Intact entries are still readable through the read-only handle.
				const entries = Array.from(readOnly.useLog('foo').query({ start: 0 }));
				expect(entries.length).toBeGreaterThan(0);
				expect(statSync(logPath).size).toBe(tornSize);
				readOnly.close();
				expect(statSync(logPath).size).toBe(tornSize);

				// A writable open owns recovery and discards the torn tail.
				writer.open();
				const recovered = readFileSync(logPath);
				expect(recovered.length).toBeLessThanOrEqual(tornSize);
				const tailIsCleared = recovered.subarray(tornSize - 64).every((byte) => byte === 0);
				if (process.platform === 'win32') {
					// Windows cannot shrink a file while any mapping covers it
					// (sections are mandatory), and whether the read-only
					// handle's mapping above is still live here is a GC-timing
					// detail — so recovery either truncates or zero-fills the
					// torn range, and the zero-fill path is asserted
					// deterministically in test/native/transaction_log_erase_tail_test.cc.
					expect(recovered.length < tornSize || tailIsCleared).toBe(true);
				} else {
					expect(recovered.length).toBeLessThan(tornSize);
					expect(tailIsCleared).toBe(true);
				}
			}
		));

	// The registry entry for a path is shared by every handle on it and outlives
	// the handle that created it, so the mode of the OPENING handle — not the
	// entry's — has to decide how discovery loads a store. A writer that has
	// since closed used to leave the entry stamped writable, and the next
	// read-only open then loaded newly-appeared stores with retention purge and
	// recoverTail() truncation against what may be a live primary's logs.
	it('should not recover logs discovered after a writer closed but left the entry alive', () =>
		dbRunner(
			{ skipOpen: true, dbOptions: [{}, { readOnly: true }] },
			async ({ db, dbPath }, { db: readOnly1 }) => {
				db.open();
				const log = db.useLog('foo');
				await db.transaction(async (txn) => {
					await txn.put('foo', 'bar');
					log.addEntry(Buffer.from('hello'), txn.id);
				});

				// The read-only handle keeps the path's registry entry alive past
				// the writer's close.
				readOnly1.open();
				db.close();

				// A store that appears only now — as a live primary in another
				// process would create it — is discovered by the next open.
				const logsDir = join(dbPath, 'transaction_logs');
				cpSync(join(logsDir, 'foo'), join(logsDir, 'bar'), { recursive: true });
				const barLog = join(
					logsDir,
					'bar',
					readdirSync(join(logsDir, 'bar')).find((file) => file.endsWith('.txnlog'))!
				);
				appendFileSync(barLog, Buffer.alloc(64, 0xff));
				const tornSize = statSync(barLog).size;

				// A distinct handle kind (a secondary) is a distinct registry
				// entry, so this open runs its own discovery of the new store.
				const secondary = new RocksDatabase(dbPath, { secondaryPath: `${dbPath}.secondary` });
				try {
					secondary.open();
					expect(statSync(barLog).size).toBe(tornSize);
					expect(Array.from(secondary.useLog('bar').query({ start: 0 })).length).toBeGreaterThan(0);
				} finally {
					secondary.close();
					// Windows can still hold the workspace briefly after close.
					rmSync(`${dbPath}.secondary`, {
						force: true,
						recursive: true,
						maxRetries: 5,
						retryDelay: 50,
					});
				}
			}
		));

	// The one guard whose regression mode is a process abort rather than a wrong
	// value: resolveTransactionLogStore throws a DBException, which derives from
	// std::exception (not std::runtime_error), and an escaped C++ exception
	// aborts from an N-API callback. Both useLog surfaces must turn it into a JS
	// error instead.
	it('should reject a writer adopting a readonly-loaded log at both useLog surfaces', () =>
		dbRunner({ skipOpen: true, dbOptions: [{}] }, async ({ db, dbPath }) => {
			db.open();
			const log = db.useLog('foo');
			await db.transaction(async (txn) => {
				await txn.put('foo', 'bar');
				log.addEntry(Buffer.from('hello'), txn.id);
			});

			// A store that appears while the writer is already open — as a live
			// primary in another process would create it — is discovered by the
			// next open, and a secondary discovers it read-only (no tail
			// recovery). The open writer must not then adopt it.
			const logsDir = join(dbPath, 'transaction_logs');
			cpSync(join(logsDir, 'foo'), join(logsDir, 'bar'), { recursive: true });

			const secondary = new RocksDatabase(dbPath, { secondaryPath: `${dbPath}.secondary` });
			try {
				secondary.open();
				expect(Array.from(secondary.useLog('bar').query({ start: 0 })).length).toBeGreaterThan(0);

				expect(() => db.useLog('bar')).toThrow('is open read-only in this process');
				await expect(
					db.transaction(async (txn) => {
						txn.useLog('bar');
					})
				).rejects.toThrow('is open read-only in this process');

				// The writer is still usable: the rejection is an error, not a
				// poisoned handle.
				expect(db.getSync('foo')).toBe('bar');
			} finally {
				secondary.close();
				rmSync(`${dbPath}.secondary`, {
					force: true,
					recursive: true,
					maxRetries: 5,
					retryDelay: 50,
				});
			}

			// Closing the last read-only registrant evicts the unrecovered store,
			// allowing the live writer to reopen it with recovery.
			const writableLog = db.useLog('bar');
			await db.transaction(async (txn) => {
				await txn.put('bar', 'writable');
				writableLog.addEntry(Buffer.from('writer entry'), txn.id);
			});
			expect(db.getSync('bar')).toBe('writable');
		}));

	it('should refuse a writable open while transaction logs are held readonly', () =>
		dbRunner(
			{ skipOpen: true, dbOptions: [{}, { readOnly: true }, {}] },
			async ({ db }, { db: readOnly }, { db: writer }) => {
				db.open();
				const log = db.useLog('foo');
				await db.transaction(async (txn) => {
					await txn.put('foo', 'bar');
					log.addEntry(Buffer.from('hello'), txn.id);
				});
				db.close();

				// The read-only open loaded the store without tail recovery, so a
				// writer must not adopt it: appends would land past a torn tail.
				readOnly.open();
				expect(() => writer.open()).toThrow('transaction logs are open read-only in this process');

				// Once the read-only handle closes, the writer loads with recovery.
				readOnly.close();
				writer.open();
				expect(writer.getSync('foo')).toBe('bar');
			}
		));

	// The guard only meets both handles when they land on the same log-store
	// registry entry, and the entry is keyed by path. Two spellings of one
	// directory (a trailing slash here; a relative path or a symlinked /tmp in
	// the field) used to open two entries over one transaction_logs tree, so the
	// writer never saw the reader and truncated a segment the reader had mapped.
	it('should refuse a writable open spelled differently from the readonly one', () =>
		dbRunner(
			{ skipOpen: true, dbOptions: [{}, { readOnly: true }] },
			async ({ db, dbPath }, { db: readOnly }) => {
				db.open();
				const log = db.useLog('foo');
				await db.transaction(async (txn) => {
					await txn.put('foo', 'bar');
					log.addEntry(Buffer.from('hello'), txn.id);
				});
				db.close();

				readOnly.open();
				const writer = new RocksDatabase(`${dbPath}/`);
				try {
					expect(() => writer.open()).toThrow(
						'transaction logs are open read-only in this process'
					);
				} finally {
					writer.close();
				}
			}
		));

	it('should open a db in readonly mode in separate process', () =>
		dbRunner(async ({ db, dbPath }) => {
			db.putSync('foo', 'bar');

			await new Promise<void>((resolve, reject) => {
				const args = [join(__dirname, 'fixtures', 'fork-open-readonly.mts'), dbPath];

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
