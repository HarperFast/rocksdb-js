import { RocksDatabase } from '../src/index.ts';
import { generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { existsSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'node:worker_threads';
import { describe, expect, it } from 'vitest';

function cleanup(...paths: string[]): void {
	if (process.env.KEEP_FILES) {
		return;
	}
	for (const path of paths) {
		try {
			rmSync(path, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
		} catch (err) {
			console.error(`Error removing: ${path}: ${err}`);
		}
	}
}

describe('Secondary Instances', () => {
	it('should follow a live primary through catchUpWithPrimary', async () => {
		const dbPath = generateDBPath();
		const secondaryPath = `${dbPath}.secondary`;
		const primary = new RocksDatabase(dbPath);
		const secondary = new RocksDatabase(dbPath, { secondaryPath });
		try {
			primary.open();
			primary.putSync('before-open', 'a');

			secondary.open();
			expect(secondary.readOnly).toBe(true);
			expect(secondary.secondaryPath).toBe(secondaryPath);
			expect(secondary.getSync('before-open')).toBe('a');

			// writes after the secondary opened are invisible until catch-up
			const blob = 'x'.repeat(8 * 1024); // above the 2KB blob threshold
			primary.putSync('after-open', 'b');
			primary.putSync('large', blob);
			expect(secondary.getSync('after-open')).toBeUndefined();
			expect(secondary.getSync('large')).toBeUndefined();

			await secondary.catchUpWithPrimary();
			expect(secondary.getSync('after-open')).toBe('b');
			expect(secondary.getSync('large')).toBe(blob);

			// Sync variant, and catch-up of flushed (SST/blob-resident) state.
			primary.putSync('flushed', 'c');
			primary.flushSync();
			expect(secondary.getSync('flushed')).toBeUndefined();
			secondary.catchUpWithPrimarySync();
			expect(secondary.getSync('flushed')).toBe('c');
		} finally {
			secondary.close();
			primary.close();
			cleanup(dbPath, secondaryPath);
		}
	});

	it('should enforce read-only behavior on a secondary', async () => {
		const dbPath = generateDBPath();
		const secondaryPath = `${dbPath}.secondary`;
		const primary = new RocksDatabase(dbPath);
		const secondary = new RocksDatabase(dbPath, { secondaryPath });
		try {
			primary.open();
			primary.putSync('foo', 'bar');
			secondary.open();

			expect(() => secondary.putSync('foo', 'baz')).toThrow(
				'Not supported operation in secondary mode'
			);
			await expect(secondary.put('foo', 'baz')).rejects.toThrow(
				'Not supported operation in secondary mode'
			);
			// flush/compact are no-ops on read-only handles, secondaries included
			await secondary.flush();
			secondary.flushSync();

			// read-only transactions work
			await secondary.transaction(async (txn) => {
				expect(await txn.get('foo')).toBe('bar');
			});
		} finally {
			secondary.close();
			primary.close();
			cleanup(dbPath, secondaryPath);
		}
	});

	it('should reject catchUpWithPrimary on non-secondary handles', async () => {
		const dbPath = generateDBPath();
		const primary = new RocksDatabase(dbPath);
		const readOnly = new RocksDatabase(dbPath, { readOnly: true });
		try {
			primary.open();
			readOnly.open();

			for (const db of [primary, readOnly]) {
				await expect(db.catchUpWithPrimary()).rejects.toMatchObject({
					code: 'ERR_NOT_SECONDARY',
				});
				expect(() => db.catchUpWithPrimarySync()).toThrow('Database is not a secondary instance');
			}
		} finally {
			readOnly.close();
			primary.close();
			cleanup(dbPath);
		}
	});

	it('should validate secondaryPath option combinations', async () => {
		const dbPath = generateDBPath();
		const secondaryPath = `${dbPath}.secondary`;
		const primary = new RocksDatabase(dbPath);
		try {
			primary.open();

			// contradiction with explicit readOnly: false
			expect(() => new RocksDatabase(dbPath, { secondaryPath, readOnly: false })).toThrow(
				'secondaryPath cannot be combined with readOnly: false'
			);

			// redundant readOnly: true is fine
			const redundant = new RocksDatabase(dbPath, { secondaryPath, readOnly: true });
			redundant.open();
			expect(redundant.readOnly).toBe(true);
			redundant.close();

			// empty string is not "no secondary"
			const empty = new RocksDatabase(dbPath, { secondaryPath: '' });
			expect(() => empty.open()).toThrow('secondaryPath must not be empty');

			// a bounded table cache would reintroduce the missing-file race
			const bounded = new RocksDatabase(dbPath, { secondaryPath, maxOpenFiles: 500 });
			expect(() => bounded.open()).toThrow('A secondary open requires maxOpenFiles: -1');
			const unbounded = new RocksDatabase(dbPath, { secondaryPath, maxOpenFiles: -1 });
			unbounded.open();
			unbounded.close();
		} finally {
			primary.close();
			cleanup(dbPath, secondaryPath);
		}
	});

	it('should treat secondaryPath: null as absent, matching the native layer', async () => {
		const dbPath = generateDBPath();
		const db = new RocksDatabase(dbPath, { secondaryPath: null as unknown as string });
		try {
			db.open();
			expect(db.readOnly).toBe(false);
			expect(db.secondaryPath).toBeUndefined();
			db.putSync('foo', 'bar');
			expect(db.getSync('foo')).toBe('bar');
		} finally {
			db.close();
			cleanup(dbPath);
		}
	});

	it('should not create transaction log stores through a secondary useLog', async () => {
		const dbPath = generateDBPath();
		const secondaryPath = `${dbPath}.secondary`;
		const primary = new RocksDatabase(dbPath);
		const secondary = new RocksDatabase(dbPath, { secondaryPath });
		try {
			primary.open();
			const log = primary.useLog('exists');
			await primary.transaction(async (txn) => {
				await txn.put('foo', 'bar');
				log.addEntry(Buffer.from('hello'), txn.id);
			});
			// Close the writer so the secondary is the path's sole (read-only)
			// registrant — the shape of a follower in its own process. (While a
			// writable handle is open in the SAME process, the shared registry
			// entry is writer-owned and store creation stays legal.)
			primary.close();

			secondary.open();
			expect(Array.from(secondary.useLog('exists').query({ start: 0 })).length).toBeGreaterThan(0);
			// The log view is frozen at open: a store the open never discovered
			// is reported as not found, not conjured (mkdir + writable files)
			// into the primary's tree.
			expect(() => secondary.useLog('never-created')).toThrow(
				'Transaction log "never-created" not found'
			);
			expect(existsSync(join(dbPath, 'transaction_logs', 'never-created'))).toBe(false);
			expect(secondary.listLogs()).toEqual(['exists']);

			// A read-only transaction is a JS shim over the database context —
			// same frozen-view contract through txn.useLog.
			await secondary.transaction(async (txn) => {
				expect(() => txn.useLog('never-created')).toThrow(
					'Transaction log "never-created" not found'
				);
			});
		} finally {
			secondary.close();
			primary.close();
			cleanup(dbPath, secondaryPath);
		}
	});

	it('should reject reusing a secondary workspace for a different database', async () => {
		const dbPathA = generateDBPath();
		const dbPathB = generateDBPath();
		const secondaryPath = `${dbPathA}.secondary`;
		const primaryA = new RocksDatabase(dbPathA);
		const primaryB = new RocksDatabase(dbPathB);
		const secondaryA = new RocksDatabase(dbPathA, { secondaryPath });
		const secondaryB = new RocksDatabase(dbPathB, { secondaryPath });
		try {
			primaryA.open();
			primaryB.open();
			secondaryA.open();
			expect(() => secondaryB.open()).toThrow(
				`secondaryPath "${secondaryPath}" is already in use by database "${dbPathA}"`
			);
		} finally {
			secondaryB.close();
			secondaryA.close();
			primaryB.close();
			primaryA.close();
			cleanup(dbPathA, dbPathB, secondaryPath);
		}
	});

	it('should exclude a second process from a held secondary workspace', async () => {
		const dbPath = generateDBPath();
		const secondaryPath = `${dbPath}.secondary`;
		const childSecondaryPath = `${dbPath}.secondary-child`;
		const primary = new RocksDatabase(dbPath);
		const secondary = new RocksDatabase(dbPath, { secondaryPath });
		try {
			primary.open();
			primary.putSync('foo', 'bar');
			secondary.open();

			await new Promise<void>((resolve, reject) => {
				const child = spawn(process.execPath, [
					join(__dirname, 'fixtures', 'fork-open-secondary.mts'),
					dbPath,
					secondaryPath,
					childSecondaryPath,
				]);
				let output = '';
				child.stdout.on('data', (chunk) => (output += chunk));
				child.stderr.on('data', (chunk) => (output += chunk));
				child.on('close', (code) => {
					try {
						expect(code, output).toBe(0);
						resolve();
					} catch (error) {
						reject(error);
					}
				});
				child.on('error', reject);
			});
		} finally {
			secondary.close();
			primary.close();
			cleanup(dbPath, secondaryPath, childSecondaryPath);
		}
	});

	it('should tolerate a missing file at secondary open (point-in-time fallback)', async () => {
		const dbPath = generateDBPath();
		const secondaryPath = `${dbPath}.secondary`;
		const setup = new RocksDatabase(dbPath);
		try {
			setup.open();
			setup.putSync('foo', 'bar');
			setup.flushSync();
			setup.close();

			const sst = readdirSync(dbPath).find((file) => file.endsWith('.sst'));
			expect(sst).toBeDefined();
			rmSync(join(dbPath, sst!));

			// The exact deletion that fails a plain readOnly open (see
			// readonly.test.ts) does not fail a secondary open: its point-in-time
			// recovery falls back to the last version whose files all exist, so
			// the handle serves an older consistent view instead of erroring —
			// the design property that makes secondary the live-follower mode.
			const secondary = new RocksDatabase(dbPath, { secondaryPath });
			secondary.open();
			expect(secondary.getSync('foo')).toBeUndefined();
			secondary.close();
		} finally {
			cleanup(dbPath, secondaryPath);
		}
	});

	it('should reject a secondaryPath that is the database path', async () => {
		const dbPath = generateDBPath();
		const primary = new RocksDatabase(dbPath);
		try {
			primary.open();
			const aliased = new RocksDatabase(dbPath, { secondaryPath: `${dbPath}/` });
			expect(() => aliased.open()).toThrow(
				'secondaryPath must be a separate directory outside the database path'
			);
			// nested inside the primary is just as bad: destroy()'s recursive
			// delete would take the live workspace with it
			const nested = new RocksDatabase(dbPath, { secondaryPath: join(dbPath, 'follower') });
			expect(() => nested.open()).toThrow(
				'secondaryPath must be a separate directory outside the database path'
			);
		} finally {
			primary.close();
			cleanup(dbPath);
		}
	});

	it('should close secondaries on destroy and release their workspaces', async () => {
		const dbPath = generateDBPath();
		const secondaryPath = `${dbPath}.secondary`;
		let primary = new RocksDatabase(dbPath);
		const secondary = new RocksDatabase(dbPath, { secondaryPath });
		try {
			primary.open();
			primary.putSync('foo', 'bar');
			secondary.open();

			// Destroy must claim and close EVERY descriptor for the path — a
			// secondary left unclosed would hold its workspace lock for the life
			// of the process.
			primary.destroy();
			expect(secondary.isOpen()).toBe(false);

			// The workspace lock was released by the close, so a fresh secondary
			// can use the same workspace against a recreated database.
			primary = new RocksDatabase(dbPath);
			primary.open();
			primary.putSync('foo', 'baz');
			const again = new RocksDatabase(dbPath, { secondaryPath });
			again.open();
			expect(again.getSync('foo')).toBe('baz');
			again.close();
		} finally {
			secondary.close();
			primary.close();
			cleanup(dbPath, secondaryPath);
		}
	});

	it('should surface secondary workspace creation failures', async () => {
		const dbPath = generateDBPath();
		const primary = new RocksDatabase(dbPath);
		try {
			primary.open();
			// a path under a regular file cannot be created as a directory
			const blocker = `${dbPath}.blocker`;
			writeFileSync(blocker, 'not a directory');
			const secondary = new RocksDatabase(dbPath, {
				secondaryPath: join(blocker, 'nested'),
			});
			expect(() => secondary.open()).toThrow('Failed to create secondary path');
			cleanup(blocker);
		} finally {
			primary.close();
			cleanup(dbPath);
		}
	});

	it('should error opening a secondary of a database that does not exist', () => {
		const dbPath = generateDBPath();
		const secondaryPath = `${dbPath}.secondary`;
		const secondary = new RocksDatabase(dbPath, { secondaryPath });
		try {
			expect(() => secondary.open()).toThrow('as secondary');
		} finally {
			secondary.close();
			cleanup(dbPath, secondaryPath);
		}
	});

	it('should open existing column families and reject missing ones', async () => {
		const dbPath = generateDBPath();
		const secondaryPath = `${dbPath}.secondary`;
		const primary = new RocksDatabase(dbPath);
		const primaryBaz = new RocksDatabase(dbPath, { name: 'baz' });
		try {
			primary.open();
			primaryBaz.open();
			primaryBaz.putSync('foo', 'bar');

			const secondaryBaz = new RocksDatabase(dbPath, { secondaryPath, name: 'baz' });
			secondaryBaz.open();
			expect(secondaryBaz.getSync('foo')).toBe('bar');
			secondaryBaz.close();

			// A CF the secondary's snapshot does not have cannot be created
			const missing = new RocksDatabase(dbPath, {
				secondaryPath: `${dbPath}.secondary2`,
				name: 'missing',
			});
			expect(() => missing.open()).toThrow(
				'Column family "missing" not found: cannot create column family in read-only mode'
			);
			cleanup(`${dbPath}.secondary2`);
		} finally {
			primaryBaz.close();
			primary.close();
			cleanup(dbPath, secondaryPath);
		}
	});

	// Proves the race rather than asserting it: a worker thread drives
	// continuous write + flush traffic on the primary so compactions delete
	// input SSTs and blob GC deletes rewritten blob files; a read-only open
	// replays the MANIFEST and then opens files it holds no reference on, so it
	// loses that race (and even a successful open can lose later reads to it) —
	// while a secondary open must never fail for it. Measured on this recipe on
	// Linux: ~6% of read-only attempts race per iteration; the loop stops early
	// once the race has been demonstrated a few times. On an environment where
	// the churn never lines up the test skips rather than failing a correct
	// build; the secondary-side zero-failure assertions stay hard.
	it(
		'should survive a live writer as a secondary where readOnly races',
		{ timeout: 120_000 },
		async (ctx) => {
			const dbPath = generateDBPath();
			const secondaryPath = `${dbPath}.secondary`;
			const setup = new RocksDatabase(dbPath);
			const KEYS = 200;
			let raceErrors = 0;
			let readFailures = 0;
			let readOnlyOpens = 0;

			setup.open();
			for (let i = 0; i < KEYS; i++) {
				setup.putSync(`key${i}`, 'x'.repeat(3 * 1024));
			}
			setup.flushSync();
			setup.close();

			const worker = new Worker(new URL('./workers/secondary-race-writer.mts', import.meta.url), {
				workerData: {
					dbPath,
					keys: KEYS,
					valueSize: 3 * 1024, // blob-resident, above the 2KB threshold
					writeBufferSize: 1024 * 1024,
				},
			});
			await new Promise((resolve) => worker.once('message', resolve)); // ready

			try {
				// The read-only side: open, dwell so the writer obsoletes files the
				// snapshot references, read, close. Every open failure must be the
				// classified race — never a corruption report, never "Database does
				// not exist" — and read failures are the same hazard's other face.
				for (let i = 0; i < 200 && raceErrors < 5; i++) {
					const readOnly = new RocksDatabase(dbPath, { readOnly: true });
					try {
						readOnly.open();
						readOnlyOpens++;
						try {
							await delay(75);
							readOnly.getSync('key0');
							readOnly.getSync(`key${KEYS - 1}`);
						} catch {
							// A read on the stale snapshot hit a file the live writer
							// already deleted — the post-open face of the same hazard.
							readFailures++;
						}
					} catch (err) {
						expect((err as Error & { code?: string }).code).toBe('ERR_CONCURRENT_COMPACTION');
						expect((err as Error).message).not.toMatch(/may be corrupted/);
						expect((err as Error).message).not.toMatch(/Database does not exist/);
						raceErrors++;
					} finally {
						readOnly.close();
					}
				}

				// The same churn through a secondary: open once, then catch up,
				// dwell, and read continuously — zero failures allowed.
				const secondary = new RocksDatabase(dbPath, { secondaryPath });
				secondary.open();
				for (let i = 0; i < 50; i++) {
					await secondary.catchUpWithPrimary();
					await delay(25);
					// the writer worker overwrites the setup values, so only assert
					// a blob-sized value came back intact
					expect(secondary.getSync('key0')).toMatch(/^[vx]{3072}/);
					expect(secondary.getSync(`key${KEYS - 1}`)).toMatch(/^[vx]{3072}/);
				}
				secondary.close();

				// Full open/read/close cycles race the same open-time window the
				// read-only side loses; the secondary open must never lose it.
				const cyclePath = `${dbPath}.secondary-cycle`;
				for (let i = 0; i < 20; i++) {
					const cycled = new RocksDatabase(dbPath, { secondaryPath: cyclePath });
					cycled.open();
					expect(cycled.getSync('key0')).toMatch(/^[vx]{3072}/);
					cycled.close();
					await delay(25);
				}
				cleanup(cyclePath);

				console.log(
					`readOnly: ${readOnlyOpens} clean opens, ${raceErrors} classified races, ${readFailures} stale-snapshot read failures`
				);
				if (raceErrors + readFailures === 0) {
					console.warn('the readOnly race did not reproduce in this environment; skipping');
					ctx.skip();
				}
			} finally {
				worker.postMessage('stop');
				await new Promise((resolve) => worker.once('message', resolve));
				await worker.terminate();
				cleanup(dbPath, secondaryPath);
			}
		}
	);
});
