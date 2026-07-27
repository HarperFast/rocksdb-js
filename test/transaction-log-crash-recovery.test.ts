import { RocksDatabase } from '../src/index.js';
import { dbRunner, generateDBPath } from './lib/util.js';
import { spawn } from 'node:child_process';
import { existsSync, readFileSync, rmSync, statSync } from 'node:fs';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const __dirname = dirname(fileURLToPath(import.meta.url));

function runCrashFixture(dbPath: string, env: Record<string, string> = {}) {
	return new Promise<void>((resolve, reject) => {
		const fixture = join(__dirname, 'fixtures', 'txnlog-crash-window.mts');
		const args =
			process.versions.bun || process.versions.deno
				? [fixture, dbPath]
				: ['node_modules/tsx/dist/cli.mjs', fixture, dbPath];
		let output = '';
		const child = spawn(process.execPath, args, { env: { ...process.env, ...env } });
		child.stdout.setEncoding('utf8');
		child.stderr.setEncoding('utf8');
		child.stdout.on('data', (chunk) => (output += chunk));
		child.stderr.on('data', (chunk) => (output += chunk));
		child.on('error', reject);
		child.on('close', (code, signal) => {
			// The fixture SIGKILLs itself once its writes are durable, so the `ready`
			// handshake is what distinguishes a deliberate kill from a real failure. The
			// kill surfaces as the signal directly, or as 128 + SIGKILL through a loader.
			// Windows has no real signals — Node maps a self-kill to TerminateProcess(h, 1)
			// — so exit code 1 counts as abrupt only there; accepting it everywhere would
			// let a POSIX run whose kill() threw after `ready` pass with normal teardown.
			const diedAbruptly =
				signal === 'SIGKILL' || code === 137 || (process.platform === 'win32' && code === 1);
			if (diedAbruptly && output.includes('ready')) {
				resolve();
			} else {
				reject(
					new Error(`crash fixture exited unexpectedly (code=${code} signal=${signal}): ${output}`)
				);
			}
		});
	});
}

// The committed watermark (lastCommittedPosition) is in-memory state advanced by
// commitFinished(); it is never persisted. Before HarperFast/harper#1949 it was seeded on
// load from txn.state — the *flushed* position, i.e. how far RocksDB had absorbed the log —
// so after any exit without a final flush, log entries that were durable on disk sat past
// the watermark and were invisible to every committed read (replication resume,
// read_transaction_log) until an unrelated later commit jumped the watermark past them.
describe('Transaction log crash recovery', () => {
	it('exposes entries written after the last flush to committed reads on reopen', () =>
		dbRunner(async ({ db, dbPath }) => {
			let database = db;
			try {
				const log = database.useLog('foo');
				const value = Buffer.alloc(24, 'x');
				for (let i = 0; i < 3; i++) {
					await database.transaction(async (txn) => {
						log.addEntry(value, txn.id);
					});
				}
				expect(Array.from(log.query({ start: 0 })).length).toBe(3);
				database.close();

				// No RocksDB flush ever recorded these entries, so txn.state is absent/behind —
				// the same state a crash leaves behind. A committed read must still see them.
				database = RocksDatabase.open(dbPath);
				const reopened = database.useLog('foo');
				expect(Array.from(reopened.query({ start: 0 })).length).toBe(3);
				expect(Array.from(reopened.query({ start: 0, readUncommitted: true })).length).toBe(3);

				// and the log stays consistent for subsequent writes
				await database.transaction(async (txn) => {
					reopened.addEntry(Buffer.alloc(24, 'y'), txn.id);
				});
				expect(Array.from(reopened.query({ start: 0 })).length).toBe(4);
			} finally {
				database.close();
			}
		}));

	// Only a batch's final entry carries TRANSACTION_LOG_ENTRY_LAST_FLAG, so a crash partway
	// through a multi-entry transaction leaves whole, well-framed entries that are only a prefix
	// of it. Those bytes are kept (a readUncommitted replay still wants them), but the committed
	// watermark must stop before them — otherwise committed readers see a transaction that never
	// closed, and the next transaction's flag closes the phantom group.
	it('stops the committed watermark before a transaction that never closed', () =>
		dbRunner(async ({ db, dbPath }) => {
			let database = db;
			try {
				const log = database.useLog('foo');
				const value = Buffer.alloc(24, 'x');
				// one complete single-entry transaction
				await database.transaction(async (txn) => {
					log.addEntry(value, txn.id);
				});
				const logPath = join(dbPath, 'transaction_logs', 'foo', '1.txnlog');
				const afterComplete = statSync(logPath).size;

				// then a three-entry transaction; only its last entry closes it
				await database.transaction(async (txn) => {
					log.addEntry(value, txn.id);
					log.addEntry(value, txn.id);
					log.addEntry(value, txn.id);
				});
				const entrySize = (statSync(logPath).size - afterComplete) / 3;
				database.close();

				// simulate a crash after two of the three entries reached disk
				const image = readFileSync(logPath);
				await writeFile(logPath, image.subarray(0, image.length - entrySize));

				database = RocksDatabase.open(dbPath);
				const reopened = database.useLog('foo');
				// the surviving prefix is still on disk and still replayable...
				expect(Array.from(reopened.query({ start: 0, readUncommitted: true })).length).toBe(3);
				// ...but committed reads stop at the last transaction that actually closed
				expect(Array.from(reopened.query({ start: 0 })).length).toBe(1);
			} finally {
				database.close();
			}
		}));

	it('exposes the post-flush window to committed reads after a SIGKILL', async () => {
		const dbPath = generateDBPath();
		await mkdir(dbPath, { recursive: true });
		let db: RocksDatabase | undefined;
		try {
			await runCrashFixture(dbPath, { BEFORE_FLUSH: '3', AFTER_FLUSH: '4' });

			db = RocksDatabase.open(dbPath);
			const log = db.useLog('foo');
			// All 7 entries are durable in the log file; before the fix a committed read
			// returned only the 3 that preceded the flush recorded in txn.state.
			expect(Array.from(log.query({ start: 0 })).length).toBe(7);
		} finally {
			db?.close();
			if (!process.env.KEEP_FILES && existsSync(dbPath)) {
				rmSync(dbPath, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
			}
		}
	});
});
