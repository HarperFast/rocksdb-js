import { RocksDatabase } from '../src/index.js';
import { constants } from '../src/load-binding.js';
import { parseTransactionLog } from '../src/parse-transaction-log.js';
import { dbRunner, generateDBPath } from './lib/util.js';
import { spawn } from 'node:child_process';
import { existsSync, readFileSync, rmSync, statSync } from 'node:fs';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const __dirname = dirname(fileURLToPath(import.meta.url));
const { TRANSACTION_LOG_FILE_HEADER_SIZE, TRANSACTION_LOG_ENTRY_HEADER_SIZE } = constants;

function runCrashFixture(dbPath: string, env: Record<string, string> = {}) {
	return new Promise<void>((resolve, reject) => {
		const fixture = join(__dirname, 'fixtures', 'txnlog-crash-window.mjs');
		const args = [fixture, dbPath];
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
	// of it. Recovery discards them: writeBatch() always completes before the RocksDB commit, so
	// an interrupted log write is the newest thing in the log and nothing durable depends on it.
	// Keeping the bytes and merely holding the watermark short of them is not enough — the next
	// transaction's flag would close the phantom group once the watermark moves past.
	it('discards a transaction that never closed so the log ends on a boundary', () =>
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
				const crashedSize = image.length - entrySize;
				await writeFile(logPath, image.subarray(0, crashedSize));

				database = RocksDatabase.open(dbPath);
				const reopened = database.useLog('foo');
				// The prefix is gone from the file itself, not just hidden behind the watermark.
				expect(statSync(logPath).size).toBe(afterComplete);
				expect(parseTransactionLog(logPath).size).toBe(afterComplete);
				expect(Array.from(reopened.query({ start: 0, readUncommitted: true })).length).toBe(1);
				expect(Array.from(reopened.query({ start: 0 })).length).toBe(1);

				// so the next transaction appends at the boundary and its closing flag has
				// nothing stale to swallow into its group
				await database.transaction(async (txn) => {
					reopened.addEntry(Buffer.alloc(24, 'y'), txn.id);
				});
				expect(Array.from(reopened.query({ start: 0 })).length).toBe(2);
				expect(parseTransactionLog(logPath).entries.map((entry) => entry.flags)).toEqual([1, 1]);
			} finally {
				database.close();
			}
		}));

	// The discard is gated on proof that the trailing run is one interrupted batch of a
	// flag-setting writer: a boundary earlier in the same file, and a single timestamp across the
	// run. A log written before the flag existed satisfies neither (its transactions are all
	// unflagged and each carries its own timestamp), and must be left alone — truncating there
	// would drop complete transactions.
	it('keeps an unflagged tail that spans transactions', () =>
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
				const logPath = join(dbPath, 'transaction_logs', 'foo', '1.txnlog');
				database.close();

				// rewrite the last two entries as a pre-flag writer would have left them
				const image = readFileSync(logPath);
				const { entries } = parseTransactionLog(logPath);
				let offset = TRANSACTION_LOG_FILE_HEADER_SIZE;
				const flagOffsets = entries.map((entry) => {
					const flagOffset = offset + TRANSACTION_LOG_ENTRY_HEADER_SIZE - 1;
					offset += TRANSACTION_LOG_ENTRY_HEADER_SIZE + entry.length;
					return flagOffset;
				});
				image[flagOffsets[1]] = 0;
				image[flagOffsets[2]] = 0;
				await writeFile(logPath, image);

				database = RocksDatabase.open(dbPath);
				const reopened = database.useLog('foo');
				// two unflagged entries with different timestamps cannot be one interrupted
				// batch, so nothing is dropped...
				expect(statSync(logPath).size).toBe(image.length);
				expect(Array.from(reopened.query({ start: 0, readUncommitted: true })).length).toBe(3);
				// ...and the watermark still stops at the last transaction that closed
				expect(Array.from(reopened.query({ start: 0 })).length).toBe(1);
			} finally {
				database.close();
			}
		}));

	it('never truncates or seeds the committed watermark behind the flushed position', () =>
		dbRunner(async ({ db, dbPath }) => {
			let database = db;
			try {
				const log = database.useLog('foo');
				for (let i = 0; i < 3; i++) {
					await database.transaction(async (txn) => {
						log.addEntry(Buffer.alloc(24, 'x'), txn.id);
						database.putSync(`key-${i}`, i, { transaction: txn });
					});
				}
				await database.flush();
				const logPath = join(dbPath, 'transaction_logs', 'foo', '1.txnlog');
				database.close();

				const image = readFileSync(logPath);
				const { entries } = parseTransactionLog(logPath);
				let offset = TRANSACTION_LOG_FILE_HEADER_SIZE;
				let tailTimestampOffset = 0;
				for (const [index, entry] of entries.entries()) {
					if (index === 1) {
						tailTimestampOffset = offset;
					} else if (index === 2) {
						image.copy(image, offset, tailTimestampOffset, tailTimestampOffset + 8);
					}
					if (index >= 1) {
						image[offset + TRANSACTION_LOG_ENTRY_HEADER_SIZE - 1] = 0;
					}
					offset += TRANSACTION_LOG_ENTRY_HEADER_SIZE + entry.length;
				}
				await writeFile(logPath, image);

				database = RocksDatabase.open(dbPath);
				const reopened = database.useLog('foo');
				expect(statSync(logPath).size).toBe(image.length);
				expect(Array.from(reopened.query({ start: 0 })).length).toBe(3);
				expect(Array.from(reopened.query({ start: 0, readUncommitted: true })).length).toBe(3);
			} finally {
				database.close();
			}
		}));

	it('seeds from the immediately preceding file when the active file is header-only', () =>
		dbRunner({ dbOptions: [{ transactionLogMaxSize: 1000 }] }, async ({ db, dbPath }) => {
			let database = db;
			try {
				const log = database.useLog('foo');
				for (let i = 0; i < 9; i++) {
					await database.transaction(async (txn) => {
						log.addEntry(Buffer.alloc(100, 'x'), txn.id);
					});
				}
				const logDir = join(dbPath, 'transaction_logs', 'foo');
				const secondLog = join(logDir, '2.txnlog');
				database.close();
				await writeFile(
					join(logDir, '3.txnlog'),
					readFileSync(secondLog).subarray(0, TRANSACTION_LOG_FILE_HEADER_SIZE)
				);

				database = RocksDatabase.open(dbPath);
				const reopened = database.useLog('foo');
				expect(Array.from(reopened.query({ start: 0 })).length).toBe(9);
			} finally {
				database.close();
			}
		}));

	it('finds a committed boundary before a transaction spanning multiple rotations', () =>
		dbRunner({ dbOptions: [{ transactionLogMaxSize: 1000 }] }, async ({ db, dbPath }) => {
			let database = db;
			try {
				const log = database.useLog('foo');
				await database.transaction(async (txn) => {
					log.addEntry(Buffer.alloc(100, 'a'), txn.id);
				});
				await database.transaction(async (txn) => {
					for (let i = 0; i < 25; i++) {
						log.addEntry(Buffer.alloc(100, 'b'), txn.id);
					}
				});

				const logDir = join(dbPath, 'transaction_logs', 'foo');
				const activeLogPath = join(logDir, '4.txnlog');
				database.close();
				rmSync(join(logDir, 'txn.state'), { force: true });

				const activeImage = readFileSync(activeLogPath);
				const activeEntries = parseTransactionLog(activeLogPath).entries;
				let entryOffset = TRANSACTION_LOG_FILE_HEADER_SIZE;
				let lastFlagOffset = 0;
				for (const entry of activeEntries) {
					lastFlagOffset = entryOffset + TRANSACTION_LOG_ENTRY_HEADER_SIZE - 1;
					entryOffset += TRANSACTION_LOG_ENTRY_HEADER_SIZE + entry.length;
				}
				activeImage[lastFlagOffset] = 0;
				await writeFile(activeLogPath, activeImage);

				database = RocksDatabase.open(dbPath);
				const reopened = database.useLog('foo');
				expect(Array.from(reopened.query({ start: 0 })).length).toBe(1);
				expect(Array.from(reopened.query({ start: 0, readUncommitted: true })).length).toBe(26);
			} finally {
				database.close();
			}
		}));

	it('does not fail load when the preceding log file is damaged', () =>
		dbRunner({ dbOptions: [{ transactionLogMaxSize: 1000 }] }, async ({ db, dbPath }) => {
			let database = db;
			try {
				const log = database.useLog('foo');
				for (let i = 0; i < 9; i++) {
					await database.transaction(async (txn) => {
						log.addEntry(Buffer.alloc(100, 'x'), txn.id);
					});
				}
				const logDir = join(dbPath, 'transaction_logs', 'foo');
				const header = readFileSync(join(logDir, '2.txnlog')).subarray(
					0,
					TRANSACTION_LOG_FILE_HEADER_SIZE
				);
				database.close();
				await writeFile(join(logDir, '2.txnlog'), header.subarray(0, 6));
				await writeFile(join(logDir, '3.txnlog'), header);

				expect(() => {
					database = RocksDatabase.open(dbPath);
					database.useLog('foo');
				}).not.toThrow();
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
