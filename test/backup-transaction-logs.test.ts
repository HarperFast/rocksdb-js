import { backups, RocksDatabase } from '../src/index.js';
import { dbRunner, generateDBPath } from './lib/util.js';
import {
	existsSync,
	mkdirSync,
	readFileSync,
	renameSync,
	rmSync,
	statSync,
	utimesSync,
	writeFileSync,
} from 'node:fs';
import { join } from 'node:path';
import * as tar from 'tar';
import { afterEach, describe, expect, it } from 'vitest';

const tempPaths: string[] = [];

function tempPath(): string {
	const p = generateDBPath();
	tempPaths.push(p);
	return p;
}

/** Writes `count` committed transaction-log entries to `name`, each filled with `fill`. */
async function writeLog(db: RocksDatabase, name: string, count = 5, fill = 'x'): Promise<void> {
	const log = db.useLog(name);
	const value = Buffer.alloc(100, fill);
	for (let i = 0; i < count; i++) {
		await db.transaction(async (txn) => {
			log.addEntry(value, txn.id);
		});
	}
}

describe('Transaction log backups', () => {
	afterEach(() => {
		for (const p of tempPaths) {
			rmSync(p, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
			rmSync(`${p}.tar`, { force: true, maxRetries: 3, retryDelay: 500 });
		}
		tempPaths.length = 0;
	});

	it('backs up and restores the transaction log store (directory)', () =>
		dbRunner(async ({ db }) => {
			await writeLog(db, 'roundtrip', 5);
			const srcBytes = readFileSync(join(db.path, 'transaction_logs', 'roundtrip', '1.txnlog'));

			const backupDir = tempPath();
			const id = await db.backup(backupDir, { transactionLogs: true });

			const backupLog = join(backupDir, 'transaction_logs', String(id), 'roundtrip', '1.txnlog');
			expect(existsSync(backupLog)).toBe(true);
			expect(readFileSync(backupLog)).toEqual(srcBytes);

			const restoreDir = tempPath();
			await backups.restore(backupDir, restoreDir);

			const restoredLog = join(restoreDir, 'transaction_logs', 'roundtrip', '1.txnlog');
			expect(existsSync(restoredLog)).toBe(true);
			expect(readFileSync(restoredLog)).toEqual(srcBytes); // byte-identical restore

			// The reopened database discovers the restored log store.
			const restored = new RocksDatabase(restoreDir);
			restored.open();
			try {
				expect(restored.useLog('roundtrip').getStats().fileCount).toBe(1);
			} finally {
				restored.close();
			}
		}));

	it('does not back up transaction logs by default', () =>
		dbRunner(async ({ db }) => {
			await writeLog(db, 'nolog', 3);
			const backupDir = tempPath();
			await db.backup(backupDir);
			expect(existsSync(join(backupDir, 'transaction_logs'))).toBe(false);
		}));

	it('preserves the log file mtime through backup and restore', () =>
		dbRunner(async ({ db }) => {
			await writeLog(db, 'mtimelog', 3);
			const srcLog = join(db.path, 'transaction_logs', 'mtimelog', '1.txnlog');
			// Age the source well past "now" so a fresh mtime would be obvious.
			const old = new Date(Date.now() - 10 * 86_400_000);
			utimesSync(srcLog, old, old);

			const backupDir = tempPath();
			await db.backup(backupDir, { transactionLogs: true });
			const restoreDir = tempPath();
			await backups.restore(backupDir, restoreDir);

			const restoredMtime = statSync(
				join(restoreDir, 'transaction_logs', 'mtimelog', '1.txnlog')
			).mtimeMs;
			// Preserved (~10 days ago), not reset to now — else retention would break.
			expect(Math.abs(restoredMtime - old.getTime())).toBeLessThan(2000);
		}));

	it('streams the transaction log store and round-trips', () =>
		dbRunner(async ({ db }) => {
			await writeLog(db, 'streamlog', 4);
			const srcBytes = readFileSync(join(db.path, 'transaction_logs', 'streamlog', '1.txnlog'));

			const chunks: Buffer[] = [];
			const stream = new WritableStream<Uint8Array>({
				write(chunk) {
					chunks.push(Buffer.from(chunk));
				},
			});
			await db.backup(stream, { transactionLogs: true });

			const dir = tempPath();
			mkdirSync(dir, { recursive: true });
			const tarPath = `${tempPath()}.tar`;
			writeFileSync(tarPath, Buffer.concat(chunks));
			tar.extract({ file: tarPath, cwd: dir, sync: true });

			const extractedLog = join(dir, 'transaction_logs', 'streamlog', '1.txnlog');
			expect(existsSync(extractedLog)).toBe(true);
			expect(readFileSync(extractedLog)).toEqual(srcBytes);

			const restored = new RocksDatabase(dir);
			restored.open();
			try {
				expect(restored.useLog('streamlog').getStats().fileCount).toBe(1);
			} finally {
				restored.close();
			}
		}));

	it('purge removes the transaction log snapshots of purged backups', () =>
		dbRunner(async ({ db }) => {
			const backupDir = tempPath();
			await writeLog(db, 'purgelog', 3);
			const id1 = await db.backup(backupDir, { transactionLogs: true });
			await writeLog(db, 'purgelog', 3);
			const id2 = await db.backup(backupDir, { transactionLogs: true });

			expect(existsSync(join(backupDir, 'transaction_logs', String(id1)))).toBe(true);
			expect(existsSync(join(backupDir, 'transaction_logs', String(id2)))).toBe(true);

			await backups.purge(backupDir, 1); // keep only the newest (id2)

			expect(existsSync(join(backupDir, 'transaction_logs', String(id1)))).toBe(false);
			expect(existsSync(join(backupDir, 'transaction_logs', String(id2)))).toBe(true);
		}));

	it("delete removes the deleted backup's transaction log snapshot", () =>
		dbRunner(async ({ db }) => {
			const backupDir = tempPath();
			await writeLog(db, 'deletelog', 3);
			const id1 = await db.backup(backupDir, { transactionLogs: true });
			await writeLog(db, 'deletelog', 2);
			const id2 = await db.backup(backupDir, { transactionLogs: true });

			await backups.delete(backupDir, id1);

			expect(existsSync(join(backupDir, 'transaction_logs', String(id1)))).toBe(false);
			expect(existsSync(join(backupDir, 'transaction_logs', String(id2)))).toBe(true);
		}));

	it('restore wipes stale transaction logs before restoring', () =>
		dbRunner(async ({ db }) => {
			await writeLog(db, 'wipelog', 3);
			const backupDir = tempPath();
			await db.backup(backupDir, { transactionLogs: true });

			const restoreDir = tempPath();
			await backups.restore(backupDir, restoreDir);

			// Simulate a straggler left by a previous (newer) restore.
			const straggler = join(restoreDir, 'transaction_logs', 'wipelog', '999.txnlog');
			writeFileSync(straggler, 'stale');
			expect(existsSync(straggler)).toBe(true);

			// Restoring again must wipe transaction_logs first.
			await backups.restore(backupDir, restoreDir);
			expect(existsSync(straggler)).toBe(false);
			expect(existsSync(join(restoreDir, 'transaction_logs', 'wipelog', '1.txnlog'))).toBe(true);
		}));

	it('sweeps stale staging directories left by a crashed backup', () =>
		dbRunner(async ({ db }) => {
			await writeLog(db, 'stagelog', 2);
			const backupDir = tempPath();
			await db.backup(backupDir, { transactionLogs: true });

			// Simulate a backup process that died mid-snapshot: a partial staging
			// directory that was never renamed into place.
			const stale = join(backupDir, 'transaction_logs', '.staging-999');
			mkdirSync(join(stale, 'stagelog'), { recursive: true });
			writeFileSync(join(stale, 'stagelog', '1.txnlog'), 'partial');

			const id = await db.backup(backupDir, { transactionLogs: true });
			expect(existsSync(stale)).toBe(false);
			expect(existsSync(join(backupDir, 'transaction_logs', String(id)))).toBe(true);
		}));

	it('purge prunes stale staging directories as orphans', () =>
		dbRunner(async ({ db }) => {
			await writeLog(db, 'stagepurge', 2);
			const backupDir = tempPath();
			await db.backup(backupDir, { transactionLogs: true });

			const stale = join(backupDir, 'transaction_logs', '.staging-777');
			mkdirSync(stale, { recursive: true });

			await backups.purge(backupDir, 1);
			expect(existsSync(stale)).toBe(false);
		}));

	it('a staged (unpublished) snapshot is never restored', () =>
		dbRunner(async ({ db }) => {
			await writeLog(db, 'partial', 3);
			const backupDir = tempPath();
			const id = await db.backup(backupDir, { transactionLogs: true });

			// Simulate a crash before the atomic publish: the snapshot exists only
			// under its staging name.
			const logsRoot = join(backupDir, 'transaction_logs');
			renameSync(join(logsRoot, String(id)), join(logsRoot, `.staging-${id}`));

			// The restore succeeds and treats the backup as having captured no logs:
			// the destination gets no (partial) transaction logs.
			const restoreDir = tempPath();
			await backups.restore(backupDir, restoreDir);
			expect(existsSync(join(restoreDir, 'transaction_logs'))).toBe(false);
		}));

	it('restored logs do not contain entries written after the backup', () =>
		dbRunner({ skipOpen: true }, async ({ db, dbPath }) => {
			db.open();

			// State A: five entries, then back up.
			await writeLog(db, 'log', 5, 'A');
			const backupDir = tempPath();
			await db.backup(backupDir, { transactionLogs: true });

			// Future updates (state B): three more entries, not in the backup.
			await writeLog(db, 'log', 3, 'B');
			expect(Array.from(db.useLog('log').query({ start: 0 })).length).toBe(8);

			// Roll the database back by restoring the backup over its own (state-B)
			// directory. The restore must wipe the future log data.
			db.close();
			await backups.restore(backupDir, dbPath);

			const restored = new RocksDatabase(dbPath);
			restored.open();
			try {
				// readUncommitted reads the durable on-disk entries directly (a freshly
				// reopened store hasn't re-derived its committed position) — exactly the
				// lens for "what the restored log actually contains".
				const entries = Array.from(
					restored.useLog('log').query({ start: 0, readUncommitted: true })
				);
				// Only the five pre-backup 'A' entries survive; the three 'B' ones are gone.
				expect(entries.length).toBe(5);
				expect(entries.every((entry) => entry.data.every((byte) => byte === 0x41))).toBe(true);
				expect(entries.some((entry) => entry.data.includes(0x42))).toBe(false);
			} finally {
				restored.close();
			}
		}));
});
