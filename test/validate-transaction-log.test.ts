import { backups, RocksDatabase, validateTransactionLogStore } from '../src/index.js';
import { dbRunner, generateDBPath } from './lib/util.js';
import { readFileSync, rmSync, truncateSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

const tempPaths: string[] = [];

function tempPath(): string {
	const p = generateDBPath();
	tempPaths.push(p);
	return p;
}

/** Writes `count` committed transaction-log entries to `name`. */
async function writeLog(db: RocksDatabase, name: string, count = 5): Promise<void> {
	const log = db.useLog(name);
	const value = Buffer.alloc(100, 'x');
	for (let i = 0; i < count; i++) {
		await db.transaction(async (txn) => {
			log.addEntry(value, txn.id);
		});
	}
}

describe('validateTransactionLogStore', () => {
	afterEach(() => {
		for (const p of tempPaths) {
			rmSync(p, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
		}
		tempPaths.length = 0;
	});

	it('validates a store written by the database', () =>
		dbRunner(async ({ db }) => {
			await writeLog(db, 'validlog', 5);

			const result = await validateTransactionLogStore(
				join(db.path, 'transaction_logs', 'validlog')
			);
			expect(result.valid).toBe(true);
			expect(result.errors).toEqual([]);
			expect(result.warnings).toEqual([]);
			expect(result.files.length).toBe(1);
			expect(result.files[0].file).toBe('1.txnlog');
			expect(result.files[0].sequence).toBe(1);
			expect(result.files[0].entries).toBe(5);
			expect(result.files[0].valid).toBe(true);
			expect(result.files[0].validBytes).toBeGreaterThan(0);
		}));

	it('rejects when the store directory does not exist', async () => {
		await expect(validateTransactionLogStore(join(tempPath(), 'nope'))).rejects.toThrow(
			/does not exist/
		);
	});

	it('reports a corrupted header as an error', () =>
		dbRunner(async ({ db }) => {
			await writeLog(db, 'badheader', 3);
			const storePath = join(db.path, 'transaction_logs', 'badheader');
			db.close();

			// clobber the header token
			const logFile = join(storePath, '1.txnlog');
			const bytes = readFileSync(logFile);
			bytes.writeUInt32BE(0xdeadbeef, 0);
			writeFileSync(logFile, bytes);

			const result = await validateTransactionLogStore(storePath);
			expect(result.valid).toBe(false);
			expect(result.files[0].valid).toBe(false);
			expect(result.files[0].errors[0]).toMatch(/token/);
		}));

	it('reports a torn tail as a warning, or an error in strict mode', () =>
		dbRunner(async ({ db }) => {
			await writeLog(db, 'torntail', 3);
			const storePath = join(db.path, 'transaction_logs', 'torntail');
			db.close();

			// append a partial entry: a full header declaring 5000 bytes, but only 8
			// data bytes before EOF
			const logFile = join(storePath, '1.txnlog');
			const partial = Buffer.alloc(13 + 8);
			partial.writeDoubleBE(Date.now(), 0);
			partial.writeUInt32BE(5000, 8);
			partial.writeUInt8(1, 12);
			writeFileSync(logFile, Buffer.concat([readFileSync(logFile), partial]));

			const result = await validateTransactionLogStore(storePath);
			expect(result.valid).toBe(true);
			expect(result.files[0].warnings[0]).toMatch(/Torn\/partial entry/);
			expect(result.files[0].entries).toBe(3);

			const strict = await validateTransactionLogStore(storePath, { strict: true });
			expect(strict.valid).toBe(false);
			expect(strict.files[0].errors[0]).toMatch(/Torn\/partial entry/);
		}));

	it('reports a corrupt txn.state as a store-level error', () =>
		dbRunner(async ({ db }) => {
			await writeLog(db, 'badstate', 3);
			const storePath = join(db.path, 'transaction_logs', 'badstate');
			db.close();

			// a txn.state that is not exactly one 8-byte LogPosition is corrupt
			writeFileSync(join(storePath, 'txn.state'), Buffer.from([1, 2, 3]));

			const result = await validateTransactionLogStore(storePath);
			expect(result.valid).toBe(false);
			expect(result.errors[0]).toMatch(/txn\.state is 3 bytes/);
		}));
});

describe('backups.verify transaction logs', () => {
	afterEach(() => {
		for (const p of tempPaths) {
			rmSync(p, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
		}
		tempPaths.length = 0;
	});

	it('verifies a backup with a clean transaction log snapshot', () =>
		dbRunner(async ({ db }) => {
			await writeLog(db, 'verifyok', 5);
			const backupDir = tempPath();
			const id = await db.backup(backupDir, { transactionLogs: true });

			await expect(backups.verify(backupDir, id)).resolves.toBeUndefined();
		}));

	it('verifies a backup without transaction logs', () =>
		dbRunner(async ({ db }) => {
			await db.put('key', 'value');
			const backupDir = tempPath();
			const id = await db.backup(backupDir);

			await expect(backups.verify(backupDir, id)).resolves.toBeUndefined();
		}));

	it('fails verification when a snapshot log file is corrupted', () =>
		dbRunner(async ({ db }) => {
			await writeLog(db, 'verifybad', 5);
			const backupDir = tempPath();
			const id = await db.backup(backupDir, { transactionLogs: true });

			const snapshotLog = join(backupDir, 'transaction_logs', String(id), 'verifybad', '1.txnlog');
			const bytes = readFileSync(snapshotLog);
			bytes.writeUInt32BE(0xdeadbeef, 0);
			writeFileSync(snapshotLog, bytes);

			await expect(backups.verify(backupDir, id)).rejects.toThrow(
				/transaction log verification failed.*verifybad/s
			);
		}));

	it('fails verification when a snapshot log file has a torn tail', () =>
		dbRunner(async ({ db }) => {
			await writeLog(db, 'verifytorn', 5);
			const backupDir = tempPath();
			const id = await db.backup(backupDir, { transactionLogs: true });

			// snapshots are copied on committed entry boundaries, so a mid-entry cut
			// means the snapshot is incomplete — strict validation must fail it
			const snapshotLog = join(backupDir, 'transaction_logs', String(id), 'verifytorn', '1.txnlog');
			truncateSync(snapshotLog, readFileSync(snapshotLog).length - 5);

			await expect(backups.verify(backupDir, id)).rejects.toThrow(
				/transaction log verification failed/
			);
		}));

	it('skips transaction log validation when verifyTransactionLogs is false', () =>
		dbRunner(async ({ db }) => {
			await writeLog(db, 'verifyskip', 5);
			const backupDir = tempPath();
			const id = await db.backup(backupDir, { transactionLogs: true });

			const snapshotLog = join(backupDir, 'transaction_logs', String(id), 'verifyskip', '1.txnlog');
			const bytes = readFileSync(snapshotLog);
			bytes.writeUInt32BE(0xdeadbeef, 0);
			writeFileSync(snapshotLog, bytes);

			await expect(
				backups.verify(backupDir, id, { verifyTransactionLogs: false })
			).resolves.toBeUndefined();
		}));
});
