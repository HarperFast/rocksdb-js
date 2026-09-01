import { backups, RocksDatabase, type Transaction } from '../src/index.ts';
import { generateDBPath } from './lib/util.ts';
import { existsSync, rmSync } from 'node:fs';
import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

/**
 * Commit-stamping lifecycle coverage (docs/design/local-mutation-stamping.md
 * §3.7): the durable floor, enable markers, and log key-domain state live in
 * the hidden metadata column family, so backup, restore, and checkpoint carry
 * them with no extra machinery — a restored or checkpointed store keeps
 * enforcing the marker and never re-mints a stamp its artifacts carry. Mixed
 * DB+log backups additionally carry a floor-capture artifact that restore
 * publishes into the destination and open reconciles.
 */

const paths: string[] = [];

function trackedPath(suffix = ''): string {
	const path = generateDBPath() + suffix;
	paths.push(path);
	return path;
}

afterEach(() => {
	for (const path of paths.splice(0)) {
		rmSync(path, { recursive: true, force: true });
		rmSync(`${path}-logs`, { recursive: true, force: true });
	}
});

function stampedValue(payload: string): Buffer {
	const value = Buffer.alloc(8 + payload.length);
	value.write(payload, 8);
	return value;
}

describe('commit stamping lifecycle', () => {
	it('backup + restore carry the marker, floor, and floor artifact', async () => {
		const srcPath = trackedPath();
		const backupDir = trackedPath('-backup');
		const restorePath = trackedPath('-restored');

		let futureStamp = 0;
		const db = RocksDatabase.open(srcPath, {
			commitStamping: true,
			encoding: 'binary',
			transactionLogsPath: `${srcPath}-logs`,
		});
		try {
			const log = db.useLog('audit');
			// A kept future caller timestamp pushes the watermark ahead of the
			// wall clock, so re-mint protection is actually observable below.
			const future = Date.now() + 60000;
			const txn = (await db.transaction((t: Transaction): Transaction => {
				t.setTimestamp(future);
				t.putSync('f', stampedValue('future'));
				log.addEntry(Buffer.from('future-entry'), t.id);
				return t;
			})) as Transaction;
			futureStamp = txn.getCommittedLocalTime()!;
			expect(futureStamp).toBe(future);

			await db.backup(backupDir, { transactionLogs: true });
		} finally {
			db.close();
		}

		// The floor artifact rides the log snapshot.
		const artifact = join(backupDir, 'transaction_logs', '1', 'STAMP_FLOOR');
		expect(existsSync(artifact)).toBe(true);
		const bytes = await readFile(artifact);
		expect(bytes.length).toBe(20);
		expect(bytes.subarray(0, 4).toString()).toBe('RJSF');
		expect(bytes.readDoubleBE(4)).toBeGreaterThan(futureStamp);

		await backups.restore(backupDir, restorePath);
		// Restore published the pending copy in the destination too.
		expect(existsSync(join(restorePath, 'transaction_logs', '.stamp-floor-pending'))).toBe(true);

		// The restored database enforces the marker (no option passed) and never
		// re-mints: its first stamp must exceed the pre-backup future stamp.
		const restored = RocksDatabase.open(restorePath, {
			encoding: 'binary',
			transactionLogsPath: join(restorePath, 'transaction_logs'),
		});
		try {
			expect((restored.getSync('f') as Buffer).readDoubleBE(0)).toBe(futureStamp);
			const txn = restored.transactionSync((t: Transaction): Transaction => {
				t.putSync('after-restore', stampedValue('post'));
				return t;
			}) as Transaction;
			expect(txn.getCommittedLocalTime()!).toBeGreaterThan(futureStamp);
			// Explicit false still fails closed on the restored copy.
		} finally {
			restored.close();
		}
		expect(() =>
			RocksDatabase.open(restorePath, { encoding: 'binary', commitStamping: false })
		).toThrow(/durably marked/);
	});

	it('open reconciles a floor artifact above the metadata ceiling (the backup race shape)', async () => {
		const srcPath = trackedPath();
		const backupDir = trackedPath('-backup');
		const restorePath = trackedPath('-restored');

		const db = RocksDatabase.open(srcPath, {
			commitStamping: true,
			encoding: 'binary',
			transactionLogsPath: `${srcPath}-logs`,
		});
		try {
			const log = db.useLog('audit');
			await db.transaction((t: Transaction) => {
				t.putSync('k', stampedValue('v'));
				log.addEntry(Buffer.from('entry'), t.id);
			});
			await db.backup(backupDir, { transactionLogs: true });
		} finally {
			db.close();
		}

		await backups.restore(backupDir, restorePath);

		// Simulate the DB-snapshot-older-than-log-snapshot race: doctor the
		// pending artifact to a ceiling far above the restored metadata ceiling.
		// Open must fold it, so the first post-restore stamp exceeds it.
		const doctored = Date.now() + 120000;
		const pending = join(restorePath, 'transaction_logs', '.stamp-floor-pending');
		const bytes = await readFile(pending);
		bytes.writeDoubleBE(doctored, 4);
		const complement = Buffer.alloc(8);
		complement.writeDoubleBE(doctored, 0);
		for (let i = 0; i < 8; i++) bytes[12 + i] = ~complement[i] & 0xff;
		await writeFile(pending, bytes);

		const restored = RocksDatabase.open(restorePath, {
			encoding: 'binary',
			transactionLogsPath: join(restorePath, 'transaction_logs'),
		});
		try {
			const txn = restored.transactionSync((t: Transaction): Transaction => {
				t.putSync('post', stampedValue('post'));
				return t;
			}) as Transaction;
			expect(txn.getCommittedLocalTime()!).toBeGreaterThan(doctored);
		} finally {
			restored.close();
		}
	});

	it('a checkpoint carries the metadata CF (marker + floor) atomically', async () => {
		const srcPath = trackedPath();
		const checkpointPath = trackedPath('-checkpoint');

		let futureStamp = 0;
		const db = RocksDatabase.open(srcPath, { commitStamping: true, encoding: 'binary' });
		try {
			const future = Date.now() + 60000;
			const txn = db.transactionSync((t: Transaction): Transaction => {
				t.setTimestamp(future);
				t.putSync('f', stampedValue('future'));
				return t;
			}) as Transaction;
			futureStamp = txn.getCommittedLocalTime()!;
			await db.createCheckpoint(checkpointPath);
		} finally {
			db.close();
		}

		const checkpointed = RocksDatabase.open(checkpointPath, { encoding: 'binary' });
		try {
			expect((checkpointed.getSync('f') as Buffer).readDoubleBE(0)).toBe(futureStamp);
			const txn = checkpointed.transactionSync((t: Transaction): Transaction => {
				t.putSync('post', stampedValue('post'));
				return t;
			}) as Transaction;
			// Marker inherited; floor (ceiling row, snapshotted atomically with the
			// data by the checkpoint) prevents re-minting the future stamp.
			expect(txn.getCommittedLocalTime()!).toBeGreaterThan(futureStamp);
		} finally {
			checkpointed.close();
		}
	});

	it('a corrupt floor artifact fails the open closed', async () => {
		const srcPath = trackedPath();
		const backupDir = trackedPath('-backup');
		const restorePath = trackedPath('-restored');

		const db = RocksDatabase.open(srcPath, {
			commitStamping: true,
			encoding: 'binary',
			transactionLogsPath: `${srcPath}-logs`,
		});
		try {
			const log = db.useLog('audit');
			await db.transaction((t: Transaction) => {
				t.putSync('k', stampedValue('v'));
				log.addEntry(Buffer.from('entry'), t.id);
			});
			await db.backup(backupDir, { transactionLogs: true });
		} finally {
			db.close();
		}

		await backups.restore(backupDir, restorePath);
		const pending = join(restorePath, 'transaction_logs', '.stamp-floor-pending');
		const bytes = await readFile(pending);
		bytes.writeDoubleBE(Date.now() + 120000, 4); // ceiling changed, complement stale
		await writeFile(pending, bytes);

		expect(() =>
			RocksDatabase.open(restorePath, {
				encoding: 'binary',
				transactionLogsPath: join(restorePath, 'transaction_logs'),
			})
		).toThrow(/floor artifact/);
	});
});
