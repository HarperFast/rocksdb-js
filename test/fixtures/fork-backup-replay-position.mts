import { RocksDatabase, backups } from '../../src/index.ts';
import { isFlushCompletedDelayedForTesting } from '../../src/load-binding.ts';
import { mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';
import * as tar from 'tar';

// A backup's txn.state must be the position recorded for exactly the flushes its
// table files hold: one behind them replays writes the files already hold (or
// that were since evicted), and one ahead of them skips writes they lack. Each
// transaction writes one log entry and one key, so in the restored database a
// transaction is either present or replayed from txn.state, never both, and never
// neither when it committed before the backup started.

const LOG = 'replay';
const BATCH = 10;
// RocksDB's default level0_file_num_compaction_trigger.
const L0_COMPACTION_TRIGGER = 4;
const report = (event: Record<string, unknown>) => console.log(JSON.stringify(event));

async function commitOne(db: RocksDatabase, i: number, padding?: string): Promise<void> {
	const log = db.useLog(LOG);
	await db.transaction(async (txn) => {
		log.addEntry(Buffer.from(`entry-${i}`), txn.id);
		db.putSync(`txn-${i}`, i, { transaction: txn });
		if (padding) db.putSync(`pad-${i}`, padding, { transaction: txn });
	});
}

async function commit(db: RocksDatabase, from: number, to: number): Promise<void> {
	for (let i = from; i < to; i++) await commitOne(db, i);
}

function replayedCount(db: RocksDatabase): number {
	return Array.from(db.useLog(LOG).query({ startFromLastFlushed: true })).length;
}

/**
 * Transactions below `committedBefore` must be restored; any up to `upTo` may be.
 * `allowBehind` accepts duplicates: the store samples its commit positions, so
 * under continuous commits the position it records for a flush can lag the
 * flushed data, live as in a backup.
 */
function verifyRestored(
	dir: string,
	committedBefore: number,
	upTo = committedBefore,
	allowBehind = false
): void {
	const restored = RocksDatabase.open(dir);
	try {
		const replayed = new Set(
			Array.from(restored.useLog(LOG).query({ startFromLastFlushed: true }), (entry) =>
				Buffer.from(entry.data).toString()
			)
		);
		let both = 0;
		let neither = 0;
		for (let i = 0; i < upTo; i++) {
			const present = restored.getSync(`txn-${i}`) === i;
			const isReplayed = replayed.has(`entry-${i}`);
			if (present && isReplayed && !allowBehind) both++;
			else if (!present && !isReplayed && i < committedBefore) neither++;
		}
		if (both || neither) {
			throw new Error(
				`Restored txn.state does not match the table files: ${both} transactions both present and replayed (position behind), ${neither} neither (position ahead)`
			);
		}
	} finally {
		restored.close();
	}
}

async function directoryBackup(db: RocksDatabase, target: string): Promise<string> {
	const id = await db.backup(target, { transactionLogs: true });
	const restoreDir = `${target}-restore`;
	await backups.restore(target, restoreDir, { backupId: id });
	return restoreDir;
}

/** `afterFilesListed` holds the first chunk, so it runs before the transaction logs are copied. */
async function streamBackup(
	db: RocksDatabase,
	target: string,
	afterFilesListed?: () => Promise<void>,
	flushBeforeBackup?: boolean
): Promise<string> {
	let firstChunk = true;
	const chunks: Buffer[] = [];
	const stream = new WritableStream<Uint8Array>({
		async write(chunk) {
			if (firstChunk && afterFilesListed) {
				firstChunk = false;
				await afterFilesListed();
			}
			chunks.push(Buffer.from(chunk));
		},
	});
	await db.backup(stream, { transactionLogs: true, flushBeforeBackup });
	writeFileSync(`${target}.tar`, Buffer.concat(chunks));
	mkdirSync(target, { recursive: true });
	tar.extract({ file: `${target}.tar`, cwd: target, sync: true });
	return target;
}

function removeTargets(target: string): void {
	rmSync(target, { force: true, recursive: true });
	rmSync(`${target}-restore`, { force: true, recursive: true });
	rmSync(`${target}.tar`, { force: true });
}

const path = process.argv[2];
const mode = process.argv[3];
const targets: string[] = [];
const newTarget = () => {
	const target = `${path}-target-${targets.length}`;
	targets.push(target);
	removeTargets(target);
	return target;
};

try {
	// disableWAL makes flushBeforeBackup the default, which is Harper's configuration.
	const db = RocksDatabase.open(path, {
		disableWAL: true,
		...(mode.startsWith('underLoad') ? { writeBufferSize: 64 * 1024 } : {}),
		...(mode === 'underLoadPessimistic' ? { pessimistic: true } : {}),
	});
	let total = 0;

	if (mode === 'directoryBehind' || mode === 'streamBehind') {
		// Flush() returns when its flush thread signals, which is normally after
		// OnFlushCompleted. A compaction finishing signals the same waiter, so leave
		// the backup's own flush to write the L0 file that triggers one: the backup
		// then resumes while that flush's delayed listener has not yet recorded the
		// position its table files hold.
		const fillToTrigger = async (database: RocksDatabase) => {
			let count = 0;
			for (let file = 0; file < L0_COMPACTION_TRIGGER; file++) {
				await commit(database, count, (count += BATCH));
				if (file < L0_COMPACTION_TRIGGER - 1) database.flushSync();
			}
			return count;
		};
		// The same setup with a plain flush must return while its listener is still delayed,
		// or this run cannot reach the interleaving it is testing.
		const probe = RocksDatabase.open(`${path}-probe`, { disableWAL: true });
		let reached: boolean;
		try {
			await fillToTrigger(probe);
			await probe.flush();
			reached = isFlushCompletedDelayedForTesting();
		} finally {
			probe.close();
			rmSync(`${path}-probe`, { force: true, recursive: true });
		}
		if (!reached) throw new Error('Flush() waited for its listener; this case proves nothing');

		total = await fillToTrigger(db);
		const target = newTarget();
		const restored =
			mode === 'directoryBehind'
				? await directoryBackup(db, target)
				: await streamBackup(db, target);
		verifyRestored(restored, total);
	} else if (mode === 'streamAhead') {
		await commit(db, total, (total += BATCH));
		const restored = await streamBackup(db, newTarget(), async () => {
			await commit(db, total, (total += BATCH));
			await db.flush();
			const deadline = performance.now() + 10_000;
			while (replayedCount(db) !== 0) {
				if (performance.now() > deadline)
					throw new Error('The live txn.state never advanced; this case proves nothing');
				await delay(10);
			}
		});
		verifyRestored(restored, total);
	} else if (mode === 'underLoad' || mode === 'underLoadPessimistic') {
		// Small memtables and a 1 KiB value per transaction keep flushes running
		// back to back; every backup must still converge and never skip a write.
		const padding = 'p'.repeat(1024);
		let writing = true;
		const writer = (async () => {
			while (writing) {
				await commitOne(db, total, padding);
				total++;
				if (total % 200 === 0) await db.flush();
			}
		})();
		while (total < 500) await delay(10);
		for (let round = 0; round < 3; round++) {
			const before = total;
			const [directory, stream] = await Promise.all([
				directoryBackup(db, newTarget()),
				streamBackup(db, newTarget()),
			]);
			const upTo = total;
			verifyRestored(directory, before, upTo, true);
			verifyRestored(stream, before, upTo, true);
		}
		writing = false;
		await writer;
	} else if (mode === 'closeDuringCapture') {
		// A second handle keeps the database open, so closing the first waits only
		// for its own backup, which is waiting on a flush whose listener is delayed.
		const other = RocksDatabase.open(path, { disableWAL: true });
		await commit(db, 0, BATCH);
		const flushing = other.flush();
		const deadline = performance.now() + 10_000;
		while (!isFlushCompletedDelayedForTesting()) {
			if (performance.now() > deadline)
				throw new Error('No flush entered the delayed listener; this case proves nothing');
			await delay(10);
		}
		const outcome = db
			.backup(newTarget(), { transactionLogs: true, flushBeforeBackup: false })
			.then(
				() => 'completed',
				(error: Error) => error.message
			);
		await delay(200);
		const start = performance.now();
		db.close();
		const closeMs = performance.now() - start;
		const result = await outcome;
		if (!isFlushCompletedDelayedForTesting())
			throw new Error('The delayed listener finished first; this case proves nothing');
		if (result === 'completed') throw new Error('The backup completed across the close');
		if (closeMs > 1_000)
			throw new Error(`Closing during the capture took ${Math.round(closeMs)}ms`);
		await flushing;
		other.close();
		report({ mode, result, closeMs });
		process.exit(0);
	} else if (mode === 'pessimistic') {
		// TransactionDB enables two-phase commit, and RocksDB then flushes inside every file
		// listing, so the capture cannot list with new flushes parked.
		db.close();
		const pessimistic = RocksDatabase.open(path, { disableWAL: true, pessimistic: true });
		await commit(pessimistic, 0, BATCH);
		const directoryTarget = newTarget();
		const id = await pessimistic.backup(directoryTarget, {
			transactionLogs: true,
			flushBeforeBackup: false,
			checkDiskSpace: false,
		});
		await commit(pessimistic, BATCH, 2 * BATCH);
		const stream = await streamBackup(pessimistic, newTarget(), undefined, false);
		await backups.restore(directoryTarget, `${directoryTarget}-restore`, { backupId: id });
		verifyRestored(`${directoryTarget}-restore`, BATCH);
		verifyRestored(stream, 2 * BATCH);
		pessimistic.close();
		report({ mode, ok: true });
		process.exit(0);
	} else {
		throw new Error(`Unknown mode ${mode}`);
	}
	db.close();
	report({ mode, ok: true });
} finally {
	for (const target of targets) removeTargets(target);
}
