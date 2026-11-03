import { RocksDatabase, getRegistryStatus, shutdown } from '../../src/index.ts';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';

const FILES = 4;
const KEYS_PER_FILE = 10_000;
const AFTER_KEYS = 100;
const CLOSE_BOUND_MS = 5_000;

const keyOf = (n: number) => `key-${String(n).padStart(6, '0')}`;

/** Each flush spans the whole key range, so the L0 files overlap and must be merged. */
function writeOverlappingFiles(db: RocksDatabase, files: number, keysPerFile: number): void {
	for (let file = 0; file < files; file++) {
		for (let i = 0; i < keysPerFile; i++) {
			const n = i * files + file;
			db.putSync(keyOf(n), `value-${n}`);
		}
		db.flushSync();
	}
}

function writeUnflushed(db: RocksDatabase): void {
	for (let i = 0; i < AFTER_KEYS; i++) {
		db.putSync(`after-${i}`, `after-value-${i}`);
	}
}

function verify(
	db: RocksDatabase,
	files: number = FILES,
	keysPerFile: number = KEYS_PER_FILE
): void {
	for (let n = 0; n < files * keysPerFile; n++) {
		if (db.getSync(keyOf(n)) !== `value-${n}`) throw new Error(`Missing or wrong ${keyOf(n)}`);
	}
	for (let i = 0; i < AFTER_KEYS; i++) {
		if (db.getSync(`after-${i}`) !== `after-value-${i}`)
			throw new Error(`Missing after-${i}: the close-time flush did not persist it`);
	}
}

async function waitForCompaction(db: RocksDatabase): Promise<void> {
	const deadline = performance.now() + 10_000;
	while ((db.getDBIntProperty('rocksdb.num-running-compactions') ?? 0) < 1) {
		if (performance.now() > deadline) throw new Error('No background compaction started');
		await delay(20);
	}
}

function timedClose(db: RocksDatabase, label: string): number {
	const started = performance.now();
	db.close();
	const elapsed = performance.now() - started;
	if (elapsed > CLOSE_BOUND_MS)
		throw new Error(`${label} took ${elapsed}ms: close waited for background compaction`);
	return elapsed;
}

async function compactingDatabase(path: string): Promise<RocksDatabase> {
	const db = RocksDatabase.open(path, { disableWAL: true });
	writeOverlappingFiles(db, FILES, KEYS_PER_FILE);
	await waitForCompaction(db);
	writeUnflushed(db);
	return db;
}

const report = (event: Record<string, unknown>) => console.log(JSON.stringify(event));

if (!isMainThread) {
	const db = await compactingDatabase(workerData.path);
	parentPort!.postMessage('compacting');
	// Keep the handle reachable until the main thread terminates this worker.
	setInterval(() => db.isOpen(), 1_000);
} else {
	const path = process.argv[2];
	const mode = process.argv[3];

	if (mode === 'close') {
		const db = await compactingDatabase(path);
		const first = timedClose(db, 'close()');
		db.open();
		verify(db);
		// The aborted compaction left its inputs in L0; one more flush schedules it again.
		writeOverlappingFiles(db, 1, KEYS_PER_FILE);
		await waitForCompaction(db);
		const second = timedClose(db, 'close() after reopen');
		report({ mode, first, second });
	} else if (mode === 'exit') {
		// Left open: when the event loop drains, the env cleanup hook closes it.
		// (process.exit() skips that hook and closes nothing.)
		await compactingDatabase(path);
		report({ mode, compacting: true });
	} else if (mode === 'verify') {
		const db = RocksDatabase.open(path);
		verify(db);
		db.close();
		report({ mode });
	} else if (mode === 'worker') {
		const worker = new Worker(new URL(import.meta.url), { workerData: { path } });
		await new Promise<void>((resolve, reject) => {
			worker.once('message', () => resolve());
			worker.once('error', reject);
		});
		const started = performance.now();
		await worker.terminate();
		const db = RocksDatabase.open(path);
		const elapsed = performance.now() - started;
		if (elapsed > CLOSE_BOUND_MS)
			throw new Error(`Worker teardown took ${elapsed}ms: close waited for background compaction`);
		verify(db);
		db.close();
		report({ mode, elapsed });
	} else if (mode === 'compactOnClose') {
		const files = 3; // below level0_file_num_compaction_trigger: no automatic compaction
		const keysPerFile = 100;
		RocksDatabase.config({ compactOnClose: true });
		const db = RocksDatabase.open(path, { disableWAL: true });
		writeOverlappingFiles(db, files, keysPerFile);
		writeUnflushed(db);
		const started = performance.now();
		db.close();
		const elapsed = performance.now() - started;
		const floor = files * keysPerFile * Number(process.env.ROCKSDB_JS_COMPACTION_KEY_DELAY_MS);
		if (elapsed < floor)
			throw new Error(`compactOnClose close took ${elapsed}ms, under its ${floor}ms compaction`);
		RocksDatabase.config({ compactOnClose: false });
		db.open();
		const level0 = db.getDBProperty('rocksdb.num-files-at-level0');
		if (level0 !== '0') throw new Error(`compactOnClose left ${level0} L0 files`);
		verify(db, files, keysPerFile);
		db.close();
		report({ mode, elapsed });
	} else if (mode === 'checkpoint') {
		const db = RocksDatabase.open(path);
		const log = db.useLog('close');
		for (let i = 0; i < 10; i++) {
			await db.transaction(async (txn) => {
				log.addEntry(Buffer.from(`entry-${i}`), txn.id);
				db.putSync(`txn-${i}`, i, { transaction: txn });
			});
		}
		// Leave a flush installed but still inside its (delayed) OnFlushCompleted,
		// then close from outside the handle: the close-time flush finds nothing
		// left to write, and the listener's own update lands after its log store
		// has been unregistered.
		const flushing = db.flush().catch(() => {});
		await delay(200);
		shutdown();
		await flushing;
		const reopened = RocksDatabase.open(path);
		const replayed = Array.from(reopened.useLog('close').query({ startFromLastFlushed: true }));
		if (replayed.length !== 0)
			throw new Error(`Restart would replay ${replayed.length} flushed entries`);
		reopened.close();
		report({ mode });
	} else if (mode === 'destroyStalled') {
		// Hold the database in a write stall behind the slowed compaction, then
		// destroy it with deletion failing (ROCKSDB_JS_DESTROY_FAILURE): the
		// destroy flush must not wait out the stall, and must still persist.
		const db = await compactingDatabase(path);
		const stallDeadline = performance.now() + 10_000;
		for (let file = 0; !db.isWriteStalled(); file++) {
			if (performance.now() > stallDeadline) throw new Error('The database never stalled');
			for (let i = 0; i < 1_000; i++) db.putSync(`stall-${file}-${i}`, i);
			db.flushSync({ allowWriteStall: true });
		}
		for (let i = 0; i < AFTER_KEYS; i++) db.putSync(`tail-${i}`, i);
		const started = performance.now();
		try {
			db.destroy();
			throw new Error('Expected the injected destruction failure');
		} catch (error) {
			if (!String(error).includes('Injected database destruction failure')) throw error;
		}
		const elapsed = performance.now() - started;
		// A flush that waits out the stall takes seconds here; this one takes milliseconds.
		if (elapsed > 2_000)
			throw new Error(`destroy() took ${elapsed}ms: its flush waited out the write stall`);
		const reopened = RocksDatabase.open(path);
		verify(reopened);
		for (let i = 0; i < AFTER_KEYS; i++) {
			if (reopened.getSync(`tail-${i}`) !== i)
				throw new Error(`Missing tail-${i}: the destroy flush did not persist it`);
		}
		// Still stalled, and close() waits out a stall; destroy() does not.
		try {
			reopened.destroy();
		} catch (error) {
			if (!String(error).includes('Injected database destruction failure')) throw error;
		}
		report({ mode, elapsed });
	} else if (mode === 'retryAfterCancel') {
		const db = RocksDatabase.open(path, { disableWAL: true });
		db.putSync('key', 'value');
		try {
			db.close();
			throw new Error('Expected close to surface the injected failure');
		} catch (error) {
			if (!String(error).includes('Injected database close failure after cancel')) throw error;
		}
		if (!getRegistryStatus().some((entry) => entry.path === path && entry.closeError))
			throw new Error('The failure did not quarantine the descriptor');
		shutdown();
		const reopened = RocksDatabase.open(path);
		if (reopened.getSync('key') !== 'value') throw new Error('The retried close lost the write');
		reopened.close();
		report({ mode });
	} else {
		throw new Error(`Unknown mode ${mode}`);
	}
}
