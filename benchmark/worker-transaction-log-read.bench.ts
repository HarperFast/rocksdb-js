import type { RocksDatabase } from '../dist/index.mjs';
import {
	type BenchmarkContext,
	type LMDBDatabase,
	workerBenchmark as benchmark,
	workerDescribe as describe,
} from './setup.js';
import { workerData } from 'node:worker_threads';

// In role-split benches (1 writer + N readers), worker 1 is the writer; the
// rest are readers. Worker IDs are 1-based, assigned by the framework.
const WORKER_ID = workerData?.benchmarkWorkerId ?? 1;
const WORKER_ROLE_IS_WRITER = WORKER_ID === 1;

// Sized to produce a handful of rotations during setup (so reads exercise
// the multi-file path) without creating dozens of small files. At ~113
// bytes/entry (100 byte payload + 13 byte header), 256 KB holds ~2300
// entries — so 8 workers × 1000 entries (~8K total) yields ~4 log files.
const MAX_LOG_FILE_SIZE = 256 * 1024;

// All work is multi-threaded; saturate 8 workers to surface contention in
// both engines.
const NUM_WORKERS = 8;

// Each worker contributes this many seed entries; total log size is
// NUM_WORKERS × this. 8 workers × 1000 entries = 8000 total — small enough
// that setup completes in a couple of seconds on CI, large enough that
// bulk scans take multiple ms (so reader work gates the per-tick window
// in role-split benches, regardless of writer commit cost).
const ENTRIES_PER_WORKER = 1000;

// Match lazy-durability semantics on both engines so the comparison
// reflects data-structure cost, not fsync cost:
//   - rocksdb-js's transaction log is a separate append-only file (.txnlog)
//     written via writev with no per-write fsync — it does NOT go through
//     RocksDB's WAL or storage. The bench's reads (log.query()) hit the
//     mmap'd .txnlog file directly; RocksDB is only involved for txn id
//     allocation/commit on the writer side.
//   - LMDB defaults to fsyncing the metapage on every commit. `noSync: true`
//     drops that to match the .txnlog file's no-per-write-fsync profile.
// A separate "both fully durable" comparison would explicitly fsync the
// .txnlog file per write and remove `noSync` here — that's a different bench.
const LMDB_FAIR_OPTIONS = { noSync: true };

async function rocksdbSetup(ctx: BenchmarkContext<RocksDatabase>): Promise<void> {
	const db = ctx.db;
	const log = db.useLog('0');
	ctx.log = log;
	const value = Buffer.alloc(100, 'a');
	ctx.value = value;
	let firstTs: number | undefined;
	let lastTs = 0;
	for (let i = 0; i < ENTRIES_PER_WORKER; i++) {
		await db.transaction(async (txn) => {
			log.addEntry(value, txn.id);
			const ts = txn.getTimestamp();
			if (firstTs === undefined) firstTs = ts;
			lastTs = ts;
		});
		if (i % 100 === 0) {
			await new Promise((resolve) => setTimeout(resolve, 1));
		}
	}
	ctx.firstTs = firstTs!;
	ctx.lastTs = lastTs;
}

function lmdbSetup(ctx: BenchmarkContext<LMDBDatabase>): void {
	const value = Buffer.alloc(100, 'a');
	ctx.value = value;
	// Mirror rocksdb's timestamp-as-key model so the query patterns are
	// apples-to-apples. Keys are monotonic numbers that look like ms
	// timestamps. Each worker uses a distinct numeric range (offset by
	// WORKER_ID × ENTRIES_PER_WORKER × 10) so parallel setup writes don't
	// collide on duplicate keys.
	const firstKey = Date.now() + WORKER_ID * ENTRIES_PER_WORKER * 10;
	ctx.firstKey = firstKey;
	for (let i = 0; i < ENTRIES_PER_WORKER; i++) {
		ctx.db.putSync((firstKey + i) as unknown as string, value);
	}
	ctx.lastKey = firstKey + ENTRIES_PER_WORKER - 1;
	ctx.nextKey = firstKey + ENTRIES_PER_WORKER;
}

describe(`Transaction log read access patterns (${NUM_WORKERS} workers)`, () => {
	const TOTAL_ENTRIES = NUM_WORKERS * ENTRIES_PER_WORKER;

	// Access pattern: bulk forward scan, no concurrent writes.
	// Each worker iterates every entry from start. Exercises the per-entry
	// iteration cost: mmap'd buffer reads, JS DataView decoding. Iterator
	// is opened once per tick, so this is dominated by per-entry cost,
	// not iterator-open cost.
	//
	// Metric: hz × NUM_WORKERS × TOTAL_ENTRIES = total entries scanned/sec.
	describe(`Bulk forward scan, no writes: ${NUM_WORKERS} workers each scan ~${TOTAL_ENTRIES} entries`, () => {
		benchmark('rocksdb', {
			mode: 'essential',
			numWorkers: NUM_WORKERS,
			dbOptions: { transactionLogMaxSize: MAX_LOG_FILE_SIZE },
			async setup(ctx: BenchmarkContext<RocksDatabase>) {
				await rocksdbSetup(ctx);
			},
			async bench({ log }) {
				let count = 0;
				for (const _entry of log.query({ start: 1 })) {
					count++;
				}
			},
		});

		benchmark('lmdb', {
			mode: 'essential',
			numWorkers: NUM_WORKERS,
			dbOptions: LMDB_FAIR_OPTIONS,
			async setup(ctx: BenchmarkContext<LMDBDatabase>) {
				lmdbSetup(ctx);
			},
			async bench({ db }) {
				let count = 0;
				for (const _entry of db.getRange({ snapshot: false })) {
					count++;
				}
			},
		});
	});

	// Access pattern: bulk forward scan with one concurrent writer.
	// 7 workers scan; 1 writer continuously appends. Tests whether write
	// activity slows the scan (it shouldn't on either engine — both have
	// non-blocking reader paths).
	//
	// Calibration: with TOTAL_ENTRIES = ~16000, a full scan takes ~ms; a
	// single commit takes ~ms (rocksdb) or ~µs (lmdb noSync). Reader work
	// dominates the tick window, so hz × 7 × TOTAL_ENTRIES = aggregate
	// reader throughput.
	describe(`Bulk forward scan with 1 concurrent writer (${NUM_WORKERS - 1} readers)`, () => {
		benchmark('rocksdb', {
			mode: 'essential',
			numWorkers: NUM_WORKERS,
			dbOptions: { transactionLogMaxSize: MAX_LOG_FILE_SIZE },
			async setup(ctx: BenchmarkContext<RocksDatabase>) {
				await rocksdbSetup(ctx);
			},
			async bench(ctx: BenchmarkContext<RocksDatabase>) {
				if (WORKER_ROLE_IS_WRITER) {
					await ctx.db.transaction((txn) => {
						ctx.log.addEntry(ctx.value, txn.id);
					});
				} else {
					let count = 0;
					for (const _entry of ctx.log.query({ start: 1 })) {
						count++;
					}
				}
			},
		});

		benchmark('lmdb', {
			mode: 'essential',
			numWorkers: NUM_WORKERS,
			dbOptions: LMDB_FAIR_OPTIONS,
			async setup(ctx: BenchmarkContext<LMDBDatabase>) {
				lmdbSetup(ctx);
			},
			async bench(ctx: BenchmarkContext<LMDBDatabase>) {
				if (WORKER_ROLE_IS_WRITER) {
					await ctx.db.put(ctx.nextKey++ as unknown as string, ctx.value);
				} else {
					let count = 0;
					for (const _entry of ctx.db.getRange({ snapshot: false })) {
						count++;
					}
				}
			},
		});
	});

	// Access pattern: high-frequency short-range queries.
	// Each worker repeatedly opens a fresh iterator at a random position
	// and reads at most one entry. Per-query setup cost (findPosition +
	// getMemoryMap + getLogFileSize) dominates per-entry iteration cost.
	// This is the access pattern most sensitive to mutex contention
	// across many threads.
	//
	// Metric: hz × NUM_WORKERS × QUERIES_PER_TICK = total queries/sec.
	const QUERIES_PER_TICK = 1000;
	describe(`Short-range queries: ${NUM_WORKERS} workers each open ${QUERIES_PER_TICK} iterators per tick`, () => {
		benchmark('rocksdb', {
			mode: 'essential',
			numWorkers: NUM_WORKERS,
			dbOptions: { transactionLogMaxSize: MAX_LOG_FILE_SIZE },
			async setup(ctx: BenchmarkContext<RocksDatabase>) {
				await rocksdbSetup(ctx);
			},
			async bench({ log, firstTs, lastTs }) {
				const span = lastTs - firstTs;
				for (let i = 0; i < QUERIES_PER_TICK; i++) {
					for (const _entry of log.query({
						start: firstTs + Math.random() * span,
					})) {
						break;
					}
				}
			},
		});

		benchmark('lmdb', {
			mode: 'essential',
			numWorkers: NUM_WORKERS,
			dbOptions: LMDB_FAIR_OPTIONS,
			async setup(ctx: BenchmarkContext<LMDBDatabase>) {
				lmdbSetup(ctx);
			},
			async bench({ db, firstKey, lastKey }) {
				const span = lastKey - firstKey;
				for (let i = 0; i < QUERIES_PER_TICK; i++) {
					const start = (firstKey + Math.random() * span) as unknown as string;
					for (const _entry of db.getRange({ start, snapshot: false })) {
						break;
					}
				}
			},
		});
	});

	// Access pattern: cursor-advance tail scan with concurrent writer.
	// 1 worker continuously appends; 7 workers each maintain a cursor and
	// scan only entries past their last-seen position each tick. The
	// reader work per tick is small (a few entries), so the writer's
	// commit cost gates the tick window. Mixed read+write workload.
	const WRITES_PER_WRITER_TICK = 50;
	describe(`Cursor-advance tail scan with 1 concurrent writer (${NUM_WORKERS - 1} readers, ${WRITES_PER_WRITER_TICK} writes/tick)`, () => {
		benchmark('rocksdb', {
			mode: 'essential',
			numWorkers: NUM_WORKERS,
			dbOptions: { transactionLogMaxSize: MAX_LOG_FILE_SIZE },
			async setup(ctx: BenchmarkContext<RocksDatabase>) {
				await rocksdbSetup(ctx);
				// Cursor starts at the current head — only new writes after
				// this point are consumed.
				ctx.cursorTs = ctx.lastTs;
			},
			async bench(ctx: BenchmarkContext<RocksDatabase>) {
				if (WORKER_ROLE_IS_WRITER) {
					for (let i = 0; i < WRITES_PER_WRITER_TICK; i++) {
						await ctx.db.transaction((txn) => {
							ctx.log.addEntry(ctx.value, txn.id);
						});
					}
				} else {
					let max = ctx.cursorTs;
					// Strictly-greater than cursorTs via small epsilon.
					// rocksdb-js timestamps have sub-ms precision so a small
					// epsilon is safe.
					for (const entry of ctx.log.query({ start: ctx.cursorTs + 0.0001 })) {
						if (entry.timestamp > max) max = entry.timestamp;
					}
					ctx.cursorTs = max;
				}
			},
		});

		benchmark('lmdb', {
			mode: 'essential',
			numWorkers: NUM_WORKERS,
			dbOptions: LMDB_FAIR_OPTIONS,
			async setup(ctx: BenchmarkContext<LMDBDatabase>) {
				lmdbSetup(ctx);
				// Cursor starts at the current head — only new writes after
				// this point are consumed. Numeric key, mirroring rocksdb.
				ctx.cursorKey = ctx.lastKey;
			},
			async bench(ctx: BenchmarkContext<LMDBDatabase>) {
				if (WORKER_ROLE_IS_WRITER) {
					for (let i = 0; i < WRITES_PER_WRITER_TICK; i++) {
						await ctx.db.put(ctx.nextKey++ as unknown as string, ctx.value);
					}
				} else {
					let max = ctx.cursorKey;
					for (const { key } of ctx.db.getRange({
						start: ctx.cursorKey as unknown as string,
						exclusiveStart: true,
						snapshot: false,
					})) {
						const numKey = key as unknown as number;
						if (numKey > max) max = numKey;
					}
					ctx.cursorKey = max;
				}
			},
		});
	});
});
