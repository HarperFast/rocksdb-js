// WriteBufferManager flush-policy and memtable arena A/B (`writeBufferManagerFlushPolicy`:
// oldest vs largest vs largestAcrossDBs; `arenaBlockSize`). Harper shape: several databases, many
// column families each, one process-wide WBM that is the only flush trigger (per-CF
// writeBufferSize is far above it). Imports dist, so run `pnpm build` first.
//
//   node benchmark/wbm-flush-policy.mts            # driver: runs every arm in its own process
//   ARM=<policy>:<workload> node benchmark/wbm-flush-policy.mts   # one arm (child)
//
// BENCH_ARENAS is a list of arena sizes in KiB; `default` is the automatic size (scaled to the
// WBM budget), and `1024` is RocksDB's derived size for the 16 MiB default writeBufferSize,
// i.e. the behavior before the automatic size existed.
import { spawnSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { mkdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';

const MIB = 1024 * 1024;
const DBS = Number(process.env.BENCH_DBS ?? 4);
const CFS = Number(process.env.BENCH_CFS ?? 12);
const WBM = Number(process.env.BENCH_WBM_MIB ?? 64) * MIB;
const TOTAL = Number(process.env.BENCH_TOTAL_MIB ?? 512) * MIB;
const REPS = Number(process.env.BENCH_REPS ?? 2);
const DATA_DIR = process.env.BENCH_DATA_DIR ?? join('benchmark', 'data');
const POLICIES = (process.env.BENCH_POLICIES ?? 'oldest,largest,largestAcrossDBs').split(',');
const WORKLOADS = (process.env.BENCH_WORKLOADS ?? 'skewed,crossdb,uniform').split(',');

type Target = { db: number; cf: number; bytes: number };

// skewed:  one hot table (1 KiB rows) in db0 takes ~70% of bytes; small index-like writes
//          (128 B) round-robin across every CF of every database.
// crossdb: hot table in db0 only; the other databases get a trickle of small writes, so their
//          writers keep hitting the WBM limit with nothing of their own worth flushing.
// uniform: 512 B rows round-robin across all CFs (no skew; control).
function* targets(workload: string): Generator<Target> {
	let i = 0;
	for (;;) {
		i++;
		if (workload === 'uniform') {
			const n = i % (DBS * CFS);
			yield { db: Math.floor(n / CFS), cf: n % CFS, bytes: 512 };
		} else if (workload === 'skewed') {
			if (i % 3 === 0) {
				const n = (i / 3) % (DBS * CFS);
				yield { db: Math.floor(n / CFS), cf: n % CFS, bytes: 128 };
			} else {
				yield { db: 0, cf: 0, bytes: 1024 };
			}
		} else if (workload === 'crossdb') {
			if (i % 20 === 0) {
				const n = (i / 20) % ((DBS - 1) * CFS);
				yield { db: 1 + Math.floor(n / CFS), cf: n % CFS, bytes: 128 };
			} else {
				yield { db: 0, cf: i % 4, bytes: 1024 };
			}
		} else {
			throw new Error(`unknown workload ${workload}`);
		}
	}
}

function sumStat(handles: any[], key: string): number {
	return handles.reduce((t, h) => t + Number(h.getStats(true)[key] ?? 0), 0);
}

function sumProp(dbs: any[][], prop: string): number {
	let total = 0;
	for (const cfs of dbs)
		for (const h of cfs) total += Number(h.getDBIntProperty?.(prop) ?? h.getDBProperty(prop) ?? 0);
	return total;
}

async function runArm(policy: string, workload: string) {
	const { RocksDatabase, getWriteBufferManagerStats, shutdown } = await import('../dist/index.mjs');
	RocksDatabase.config({
		writeBufferManagerSize: WBM,
		writeBufferManagerCostToCache: true,
		writeBufferManagerAllowStall: !!process.env.BENCH_ALLOW_STALL,
		writeBufferManagerFlushPolicy: policy as 'oldest' | 'largest' | 'largestAcrossDBs',
	});
	const root =
		process.env.BENCH_ARM_ROOT ??
		join(DATA_DIR, `wbm-policy-${policy}-${workload}-${randomBytes(4).toString('hex')}`);
	mkdirSync(root, { recursive: true });
	const arena = process.env.BENCH_ARENA_KIB;
	const opts = {
		enableStats: true,
		writeBufferSize: 256 * MIB,
		maxWriteBufferNumber: 4,
		...(arena && arena !== 'default' ? { arenaBlockSize: Number(arena) * 1024 } : {}),
	};
	const dbs: any[][] = [];
	for (let d = 0; d < DBS; d++) {
		const cfs: any[] = [];
		for (let c = 0; c < CFS; c++) {
			cfs.push(
				RocksDatabase.open(join(root, `db${d}`), c === 0 ? opts : { ...opts, name: `t${c}` })
			);
		}
		dbs.push(cfs);
	}
	const primaries = dbs.map((cfs) => cfs[0]);
	const pool = randomBytes(8 * MIB);
	let written = 0;
	let seq = 0;
	let peakWbm = 0;
	let maxBatchMs = 0;
	const batchMs: number[] = [];
	const gen = targets(workload);
	const start = performance.now();
	let batchStart = start;
	while (written < TOTAL) {
		const t = gen.next().value as Target;
		const off = (seq * 4099) % (pool.length - t.bytes);
		const key = `k${(Math.imul(seq, 2654435761) >>> 0).toString(36)}-${seq}`;
		dbs[t.db][t.cf].putSync(key, pool.subarray(off, off + t.bytes));
		written += t.bytes;
		if (++seq % 2048 === 0) {
			const now = performance.now();
			const ms = now - batchStart;
			batchMs.push(ms);
			if (ms > maxBatchMs) maxBatchMs = ms;
			batchStart = now;
			const usage = getWriteBufferManagerStats().memoryUsage;
			if (usage > peakWbm) peakWbm = usage;
		}
	}
	const elapsed = performance.now() - start;
	const l0AtEnd = sumProp(dbs, 'rocksdb.num-files-at-level0');
	const flushCount = primaries.reduce(
		(t, h) => t + (h.getStats()['rocksdb.db.flush.micros']?.count ?? 0),
		0
	);
	const wbmFlushes = sumStat(primaries, 'rocksdb.flush.reason.write_buffer_manager');
	const flushBytes = sumStat(primaries, 'rocksdb.flush.write.bytes');
	// let compactions settle so write-amp reflects the flush pattern, not where we stopped
	for (
		let idle = 0, deadline = performance.now() + 120_000;
		idle < 3 && performance.now() < deadline;
	) {
		const busy = primaries.some((h) => {
			const s = h.getStats();
			return (
				s['rocksdb.compaction-pending'] ||
				s['rocksdb.num-running-compactions'] ||
				s['rocksdb.num-running-flushes']
			);
		});
		idle = busy ? 0 : idle + 1;
		await delay(100);
	}
	const compactBytes = sumStat(primaries, 'rocksdb.compact.write.bytes');
	batchMs.sort((a, b) => a - b);
	const result = {
		policy,
		workload,
		mibPerSec: +(written / MIB / (elapsed / 1000)).toFixed(1),
		p99BatchMs: batchMs.length ? +batchMs[Math.floor(batchMs.length * 0.99)].toFixed(1) : 0,
		maxBatchMs: +maxBatchMs.toFixed(1),
		flushes: flushCount,
		wbmFlushes,
		avgFlushKiB: flushCount ? +(flushBytes / flushCount / 1024).toFixed(0) : 0,
		l0AtEnd,
		compactMiB: +(compactBytes / MIB).toFixed(0),
		peakWbmMiB: +(peakWbm / MIB).toFixed(1),
	};
	for (const cfs of dbs) for (let c = cfs.length - 1; c >= 0; c--) cfs[c].close();
	shutdown();
	if (!process.env.BENCH_KEEP) rmSync(root, { recursive: true, force: true });
	else console.log(`ROOT ${root}`);
	console.log('RESULT ' + JSON.stringify(result));
}

if (process.env.ARM) {
	const [policy, workload] = process.env.ARM.split(':');
	await runArm(policy, workload);
} else {
	// Outer sweeps re-run the whole policy set per point; each child reads its own WBM/arena/stall
	// settings from the environment.
	const budgets = (process.env.BENCH_WBMS_MIB ?? String(WBM / MIB)).split(',');
	const arenas = (process.env.BENCH_ARENAS ?? 'default,1024').split(',');
	const timeoutMs = Number(process.env.BENCH_ARM_TIMEOUT_S ?? 600) * 1000;
	const rows: any[] = [];
	for (const budget of budgets) {
		for (const arena of arenas) {
			for (const workload of WORKLOADS) {
				for (let rep = 0; rep < REPS; rep++) {
					for (const policy of POLICIES) {
						const armRoot = join(
							DATA_DIR,
							`wbm-policy-${policy}-${workload}-${randomBytes(4).toString('hex')}`
						);
						const env: Record<string, string | undefined> = {
							...process.env,
							ARM: `${policy}:${workload}`,
							BENCH_ARM_ROOT: armRoot,
							BENCH_WBM_MIB: budget,
							BENCH_ARENA_KIB: arena,
						};
						const child = spawnSync(process.execPath, [fileURLToPath(import.meta.url)], {
							env,
							encoding: 'utf8',
							timeout: timeoutMs,
							killSignal: 'SIGKILL',
						});
						const line = child.stdout?.split('\n').find((l) => l.startsWith('RESULT '));
						let row: any;
						if (line && child.status === 0) {
							row = JSON.parse(line.slice(7));
						} else if ((child.error as NodeJS.ErrnoException | undefined)?.code === 'ETIMEDOUT') {
							// a stalled WBM blocks the JS thread, so only an outside kill ends the arm
							row = { policy, workload, timedOut: true };
							rmSync(armRoot, { recursive: true, force: true });
						} else {
							console.error(child.stdout, child.stderr);
							throw new Error(`arm ${policy}:${workload} failed (status ${child.status})`);
						}
						row = {
							wbmMiB: Number(budget),
							arenaKiB: arena,
							stall: !!process.env.BENCH_ALLOW_STALL,
							rep,
							...row,
						};
						console.log(JSON.stringify(row));
						rows.push(row);
					}
				}
			}
		}
	}
	console.table(rows);
}
