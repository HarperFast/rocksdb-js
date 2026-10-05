// Optimistic commit throughput, latency and CPU under the commit patterns that reach RocksDB's
// commit lock buckets. Prints one JSON result line. Usage:
//   node benchmark/occ-commit-contention.mts --scenario one-db --workers 4 --keys 64
// --lib points at another build's dist/index.mjs so one script can compare several builds;
// --occ-lock-buckets and --occ-validation apply RocksDatabase.config() before any open.
import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { resolve } from 'node:path';
import { performance } from 'node:perf_hooks';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';
import { isMainThread, parentPort, Worker, workerData } from 'node:worker_threads';

type Scenario = 'one-db' | 'four-db' | 'mixed-sync' | 'conflict' | 'memory';
type Config = {
	lib: string;
	scenario: Scenario;
	workers: number;
	syncWorkers: number;
	keys: number;
	valueSize: number;
	concurrency: number;
	warmup: number;
	seconds: number;
	wal: boolean;
	hotKeys: number;
	occLockBuckets: number | null;
	occValidation: string | null;
};
type WorkerResult = {
	id: number;
	sync: boolean;
	commits: number;
	conflicts: number;
	tryAgains: number;
	latencies: Float64Array;
	increments?: number;
};

const PHASE_WARMUP = 0;
const PHASE_MEASURE = 1;
const PHASE_STOP = 2;

async function loadLib(lib: string) {
	return import(pathToFileURL(resolve(lib)).href);
}

function anonymousKiB(): number | null {
	if (process.platform !== 'linux') return null;
	return Number(/Anonymous:\s+(\d+)/.exec(readFileSync('/proc/self/smaps_rollup', 'utf8'))![1]);
}

function percentile(sorted: Float64Array, p: number): number {
	if (sorted.length === 0) return NaN;
	return sorted[Math.min(sorted.length - 1, Math.floor((sorted.length * p) / 100))];
}

if (isMainThread) {
	const { values } = parseArgs({
		options: {
			lib: { type: 'string', default: resolve(import.meta.dirname, '../dist/index.mjs') },
			scenario: { type: 'string', default: 'one-db' },
			workers: { type: 'string', default: '4' },
			'sync-workers': { type: 'string', default: '0' },
			keys: { type: 'string', default: '64' },
			'value-size': { type: 'string', default: '256' },
			concurrency: { type: 'string', default: '8' },
			warmup: { type: 'string', default: '1' },
			seconds: { type: 'string', default: '5' },
			wal: { type: 'boolean', default: false },
			'hot-keys': { type: 'string', default: '0' },
			'occ-lock-buckets': { type: 'string' },
			'occ-validation': { type: 'string' },
			dir: { type: 'string', default: resolve(import.meta.dirname, 'data') },
		},
	});
	const config: Config = {
		lib: resolve(values.lib),
		scenario: values.scenario as Scenario,
		workers: Number(values.workers),
		syncWorkers: Number(values['sync-workers']),
		keys: Number(values.keys),
		valueSize: Number(values['value-size']),
		concurrency: Number(values.concurrency),
		warmup: Number(values.warmup),
		seconds: Number(values.seconds),
		wal: values.wal,
		hotKeys: Number(values['hot-keys']) || Number(values.keys) * 4,
		occLockBuckets: values['occ-lock-buckets'] ? Number(values['occ-lock-buckets']) : null,
		occValidation: values['occ-validation'] ?? null,
	};
	for (const name of [
		'workers',
		'syncWorkers',
		'keys',
		'valueSize',
		'concurrency',
		'hotKeys',
	] as const) {
		assert(Number.isSafeInteger(config[name]) && config[name] >= 0, `${name} must be an integer`);
	}
	assert(config.hotKeys >= config.keys, '--hot-keys must be at least --keys');
	assert(
		['one-db', 'four-db', 'mixed-sync', 'conflict', 'memory'].includes(config.scenario),
		'scenario must be one-db, four-db, mixed-sync, conflict or memory'
	);
	const { RocksDatabase, shutdown } = await loadLib(config.lib);
	if (config.occLockBuckets !== null) {
		RocksDatabase.config({ occLockBuckets: config.occLockBuckets });
	}
	if (config.occValidation !== null) RocksDatabase.config({ occValidation: config.occValidation });
	mkdirSync(values.dir, { recursive: true });
	const root = mkdtempSync(resolve(values.dir, 'occ-'));
	const dbOptions = { disableWAL: !config.wal };

	try {
		if (config.scenario === 'memory') {
			const rssBefore = process.memoryUsage.rss();
			const anonBefore = anonymousKiB();
			const dbs = Array.from({ length: 10 }, (_, i) =>
				RocksDatabase.open(resolve(root, `db${i}`), dbOptions)
			);
			const rssAfter = process.memoryUsage.rss();
			const anonAfter = anonymousKiB();
			for (const db of dbs) db.close();
			console.log(
				JSON.stringify({
					...config,
					rssMiB: (rssAfter - rssBefore) / 1048576,
					anonMiB: anonBefore === null ? null : (anonAfter! - anonBefore) / 1024,
				})
			);
		} else {
			const workerCount =
				config.scenario === 'four-db'
					? 4
					: config.workers + (config.scenario === 'mixed-sync' ? config.syncWorkers : 0);
			const phase = new Int32Array(new SharedArrayBuffer(4));
			// Keep the main thread's handles open so workers never pay first-open/last-close costs.
			const paths = Array.from({ length: config.scenario === 'four-db' ? 4 : 1 }, (_, i) =>
				resolve(root, `db${i}`)
			);
			const mainDbs = paths.map((path) => RocksDatabase.open(path, dbOptions));
			const workers: Worker[] = [];
			const results: Promise<WorkerResult>[] = [];
			const ready: Promise<void>[] = [];
			for (let id = 0; id < workerCount; id++) {
				const sync = config.scenario === 'mixed-sync' ? id >= config.workers : false;
				const worker = new Worker(new URL(import.meta.url), {
					workerData: {
						config,
						id,
						sync,
						path: paths[config.scenario === 'four-db' ? id : 0],
						phase,
						dbOptions,
					},
				});
				workers.push(worker);
				ready.push(
					new Promise((res, rej) => {
						worker.once('error', rej);
						worker.once('message', () => res());
					})
				);
			}
			try {
				await Promise.all(ready);
			} catch (error) {
				await Promise.all(workers.map((worker) => worker.terminate()));
				throw error;
			}
			Atomics.store(phase, 0, PHASE_WARMUP);
			for (const worker of workers) {
				const result = new Promise<WorkerResult>((res, rej) => {
					worker.once('error', rej);
					worker.once('message', res);
				});
				void result.catch(() => {});
				results.push(result);
				worker.postMessage('go');
			}
			await new Promise((res) => setTimeout(res, config.warmup * 1000));
			const cpuStart = process.cpuUsage();
			const wallStart = performance.now();
			Atomics.store(phase, 0, PHASE_MEASURE);
			await new Promise((res) => setTimeout(res, config.seconds * 1000));
			Atomics.store(phase, 0, PHASE_STOP);
			const cpu = process.cpuUsage(cpuStart);
			const wall = (performance.now() - wallStart) / 1000;
			let workerResults: WorkerResult[];
			try {
				workerResults = await Promise.all(results);
			} catch (error) {
				// Other workers may still be committing; stop them before shutdown() closes the databases.
				await Promise.all(workers.map((worker) => worker.terminate()));
				throw error;
			}

			const summarize = (group: WorkerResult[]) => {
				const commits = group.reduce((sum, r) => sum + r.commits, 0);
				const all = new Float64Array(group.reduce((sum, r) => sum + r.latencies.length, 0));
				let offset = 0;
				for (const r of group) {
					all.set(r.latencies, offset);
					offset += r.latencies.length;
				}
				all.sort();
				return {
					commits,
					commitsPerSec: commits / wall,
					p50Ms: percentile(all, 50),
					p95Ms: percentile(all, 95),
					p99Ms: percentile(all, 99),
					conflicts: group.reduce((sum, r) => sum + r.conflicts, 0),
					tryAgains: group.reduce((sum, r) => sum + r.tryAgains, 0),
				};
			};
			const total = summarize(workerResults);
			const output: Record<string, unknown> = {
				...config,
				wall,
				...total,
				cpuUsPerCommit: (cpu.user + cpu.system) / total.commits,
				cpuCores: (cpu.user + cpu.system) / 1e6 / wall,
			};
			if (config.scenario === 'mixed-sync') {
				output.async = summarize(workerResults.filter((r) => !r.sync));
				output.sync = summarize(workerResults.filter((r) => r.sync));
			}
			if (config.scenario === 'conflict') {
				const increments = workerResults.reduce((sum, r) => sum + r.increments!, 0);
				let stored = 0;
				for (let k = 0; k < config.hotKeys; k++) stored += mainDbs[0].getSync(`hot-${k}`) ?? 0;
				assert.equal(stored, increments * config.keys, 'lost update: conflict not detected');
				output.conflictsPerCommit = total.conflicts / total.commits;
			}
			for (const db of mainDbs) db.close();
			console.log(JSON.stringify(output));
		}
	} finally {
		shutdown();
		rmSync(root, { recursive: true, force: true, maxRetries: 3, retryDelay: 200 });
	}
} else {
	const { config, id, sync, path, phase, dbOptions } = workerData as {
		config: Config;
		id: number;
		sync: boolean;
		path: string;
		phase: Int32Array;
		dbOptions: object;
	};
	const { RocksDatabase, Transaction } = await loadLib(config.lib);
	const db = RocksDatabase.open(path, dbOptions);
	const pad = 'x'.repeat(Math.max(0, config.valueSize - 16));
	const latencies: number[] = [];
	let commits = 0;
	let conflicts = 0;
	let tryAgains = 0;
	let increments = 0;
	const isRetryable = (error: { code?: string }) => {
		const measuring = Atomics.load(phase, 0) === PHASE_MEASURE;
		if (error?.code === 'ERR_BUSY') conflicts += measuring ? 1 : 0;
		else if (error?.code === 'ERR_TRY_AGAIN') tryAgains += measuring ? 1 : 0;
		else return false;
		return true;
	};
	const record = (started: number, startPhase: number) => {
		if (startPhase === PHASE_MEASURE && Atomics.load(phase, 0) === PHASE_MEASURE) {
			commits++;
			latencies.push(performance.now() - started);
		}
	};

	// Each in-flight slot owns its keys, so non-conflict scenarios never conflict with themselves.
	const lastCommitted: number[] = [];
	async function slotLoop(slot: number): Promise<void> {
		for (let n = 0; Atomics.load(phase, 0) !== PHASE_STOP; n++) {
			const txn = new Transaction(db.store);
			for (let j = 0; j < config.keys; j++) txn.putSync(`w${id}-s${slot}-${j}`, { n, pad });
			const startPhase = Atomics.load(phase, 0);
			const started = performance.now();
			try {
				if (sync) txn.commitSync();
				else await txn.commit();
			} catch (error) {
				txn.abort();
				if (!isRetryable(error as { code?: string })) throw error;
				n--;
				continue;
			}
			lastCommitted[slot] = n;
			record(started, startPhase);
		}
	}

	async function conflictLoop(slot: number): Promise<void> {
		let seed = (id * 7919 + slot * 104729 + 1) >>> 0;
		const random = () => {
			seed ^= seed << 13;
			seed ^= seed >>> 17;
			seed ^= seed << 5;
			return (seed >>> 0) % config.hotKeys;
		};
		while (Atomics.load(phase, 0) !== PHASE_STOP) {
			const keys = new Set<number>();
			while (keys.size < config.keys) keys.add(random());
			const started = performance.now();
			const startPhase = Atomics.load(phase, 0);
			for (;;) {
				const txn = new Transaction(db.store);
				try {
					for (const k of keys) txn.putSync(`hot-${k}`, (txn.getSync(`hot-${k}`) ?? 0) + 1);
					if (sync) txn.commitSync();
					else await txn.commit();
					break;
				} catch (error) {
					txn.abort();
					if (!isRetryable(error as { code?: string })) throw error;
				}
			}
			increments++;
			record(started, startPhase);
		}
	}

	parentPort!.postMessage('ready');
	await new Promise((res) => parentPort!.once('message', res));
	const slots = sync ? 1 : config.concurrency;
	const loop = config.scenario === 'conflict' ? conflictLoop : slotLoop;
	await Promise.all(Array.from({ length: slots }, (_, slot) => loop(slot)));
	if (config.scenario !== 'conflict') {
		for (let slot = 0; slot < slots; slot++) {
			for (let j = 0; j < config.keys; j++) {
				assert.equal(db.getSync(`w${id}-s${slot}-${j}`)?.n, lastCommitted[slot]);
			}
		}
	}
	db.close();
	const result: WorkerResult = {
		id,
		sync,
		commits,
		conflicts,
		tryAgains,
		latencies: Float64Array.from(latencies),
		increments,
	};
	parentPort!.postMessage(result);
}
