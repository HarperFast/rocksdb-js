import { RocksDatabase, type Transaction } from '../src/index.ts';
import { generateDBPath } from '../test/lib/util.ts';
import { createWorkerBootstrapScript } from '../test/lib/worker-bootstrap.ts';
import { stressTest } from './setup.ts';
import { rmSync } from 'node:fs';
import { Worker } from 'node:worker_threads';
import { afterEach, describe, expect } from 'vitest';

/**
 * Commit-stamping performance budgets (docs/design/local-mutation-stamping.md
 * §6), asserted enabled-vs-disabled in the same process, interleaved:
 *
 *   B1  uncontended single-put commit, keep path        p50 <= 1.05x, p99 <= 1.10x
 *   B2  forced re-stamp every commit (setTimestamp(1))  p50 <= 1.4x
 *   B3  10k-put batch, keep path                        <= 1.05x
 *   B4  10k-put batch, forced re-stamp (rebuild)        <= 1.75x the keep batch
 *   B8  logged single-entry commit (claim under lock)   p50 <= 1.05x
 *
 * Absolute numbers are printed for the PR record. Generous repetition +
 * interleaving keep this stable on CI hardware; B5 (zero allocations) and B7
 * (lock-freedom) are GoogleTest assertions in test/native/local_stamp_test.cc.
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

const payload = Buffer.alloc(128, 'x');

function percentile(samples: number[], p: number): number {
	const sorted = [...samples].sort((a, b) => a - b);
	return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))];
}

/** Median of per-round paired ratios: immune to machine drift across rounds. */
function medianRatio(a: number[], b: number[]): number {
	const ratios = a.map((value, i) => b[i] / value);
	return percentile(ratios, 0.5);
}

function commitOnce(db: RocksDatabase, restamp: boolean, key: string): void {
	db.transactionSync((txn: Transaction) => {
		if (restamp) {
			txn.setTimestamp(1000.5);
		}
		txn.putSync(key, payload);
	});
}

/**
 * Interleaved A/B sampling of INDIVIDUAL commit latencies (µs), so the p99 is
 * a real tail percentile rather than a percentile of round means.
 */
function sampleCommits(
	a: RocksDatabase,
	b: RocksDatabase,
	restamp: boolean,
	rounds: number,
	perRound: number
): { a: number[]; b: number[] } {
	const samples = { a: [] as number[], b: [] as number[] };
	for (let round = 0; round < rounds; round++) {
		for (const [db, bucket] of [
			[a, samples.a],
			[b, samples.b],
		] as [RocksDatabase, number[]][]) {
			for (let i = 0; i < perRound; i++) {
				const start = process.hrtime.bigint();
				commitOnce(db, restamp, `k${i & 1023}`);
				bucket.push(Number(process.hrtime.bigint() - start) / 1000);
			}
		}
	}
	return samples;
}

describe('commit stamping budgets', () => {
	stressTest(
		'B1/B2: single-put commit ratios (keep and forced re-stamp)',
		{ mode: 'essential' },
		() => {
			const plain = RocksDatabase.open(trackedPath(), { encoding: 'binary' });
			const stamped = RocksDatabase.open(trackedPath(), {
				encoding: 'binary',
				commitStamping: true,
			});
			try {
				// warm-up
				sampleCommits(plain, stamped, false, 2, 2000);

				// Median of per-round paired ratios: immune to machine drift.
				const roundRatios50: number[] = [];
				const roundRatios99: number[] = [];
				for (let round = 0; round < 12; round++) {
					const keep = sampleCommits(plain, stamped, false, 1, 2000);
					roundRatios50.push(percentile(keep.b, 0.5) / percentile(keep.a, 0.5));
					roundRatios99.push(percentile(keep.b, 0.99) / percentile(keep.a, 0.99));
				}
				const keepP50Ratio = percentile(roundRatios50, 0.5);
				const keepP99Ratio = percentile(roundRatios99, 0.5);
				console.log(
					`B1 keep path: median per-round p50 ratio=${keepP50Ratio.toFixed(3)} (median p99 ratio=${keepP99Ratio.toFixed(3)})`
				);
				expect(keepP50Ratio).toBeLessThanOrEqual(1.05);
				expect(keepP99Ratio).toBeLessThanOrEqual(1.1);

				// B2: setTimestamp(1000.5) is below the watermark on the stamped DB
				// (every commit re-stamps + rebuilds); on the plain DB it is just a
				// timestamp assignment, so the ratio prices the whole re-stamp path.
				const restampRatios: number[] = [];
				for (let round = 0; round < 10; round++) {
					const restamp = sampleCommits(plain, stamped, true, 1, 2000);
					restampRatios.push(percentile(restamp.b, 0.5) / percentile(restamp.a, 0.5));
				}
				const restampP50Ratio = percentile(restampRatios, 0.5);
				console.log(`B2 re-stamp: median per-round p50 ratio=${restampP50Ratio.toFixed(3)}`);
				// The re-stamp path's inherent cost (claim + full-order append rebuild)
				// measures 1.21-1.27x on this hardware; the budget bounds regression
				// above that inherent cost, not the adjudicated supported-API choice.
				expect(restampP50Ratio).toBeLessThanOrEqual(1.4);
			} finally {
				stamped.close();
				plain.close();
			}
		}
	);

	stressTest('B3/B4: 10k-put batch (keep and forced re-stamp)', { mode: 'essential' }, () => {
		const plain = RocksDatabase.open(trackedPath(), { encoding: 'binary' });
		const stamped = RocksDatabase.open(trackedPath(), { encoding: 'binary', commitStamping: true });
		try {
			const BATCH = 10000;
			const run = (db: RocksDatabase, restamp: boolean): number => {
				const start = process.hrtime.bigint();
				db.transactionSync((txn: Transaction) => {
					if (restamp) {
						txn.setTimestamp(1000.5);
					}
					for (let i = 0; i < BATCH; i++) {
						txn.putSync(`b${i}`, payload);
					}
				});
				return Number(process.hrtime.bigint() - start) / 1e6;
			};

			run(plain, false); // warm-up
			run(stamped, false);

			const samples = {
				plainKeep: [] as number[],
				stampedKeep: [] as number[],
				stampedRestamp: [] as number[],
			};
			for (let i = 0; i < 9; i++) {
				samples.plainKeep.push(run(plain, false));
				samples.stampedKeep.push(run(stamped, false));
				samples.stampedRestamp.push(run(stamped, true));
			}
			const plainKeep = percentile(samples.plainKeep, 0.5);
			const stampedKeep = percentile(samples.stampedKeep, 0.5);
			const stampedRestamp = percentile(samples.stampedRestamp, 0.5);
			const keepRatio = medianRatio(samples.plainKeep, samples.stampedKeep);
			const restampRatio = medianRatio(samples.stampedKeep, samples.stampedRestamp);
			console.log(
				`B3 10k keep: plain p50=${plainKeep.toFixed(1)}ms stamped p50=${stampedKeep.toFixed(1)}ms median ratio=${keepRatio.toFixed(3)}`
			);
			console.log(
				`B4 10k re-stamp: p50=${stampedRestamp.toFixed(1)}ms (median ${restampRatio.toFixed(3)}x the keep batch)`
			);
			expect(keepRatio).toBeLessThanOrEqual(1.05);
			// Inherent append-rebuild cost measures 1.43-1.51x the keep batch.
			expect(restampRatio).toBeLessThanOrEqual(1.75);
		} finally {
			stamped.close();
			plain.close();
		}
	});

	stressTest('B8: logged commit ratio + strict key monotonicity', { mode: 'essential' }, () => {
		const plainPath = trackedPath();
		const stampedPath = trackedPath();
		const plain = RocksDatabase.open(plainPath, {
			encoding: 'binary',
			transactionLogsPath: `${plainPath}-logs`,
		});
		const stamped = RocksDatabase.open(stampedPath, {
			encoding: 'binary',
			commitStamping: true,
			transactionLogsPath: `${stampedPath}-logs`,
		});
		try {
			const entry = Buffer.alloc(128, 'e');
			const run = (db: RocksDatabase, count: number): number => {
				const log = db.useLog('audit');
				const start = process.hrtime.bigint();
				for (let i = 0; i < count; i++) {
					db.transactionSync((txn: Transaction) => {
						txn.putSync(`k${i & 1023}`, payload);
						log.addEntry(entry, txn.id);
					});
				}
				return Number(process.hrtime.bigint() - start) / 1000 / count;
			};

			run(plain, 2000); // warm-up
			run(stamped, 2000);
			const samplesPlain: number[] = [];
			const samplesStamped: number[] = [];
			for (let i = 0; i < 12; i++) {
				samplesPlain.push(run(plain, 2000));
				samplesStamped.push(run(stamped, 2000));
			}
			const ratio = medianRatio(samplesPlain, samplesStamped);
			console.log(
				`B8 logged commit: plain p50=${percentile(samplesPlain, 0.5).toFixed(2)}µs stamped p50=${percentile(samplesStamped, 0.5).toFixed(2)}µs median per-round ratio=${ratio.toFixed(3)}`
			);
			expect(ratio).toBeLessThanOrEqual(1.05);

			// Strict per-log key monotonicity on the stamped store (§3.4).
			const log = stamped.useLog('audit');
			let last = 0;
			let count = 0;
			for (const e of log.query({ start: 1 })) {
				expect(e.timestamp).toBeGreaterThan(last);
				last = e.timestamp;
				count++;
			}
			expect(count).toBeGreaterThan(10000);
		} finally {
			stamped.close();
			plain.close();
		}
	});

	stressTest(
		'B10: overlapping transactions re-stamp rate and p95',
		{ mode: 'essential' },
		async () => {
			// The re-stamp trigger is transaction OVERLAP (a commit landing after a
			// concurrent transaction's candidate was fixed), not replication alone —
			// this measures the concurrency shape harper actually runs: hold a
			// window of open transactions, commit them in sequence, repeat.
			const plain = RocksDatabase.open(trackedPath(), { encoding: 'binary' });
			const stamped = RocksDatabase.open(trackedPath(), {
				encoding: 'binary',
				commitStamping: true,
			});
			try {
				const WINDOW = 8;
				const ROUNDS = 250;
				const run = async (db: RocksDatabase): Promise<{ samples: number[]; restamps: number }> => {
					const samples: number[] = [];
					let restamps = 0;
					for (let round = 0; round < ROUNDS; round++) {
						const holds: { resolve: () => void; done: Promise<unknown> }[] = [];
						for (let i = 0; i < WINDOW; i++) {
							let release!: () => void;
							const gate = new Promise<void>((r) => (release = r));
							const done = db.transaction(async (t: Transaction) => {
								t.putSync(`o${round & 63}-${i}`, payload);
								await gate;
								return t;
							});
							holds.push({ resolve: release, done });
						}
						// Commit in REVERSE creation order — the worst case: the newest
						// candidate commits first and pushes the watermark above every
						// earlier-created transaction's candidate.
						for (const hold of holds.reverse()) {
							const start = process.hrtime.bigint();
							hold.resolve();
							const txn = (await hold.done) as Transaction;
							samples.push(Number(process.hrtime.bigint() - start) / 1000);
							const committed = txn.getCommittedLocalTime();
							if (committed !== undefined && committed !== txn.getTimestamp()) {
								restamps++;
							}
						}
					}
					return { samples, restamps };
				};

				await run(plain); // warm-up
				await run(stamped);
				const plainRun = await run(plain);
				const stampedRun = await run(stamped);
				const total = ROUNDS * WINDOW;
				const p95Ratio = percentile(stampedRun.samples, 0.95) / percentile(plainRun.samples, 0.95);
				console.log(
					`B10 overlap: re-stamp rate=${((stampedRun.restamps / total) * 100).toFixed(1)}% ` +
						`(${stampedRun.restamps}/${total}), plain p95=${percentile(plainRun.samples, 0.95).toFixed(1)}µs ` +
						`stamped p95=${percentile(stampedRun.samples, 0.95).toFixed(1)}µs ratio=${p95Ratio.toFixed(3)}`
				);
				// Overlapped commits re-stamp by design; the gate bounds the latency
				// consequence for small batches (large-batch head-of-line cost scales
				// with batch size and is priced by B4).
				expect(p95Ratio).toBeLessThanOrEqual(2.0);
			} finally {
				stamped.close();
				plain.close();
			}
		}
	);

	stressTest(
		'B6: cross-env contention keeps every stamp unique',
		{ mode: 'essential' },
		async () => {
			const path = trackedPath();
			// Activate exclusively, then let workers inherit the marker.
			const activator = RocksDatabase.open(path, { encoding: 'binary', commitStamping: true });
			activator.close();

			const bootstrapScript = createWorkerBootstrapScript(
				'./stress-test/workers/stress-commit-stamping-worker.mts'
			);
			const WORKERS = 4;
			const COMMITS = 2000;
			const collected = await Promise.all(
				Array.from({ length: WORKERS }, () => {
					const worker = new Worker(bootstrapScript, {
						eval: true,
						workerData: { path, commits: COMMITS },
					});
					return new Promise<number[]>((resolve, reject) => {
						worker.on('message', (message) => resolve(message.stamps));
						worker.on('error', reject);
						worker.on('exit', (code) => {
							if (code !== 0) reject(new Error(`worker exited ${code}`));
						});
					});
				})
			);
			const all = collected.flat();
			const unique = new Set(all);
			console.log(`B6: ${all.length} stamps across ${WORKERS} workers, ${unique.size} unique`);
			expect(unique.size).toBe(all.length);
		}
	);
});
