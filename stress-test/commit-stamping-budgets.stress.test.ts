import { RocksDatabase, type Transaction } from '../src/index.ts';
import { generateDBPath } from '../test/lib/util.ts';
import { stressTest } from './setup.ts';
import { rmSync } from 'node:fs';
import { afterEach, describe, expect } from 'vitest';

/**
 * Commit-stamping performance budgets (docs/design/local-mutation-stamping.md
 * §6), asserted enabled-vs-disabled in the same process, interleaved:
 *
 *   B1  uncontended single-put commit, keep path        p50 <= 1.05x, p99 <= 1.10x
 *   B2  forced re-stamp every commit (setTimestamp(1))  p50 <= 1.25x
 *   B3  10k-put batch, keep path                        <= 1.05x
 *   B4  10k-put batch, forced re-stamp (rebuild)        <= 1.5x the keep batch
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

function commitOnce(db: RocksDatabase, restamp: boolean, key: string): void {
	db.transactionSync((txn: Transaction) => {
		if (restamp) {
			txn.setTimestamp(1000.5);
		}
		txn.putSync(key, payload);
	});
}

/** Interleaved A/B sampling: returns per-commit µs samples for each side. */
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
			const start = process.hrtime.bigint();
			for (let i = 0; i < perRound; i++) {
				commitOnce(db, restamp, `k${i & 1023}`);
			}
			bucket.push(Number(process.hrtime.bigint() - start) / 1000 / perRound);
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

				const keep = sampleCommits(plain, stamped, false, 10, 2000);
				const keepP50Ratio = percentile(keep.b, 0.5) / percentile(keep.a, 0.5);
				const keepP99Ratio = percentile(keep.b, 0.99) / percentile(keep.a, 0.99);
				console.log(
					`B1 keep path: plain p50=${percentile(keep.a, 0.5).toFixed(2)}µs stamped p50=${percentile(keep.b, 0.5).toFixed(2)}µs ratio=${keepP50Ratio.toFixed(3)} (p99 ratio=${keepP99Ratio.toFixed(3)})`
				);
				expect(keepP50Ratio).toBeLessThanOrEqual(1.05);
				expect(keepP99Ratio).toBeLessThanOrEqual(1.1);

				// B2: setTimestamp(1000.5) is below the watermark on the stamped DB
				// (every commit re-stamps + rebuilds); on the plain DB it is just a
				// timestamp assignment, so the ratio prices the whole re-stamp path.
				const restamp = sampleCommits(plain, stamped, true, 10, 2000);
				const restampP50Ratio = percentile(restamp.b, 0.5) / percentile(restamp.a, 0.5);
				console.log(
					`B2 re-stamp: plain p50=${percentile(restamp.a, 0.5).toFixed(2)}µs stamped p50=${percentile(restamp.b, 0.5).toFixed(2)}µs ratio=${restampP50Ratio.toFixed(3)}`
				);
				expect(restampP50Ratio).toBeLessThanOrEqual(1.25);
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
			for (let i = 0; i < 5; i++) {
				samples.plainKeep.push(run(plain, false));
				samples.stampedKeep.push(run(stamped, false));
				samples.stampedRestamp.push(run(stamped, true));
			}
			const plainKeep = percentile(samples.plainKeep, 0.5);
			const stampedKeep = percentile(samples.stampedKeep, 0.5);
			const stampedRestamp = percentile(samples.stampedRestamp, 0.5);
			console.log(
				`B3 10k keep: plain=${plainKeep.toFixed(1)}ms stamped=${stampedKeep.toFixed(1)}ms ratio=${(stampedKeep / plainKeep).toFixed(3)}`
			);
			console.log(
				`B4 10k re-stamp: ${stampedRestamp.toFixed(1)}ms (${(stampedRestamp / stampedKeep).toFixed(3)}x the keep batch)`
			);
			expect(stampedKeep / plainKeep).toBeLessThanOrEqual(1.05);
			expect(stampedRestamp / stampedKeep).toBeLessThanOrEqual(1.5);
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
			for (let i = 0; i < 8; i++) {
				samplesPlain.push(run(plain, 2000));
				samplesStamped.push(run(stamped, 2000));
			}
			const ratio = percentile(samplesStamped, 0.5) / percentile(samplesPlain, 0.5);
			console.log(
				`B8 logged commit: plain p50=${percentile(samplesPlain, 0.5).toFixed(2)}µs stamped p50=${percentile(samplesStamped, 0.5).toFixed(2)}µs ratio=${ratio.toFixed(3)}`
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
});
