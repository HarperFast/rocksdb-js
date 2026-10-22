/**
 * Short-range probe microbenchmark: the cost of `getRange()` over one key,
 * consumed to completion, against a point get.
 *
 *   node benchmark/range-probe.mts [--sst | --files 8] [--batches 21] [--probes 20000]
 *
 * Runs against `src` (native type stripping), so it needs only the native
 * binding built. Prints the median per-probe time in microseconds for each
 * scenario. Run it under `bench-run` (turbo off, one core) for stable numbers.
 */
import { RocksDatabase } from '../src/index.ts';
import { Transaction } from '../src/transaction.ts';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { parseArgs } from 'node:util';

const { values: args } = parseArgs({
	options: {
		sst: { type: 'boolean', default: false },
		// Flush after every 1/N of the keys: N SST files with disjoint key ranges,
		// so a probe at the end of one file's range steps into the next file.
		files: { type: 'string', default: '0' },
		batches: { type: 'string', default: '21' },
		probes: { type: 'string', default: '20000' },
	},
});
const BATCHES = Number(args.batches);
const PROBES = Number(args.probes);
const KEY_COUNT = 5000;
const FILES = Number(args.files);

const dir = mkdtempSync(join(tmpdir(), 'range-probe-'));
const db = RocksDatabase.open(join(dir, 'db'));
const keys: string[] = [];
const value = 'v'.repeat(40);
for (let i = 0; i < KEY_COUNT; i++) {
	const key = `k${String(i).padStart(6, '0')}`;
	keys.push(key);
	db.putSync(key, value);
	if (FILES > 0 && (i + 1) % Math.ceil(KEY_COUNT / FILES) === 0) {
		db.flushSync();
	}
}
// A run of deleted keys right after the last probe key, for the tombstone scenario.
const lastKey = keys[KEY_COUNT - 1];
for (let i = 0; i < 1000; i++) {
	db.putSync(`t${i}`, value);
}
for (let i = 0; i < 1000; i++) {
	db.removeSync(`t${i}`);
}
db.putSync('u', value);
if (args.sst || FILES > 0) {
	db.flushSync();
}

let sink = 0;
let cursor = 0;
function nextKey(): string {
	cursor = (cursor + 7919) % KEY_COUNT;
	return keys[cursor];
}

function probe(context: { getRange: (options: any) => Iterable<any> }, options?: object): void {
	const key = nextKey();
	for (const entry of context.getRange({ start: key, end: key + '~', ...options })) {
		sink += entry.key.length;
	}
}

type Scenario = { name: string; run: (n: number) => void };
const scenarios: Scenario[] = [
	{
		name: 'point get (getBinaryFastSync)',
		run(n) {
			for (let i = 0; i < n; i++) {
				const result = db.getBinaryFastSync(nextKey());
				sink += typeof result === 'number' ? result : 1;
			}
		},
	},
	{
		name: 'db.getRange one-key probe',
		run(n) {
			for (let i = 0; i < n; i++) probe(db);
		},
	},
	{
		name: 'txn.getRange one-key probe',
		run(n) {
			db.transactionSync((txn) => {
				for (let i = 0; i < n; i++) probe(txn);
			});
		},
	},
	{
		name: 'db.getRange({ transaction }) one-key probe',
		run(n) {
			db.transactionSync((txn) => {
				const options = { transaction: txn };
				for (let i = 0; i < n; i++) probe(db, options);
			});
		},
	},
	{
		name: 'txn.getRange probe at a file range end',
		run(n) {
			const step = Math.ceil(KEY_COUNT / Math.max(FILES, 1));
			db.transactionSync((txn) => {
				for (let i = 0; i < n; i++) {
					const key = keys[(((i % Math.max(FILES, 1)) + 1) * step - 1) % KEY_COUNT];
					for (const entry of txn.getRange({ start: key, end: key + '~' })) {
						sink += entry.key.length;
					}
				}
			});
		},
	},
	{
		name: 'txn.getRange probe before 1,000 deleted keys',
		run(n) {
			db.transactionSync((txn) => {
				for (let i = 0; i < n; i++) {
					for (const entry of txn.getRange({ start: lastKey, end: lastKey + '~' })) {
						sink += entry.key.length;
					}
				}
			});
		},
	},
];

const scanRows = { name: 'txn.getRange full scan, per row', rowsPerRun: KEY_COUNT };
function scan(runs: number): void {
	db.transactionSync((txn) => {
		for (let i = 0; i < runs; i++) {
			for (const entry of txn.getRange({ start: keys[0], end: lastKey + '~' })) {
				sink += entry.key.length;
			}
		}
	});
}

// Harper's async request shape: one range per macrotask, so the pool is trimmed
// between ranges and nothing is reused. Includes the setImmediate itself.
async function probePerMacrotask(n: number): Promise<void> {
	const txn = new Transaction(db.store);
	for (let i = 0; i < n; i++) {
		probe(txn);
		await new Promise((resolve) => setImmediate(resolve));
	}
	txn.abort();
}

function median(samples: number[]): number {
	const sorted = [...samples].sort((a, b) => a - b);
	return sorted[Math.floor(sorted.length / 2)];
}

console.log(
	`node ${process.version}, ${KEY_COUNT} keys, ` +
		`${FILES > 0 ? `${FILES} SST files` : args.sst ? 'flushed to SST' : 'memtable only'}, ` +
		`${BATCHES} batches x ${PROBES} probes`
);
for (const scenario of scenarios) {
	scenario.run(PROBES); // warm up
	const samples: number[] = [];
	for (let b = 0; b < BATCHES; b++) {
		const start = process.hrtime.bigint();
		scenario.run(PROBES);
		samples.push(Number(process.hrtime.bigint() - start) / 1000 / PROBES);
	}
	console.log(`${scenario.name.padEnd(45)} ${median(samples).toFixed(2)} us/op`);
}

await probePerMacrotask(PROBES);
const macrotaskSamples: number[] = [];
for (let b = 0; b < BATCHES; b++) {
	const start = process.hrtime.bigint();
	await probePerMacrotask(PROBES);
	macrotaskSamples.push(Number(process.hrtime.bigint() - start) / 1000 / PROBES);
}
console.log(
	`${'txn.getRange probe, one per macrotask'.padEnd(45)} ${median(macrotaskSamples).toFixed(2)} us/op`
);

scan(5);
const scanSamples: number[] = [];
for (let b = 0; b < BATCHES; b++) {
	const runs = Math.max(1, Math.round(PROBES / 1000));
	const start = process.hrtime.bigint();
	scan(runs);
	scanSamples.push(Number(process.hrtime.bigint() - start) / 1000 / (runs * scanRows.rowsPerRun));
}
console.log(`${scanRows.name.padEnd(45)} ${median(scanSamples).toFixed(3)} us/row`);

db.close();
rmSync(dir, { recursive: true, force: true });
if (sink === -1) console.log(sink);
