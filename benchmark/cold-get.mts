/**
 * Cold point-get microbenchmark: a database several times larger than the block
 * cache, read at random, with and without the verification table (VT).
 *
 *   node --expose-gc benchmark/cold-get.mts [--keys 300000] [--sample 20000] [--stats]
 *     [--measure <phase>]
 *
 * Phases, each over its own disjoint random sample of keys, one read in flight:
 *   - cold / txn-cold: `getBinary(key, { populateVersion: true })`, plain and in a
 *     transaction. Mostly block-cache misses, so mostly the thread-pool path.
 *   - fresh / txn-fresh: the same keys again with `expectedVersion`. A VT seeded
 *     by the cold pass answers FRESH synchronously without reading RocksDB.
 *   - txn-plain / plain: `getBinary(key)` with no VT options, in a transaction and not.
 *   - hot: `getBinary(key)` over keys whose blocks were just read into the block cache.
 *
 * `--stats` enables RocksDB statistics and prints per-get ticker deltas (DB
 * reads, block-cache data misses). Leave it off for timing: statistics cost
 * instructions of their own.
 *
 * `--measure <phase>` brackets that phase with `perf stat` control commands when
 * PERF_CTL_FIFO / PERF_ACK_FIFO are set, so instructions are counted for that
 * phase alone (every thread, including the libuv pool):
 *
 *   mkfifo ctl ack
 *   PERF_CTL_FIFO=ctl PERF_ACK_FIFO=ack perf stat -D -1 -e instructions \
 *     --control fifo:ctl,ack -- node benchmark/cold-get.mts --measure fresh
 *
 * Runs against `src` (native type stripping). Run it under `bench-run`.
 */
import { RocksDatabase } from '../src/index.ts';
import { FRESH_VERSION_FLAG } from '../src/load-binding.ts';
import { Transaction } from '../src/transaction.ts';
import { randomBytes } from 'node:crypto';
import { mkdtempSync, openSync, readSync, rmSync, writeSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { parseArgs } from 'node:util';

const { values: args } = parseArgs({
	options: {
		keys: { type: 'string', default: '300000' },
		sample: { type: 'string', default: '20000' },
		'value-size': { type: 'string', default: '200' },
		'cache-mb': { type: 'string', default: '1' },
		stats: { type: 'boolean', default: false },
		measure: { type: 'string' },
	},
});
const KEY_COUNT = Number(args.keys);
const SAMPLE = Number(args.sample);
const VALUE_SIZE = Number(args['value-size']);
const VERSION = 1.7e12;

let perfCtl: number | undefined;
let perfAck: number | undefined;
if (process.env.PERF_CTL_FIFO && process.env.PERF_ACK_FIFO) {
	perfCtl = openSync(process.env.PERF_CTL_FIFO, 'w');
	perfAck = openSync(process.env.PERF_ACK_FIFO, 'r');
}
function perfControl(command: 'enable' | 'disable'): void {
	if (perfCtl === undefined || perfAck === undefined) return;
	writeSync(perfCtl, `${command}\n`);
	readSync(perfAck, Buffer.alloc(5));
}

RocksDatabase.config({ blockCacheSize: Number(args['cache-mb']) * 1024 * 1024 });
const dir = mkdtempSync(join(tmpdir(), 'cold-get-'));
const db = RocksDatabase.open(join(dir, 'db'), {
	encoding: false,
	verificationTable: true,
	enableStats: args.stats,
});

const keyFor = (i: number) => Buffer.from(`k${String(i).padStart(8, '0')}`);
const payload = randomBytes(VALUE_SIZE);
for (let i = 0; i < KEY_COUNT; i++) {
	const value = Buffer.allocUnsafe(8 + VALUE_SIZE);
	value.writeDoubleBE(VERSION, 0);
	payload.copy(value, 8);
	value.writeUInt32BE(i, 8);
	db.putSync(keyFor(i), value);
}
db.flushSync();
// Rewrite rather than trivially move the flushed files into the last level, so RocksDB does not
// schedule a compaction to zero their sequence numbers after the first snapshot is released: its
// block reads count as block-cache misses and its CPU as this process's.
await db.compact({ bottommost: true });

const order = Array.from({ length: KEY_COUNT }, (_, i) => i);
for (let i = order.length - 1; i > 0; i--) {
	const j = Math.floor(Math.random() * (i + 1));
	[order[i], order[j]] = [order[j], order[i]];
}
let nextSample = 0;
function takeSample(): Buffer[] {
	const keys = order.slice(nextSample, nextSample + SAMPLE).map(keyFor);
	nextSample += SAMPLE;
	return keys;
}

const TICKERS = ['rocksdb.number.keys.read', 'rocksdb.block.cache.data.miss'] as const;
function readTickers(): number[] {
	return TICKERS.map((name) => (args.stats ? Number(db.getStat(name)) : 0));
}

async function phase(
	name: string,
	keys: Buffer[],
	read: (key: Buffer, txn?: Transaction) => any,
	transactional: boolean
): Promise<void> {
	globalThis.gc?.();
	let async = 0;
	let fresh = 0;
	const before = readTickers();
	const measured = args.measure === name;
	if (measured) perfControl('enable');
	const start = process.hrtime.bigint();
	// A transaction per 100 reads: long enough to amortize its setup, short enough
	// that its snapshot is not the oldest one for the whole run.
	for (let i = 0; i < keys.length; i += 100) {
		const txn = transactional ? new Transaction(db.store) : undefined;
		const end = Math.min(i + 100, keys.length);
		for (let k = i; k < end; k++) {
			let result = read(keys[k], txn);
			if (result instanceof Promise) {
				async++;
				result = await result;
			}
			if (result === FRESH_VERSION_FLAG) fresh++;
		}
		txn?.abort();
	}
	const elapsed = Number(process.hrtime.bigint() - start);
	if (measured) perfControl('disable');
	const after = readTickers();
	const perGet = (n: number) => (n / keys.length).toFixed(3);
	const line = [
		name.padEnd(10),
		`${(elapsed / keys.length / 1000).toFixed(2)} us/get`,
		`async ${perGet(async)}`,
		`fresh ${perGet(fresh)}`,
	];
	if (args.stats) {
		TICKERS.forEach((ticker, t) => {
			line.push(`${ticker.replace('rocksdb.', '')} ${perGet(after[t] - before[t])}`);
		});
	}
	console.log(line.join('  '));
}

const plainKeys = takeSample();
const txnKeys = takeSample();
const txnPlainKeys = takeSample();
const noOptionKeys = takeSample();
// Adjacent keys share data blocks, so a contiguous run this size fits in the block cache.
const hotKeys = Array.from({ length: Math.min(SAMPLE, 2000) }, (_, i) => keyFor(i));
await phase('cold', plainKeys, (key) => db.getBinary(key, { populateVersion: true }), false);
await phase('fresh', plainKeys, (key) => db.getBinary(key, { expectedVersion: VERSION }), false);
await phase(
	'txn-cold',
	txnKeys,
	(key, transaction) => db.getBinary(key, { transaction, populateVersion: true } as any),
	true
);
await phase(
	'txn-fresh',
	txnKeys,
	(key, transaction) => db.getBinary(key, { transaction, expectedVersion: VERSION } as any),
	true
);
await phase(
	'txn-plain',
	txnPlainKeys,
	(key, transaction) => db.getBinary(key, { transaction } as any),
	true
);
await phase('plain', noOptionKeys, (key) => db.getBinary(key), false);
for (const key of hotKeys) await db.getBinary(key);
await phase('hot', hotKeys, (key) => db.getBinary(key), false);

db.close();
rmSync(dir, { recursive: true, force: true });
