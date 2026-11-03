/**
 * Per-key cost of one batched read (`getManySync()`/`getMany()`, one RocksDB `MultiGet`) against
 * one read per key, by batch size, with the block cache warm or cold.
 *
 *   node benchmark/multi-get.mts <dir> [--api sync|async] [--cache hot|cold] [--keys 1000000]
 *     [--batches 1,4,16,64,256] [--reps 7] [--keys-per-sample 20000] [--encoding msgpack|binary]
 *
 * `--api sync` compares a `getSync()` loop with `getManySync()`; `--api async` compares
 * `Promise.all()` over `get()` with `getMany()`.
 *
 * Builds `<dir>` on first use (`--keys` records of ~100 bytes, compacted into one level). `hot`
 * gives the block cache room for the whole database and reads every key once before timing;
 * `cold` shrinks the block cache to 1 MiB so nearly every read misses it (the OS page cache stays
 * warm, so this measures RocksDB's read path rather than the device). Arms run interleaved in a
 * shuffled order within every repetition, so machine noise lands on both; each line reports the
 * median nanoseconds per key across `--reps`.
 */
import { RocksDatabase } from '../src/index.ts';
import { existsSync } from 'node:fs';
import { parseArgs } from 'node:util';

const { positionals, values: args } = parseArgs({
	allowPositionals: true,
	options: {
		cache: { type: 'string', default: 'hot' },
		keys: { type: 'string', default: '1000000' },
		batches: { type: 'string', default: '1,4,16,64,256' },
		reps: { type: 'string', default: '7' },
		'keys-per-sample': { type: 'string', default: '20000' },
		encoding: { type: 'string', default: 'msgpack' },
		api: { type: 'string', default: 'sync' },
	},
});
const [dir] = positionals;
if (!dir) {
	throw new Error('usage: multi-get.mts <dir> [options]');
}
const keyCount = Number(args.keys);
const batchSizes = args.batches.split(',').map(Number);
const reps = Number(args.reps);
const keysPerSample = Number(args['keys-per-sample']);
const cold = args.cache === 'cold';
const encoding = args.encoding === 'binary' ? 'binary' : 'msgpack';
const async = args.api === 'async';

const keyFor = (n: number) => `k${String(n).padStart(10, '0')}`;

// xorshift32: the same key sequence in every run.
let seed = 0x9e3779b9;
function random(): number {
	seed ^= seed << 13;
	seed ^= seed >>> 17;
	seed ^= seed << 5;
	return (seed >>> 0) / 4294967296;
}

if (!existsSync(dir)) {
	const db = RocksDatabase.open(dir);
	for (let i = 0; i < keyCount; i++) {
		db.putSync(keyFor(i), {
			id: i,
			sku: `sku-${i}`,
			name: `product ${i} with a moderately long name`,
			price: i * 0.01,
			stock: i % 97,
		});
	}
	db.flushSync();
	db.compactSync({ bottommost: true });
	db.close();
}

RocksDatabase.config({ blockCacheSize: cold ? 1024 * 1024 : 1024 * 1024 * 1024 });
const db = RocksDatabase.open(dir, { encoding });

if (!cold) {
	for (let i = 0; i < keyCount; i++) {
		db.getSync(keyFor(i));
	}
}

function batchFor(size: number): string[] {
	return Array.from({ length: size }, () => keyFor(Math.floor(random() * keyCount)));
}

let sink = 0;
const count = (values: unknown[]) => {
	for (const value of values) {
		if (value !== undefined) sink++;
	}
};
// `get` issues the batch's reads concurrently, as a caller with N independent keys would.
const arms: Record<string, (batches: string[][]) => void | Promise<void>> = async
	? {
			async get(batches) {
				for (const batch of batches) {
					count(await Promise.all(batch.map((key) => db.get(key))));
				}
			},
			async getMany(batches) {
				for (const batch of batches) {
					count(await db.getMany(batch));
				}
			},
		}
	: {
			getSync(batches) {
				for (const batch of batches) {
					for (const key of batch) {
						if (db.getSync(key) !== undefined) sink++;
					}
				}
			},
			getManySync(batches) {
				for (const batch of batches) {
					count(db.getManySync(batch));
				}
			},
		};
const [singleArm, manyArm] = Object.keys(arms);

const median = (values: number[]) => {
	const sorted = [...values].sort((a, b) => a - b);
	return sorted[Math.floor(sorted.length / 2)];
};

const samples = new Map<string, number[]>();
for (let rep = 0; rep < reps; rep++) {
	for (const size of batchSizes) {
		const batchCount = Math.max(1, Math.floor(keysPerSample / size));
		const order = random() < 0.5 ? [singleArm, manyArm] : [manyArm, singleArm];
		for (const arm of order) {
			// Fresh keys per arm, so neither arm reads blocks the other just pulled into the cache.
			const batches = Array.from({ length: batchCount }, () => batchFor(size));
			const start = process.hrtime.bigint();
			await arms[arm](batches);
			const elapsed = Number(process.hrtime.bigint() - start);
			const id = `${size}:${arm}`;
			const list = samples.get(id) ?? [];
			list.push(elapsed / (batchCount * size));
			samples.set(id, list);
		}
	}
}

console.log(
	`cache=${args.cache} api=${args.api} encoding=${encoding} keys=${keyCount} reps=${reps} sink=${sink}`
);
console.log(`| batch | ${singleArm} ns/key | ${manyArm} ns/key | change |`);
console.log('| ---: | ---: | ---: | ---: |');
for (const size of batchSizes) {
	const single = median(samples.get(`${size}:${singleArm}`)!);
	const many = median(samples.get(`${size}:${manyArm}`)!);
	const change = ((many - single) / single) * 100;
	console.log(
		`| ${size} | ${single.toFixed(0)} | ${many.toFixed(0)} | ${change >= 0 ? '+' : ''}${change.toFixed(0)}% |`
	);
}
db.close();
