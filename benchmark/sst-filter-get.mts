/**
 * Point-get cost against a multi-level database larger than the block cache, for comparing SST
 * filter settings (`bloomFilterBitsPerKey`).
 *
 *   node benchmark/sst-filter-get.mts build <dir> [--keys 8000000] [--bits 10]
 *   node benchmark/sst-filter-get.mts read <dir> [--mode present|absent|maybe] [--ops 1000000] [--warmup n]
 *     [--cache-mib 64] [--bits 10]
 *
 * `build` writes `--keys` even-numbered keys in random order with ~100-byte values, compacts them
 * into the last level, then adds three overlapping L0 files (below the L0 compaction trigger), so
 * a get probes every L0 file and then the last level. Odd-numbered keys are absent but inside every file's key range.
 * Filters are written into SSTs at build time, so build one database per filter setting.
 *
 * `read` reopens the database and times `--ops` random gets after `--warmup` (default `--ops`) gets:
 * `present`/`absent` use `getSync`; `maybe` uses `get` and reports how many returned a Promise
 * (the block-cache-only fast path missed). Run `read` with `--ops 0 --warmup <n>` too: subtracting
 * its `perf stat` counts from a full run leaves the per-get cost. Prints one JSON line.
 */
import { RocksDatabase } from '../src/index.ts';
import { randomBytes } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { parseArgs } from 'node:util';

const { positionals, values: args } = parseArgs({
	allowPositionals: true,
	options: {
		keys: { type: 'string', default: '8000000' },
		bits: { type: 'string', default: '10' },
		mode: { type: 'string', default: 'present' },
		ops: { type: 'string', default: '1000000' },
		warmup: { type: 'string' },
		'cache-mib': { type: 'string', default: '64' },
	},
});
const [command, dir] = positionals;
if (!dir || (command !== 'build' && command !== 'read')) {
	throw new Error('usage: sst-filter-get.mts build|read <dir> [options]');
}
const bloomFilterBitsPerKey = Number(args.bits);
const keyFor = (n: number) => `k${String(n).padStart(10, '0')}`;

// xorshift32: the same key sequence in every run, without Math.random's per-process seed.
let seed = 0x9e3779b9;
function random(): number {
	seed ^= seed << 13;
	seed ^= seed >>> 17;
	seed ^= seed << 5;
	return (seed >>> 0) / 4294967296;
}

function sleep(ms: number) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function levelFileCounts(db: RocksDatabase): number[] {
	const counts: number[] = [];
	for (let level = 0; level < 7; level++) {
		counts.push(Number(db.getDBProperty(`rocksdb.num-files-at-level${level}`)));
	}
	return counts;
}

function tableProperty(db: RocksDatabase, name: string): number {
	const properties = db.getDBProperty('rocksdb.aggregated-table-properties') ?? '';
	return Number(new RegExp(`${name}[^=;]*=(\\d+)`).exec(properties)?.[1] ?? NaN);
}

if (command === 'build') {
	const keyCount = Number(args.keys);
	const db = RocksDatabase.open(dir, { bloomFilterBitsPerKey, enableStats: true });
	const order = new Uint32Array(keyCount);
	for (let i = 0; i < keyCount; i++) order[i] = i;
	for (let i = keyCount - 1; i > 0; i--) {
		const j = Math.floor(random() * (i + 1));
		const t = order[i];
		order[i] = order[j];
		order[j] = t;
	}
	const cpuStart = process.cpuUsage();
	const start = performance.now();
	for (let i = 0; i < keyCount; i++) {
		db.putSync(keyFor(order[i] * 2), randomBytes(75).toString('base64'));
	}
	db.flushSync();
	while (
		db.getDBIntProperty('rocksdb.compaction-pending') ||
		db.getDBIntProperty('rocksdb.num-running-compactions')
	) {
		await sleep(200);
	}
	// Rewrites every file under this run's filter setting and leaves one level, so each build ends
	// with the same shape however compaction happened to interleave with the load.
	db.compactSync({ bottommost: true });
	const loadSeconds = (performance.now() - start) / 1000;
	for (let file = 0; file < 3; file++) {
		for (let i = 0; i < 20000; i++) {
			db.putSync(keyFor(Math.floor(random() * keyCount) * 2), randomBytes(75).toString('base64'));
		}
		db.flushSync();
	}
	const cpu = process.cpuUsage(cpuStart);
	writeFileSync(`${dir}.keys`, String(keyCount));
	console.log(
		JSON.stringify({
			command,
			bloomFilterBitsPerKey,
			filterEnv: process.env.ROCKSDB_JS_BENCH_FILTER ?? 'bloom',
			keyCount,
			loadSeconds,
			cpuSeconds: (cpu.user + cpu.system) / 1e6,
			levels: levelFileCounts(db),
			filterBlockBytes: tableProperty(db, 'filter block size'),
			indexBlockBytes: tableProperty(db, 'index block size'),
			dataBytes: tableProperty(db, 'data block size'),
		})
	);
	db.close();
} else {
	const ops = Number(args.ops);
	const mode = args.mode;
	RocksDatabase.config({ blockCacheSize: Number(args['cache-mib']) * 1024 * 1024 });
	const db = RocksDatabase.open(dir, { bloomFilterBitsPerKey, enableStats: true });
	const keyCount = Number(readFileSync(`${dir}.keys`, 'utf8'));
	const nextKey =
		mode === 'absent'
			? () => keyFor(Math.floor(random() * keyCount) * 2 + 1)
			: () => keyFor(Math.floor(random() * keyCount) * 2);
	let found = 0;
	let promises = 0;
	async function run(count: number) {
		for (let i = 0; i < count; i++) {
			if (mode === 'maybe') {
				let value = db.get(nextKey());
				if (value instanceof Promise) {
					promises++;
					value = await value;
				}
				if (value !== undefined) found++;
			} else if (db.getSync(nextKey()) !== undefined) {
				found++;
			}
		}
	}
	await run(Number(args.warmup ?? ops));
	found = 0;
	promises = 0;
	const stat = (name: string) => db.getStat(name) as number;
	const before = {
		useful: stat('rocksdb.bloom.filter.useful'),
		filterMiss: stat('rocksdb.block.cache.filter.miss'),
		indexMiss: stat('rocksdb.block.cache.index.miss'),
		dataMiss: stat('rocksdb.block.cache.data.miss'),
	};
	const cpuStart = process.cpuUsage();
	const start = performance.now();
	await run(ops);
	const elapsed = performance.now() - start;
	const cpu = process.cpuUsage(cpuStart);
	const perOp = (value: number) => (ops ? value / ops : 0);
	console.log(
		JSON.stringify({
			command,
			mode,
			ops,
			filterEnv: process.env.ROCKSDB_JS_BENCH_FILTER ?? 'bloom',
			placementEnv: process.env.ROCKSDB_JS_BENCH_PLACEMENT ?? 'default',
			usPerOp: perOp(elapsed * 1000),
			cpuUsPerOp: perOp(cpu.user + cpu.system),
			foundRatio: perOp(found),
			promiseRatio: perOp(promises),
			filterUsefulPerOp: perOp(stat('rocksdb.bloom.filter.useful') - before.useful),
			filterMissPerOp: perOp(stat('rocksdb.block.cache.filter.miss') - before.filterMiss),
			indexMissPerOp: perOp(stat('rocksdb.block.cache.index.miss') - before.indexMiss),
			dataMissPerOp: perOp(stat('rocksdb.block.cache.data.miss') - before.dataMiss),
			levels: levelFileCounts(db),
			tableReadersMem: db.getDBIntProperty('rocksdb.estimate-table-readers-mem'),
			blockCacheUsage: db.getDBIntProperty('rocksdb.block-cache-usage'),
			blockCachePinned: db.getDBIntProperty('rocksdb.block-cache-pinned-usage'),
		})
	);
	db.close();
}
