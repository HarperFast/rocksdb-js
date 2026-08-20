import { type CompressionAlgorithm, supportedCompression } from '../dist/index.mjs';
import { benchmark } from './setup.ts';
import { describe } from 'vitest';

/**
 * RocksDB-only benchmark comparing the write/read throughput of each
 * compression algorithm compiled into the native build. The set of algorithms
 * varies by build (see `supportedCompression`), so the benchmark iterates over
 * whatever is available — e.g. a minimal build may only expose `none` and
 * `zlib`, while a full build adds `snappy`, `lz4`, `zstd`, etc.
 *
 * Pair the timings here with the on-disk size assertions in
 * `test/compression.test.ts` to reason about the space/speed tradeoff.
 */

const RECORD_COUNT = 5000;

type Record = { key: number; value: unknown };

// Compressible, realistic-ish JSON records: repetitive text compresses well,
// which is what makes the algorithm differences observable. Values exceed the
// 2 KB blob threshold so blob-file compression is exercised alongside SST
// block compression.
function makeRecords(count: number): Record[] {
	const filler = 'the quick brown fox jumps over the lazy dog '.repeat(48);
	const records: Record[] = [];
	for (let i = 0; i < count; i++) {
		records.push({
			key: i,
			value: {
				id: i,
				name: `record-${i}`,
				note: filler,
				tags: ['alpha', 'beta', 'gamma', 'delta'],
			},
		});
	}
	return records;
}

describe(`compression - write ${RECORD_COUNT} compressible records`, () => {
	for (const algorithm of supportedCompression as readonly CompressionAlgorithm[]) {
		benchmark('rocksdb', {
			name: algorithm,
			dbOptions: { compression: algorithm },
			setup(ctx) {
				ctx.records = makeRecords(RECORD_COUNT);
			},
			bench({ db, records }) {
				for (const r of records as Record[]) {
					db.putSync(r.key, r.value);
				}
				db.flushSync();
			},
		});
	}
});

describe(`compression - read ${RECORD_COUNT} compressible records`, () => {
	for (const algorithm of supportedCompression as readonly CompressionAlgorithm[]) {
		benchmark('rocksdb', {
			name: algorithm,
			dbOptions: { compression: algorithm },
			setup(ctx) {
				const records = makeRecords(RECORD_COUNT);
				for (const r of records) {
					ctx.db.putSync(r.key, r.value);
				}
				ctx.db.flushSync();
				ctx.db.compactSync();
				ctx.keys = records.map((r) => r.key);
			},
			bench({ db, keys }) {
				for (const k of keys as number[]) {
					db.getSync(k);
				}
			},
		});
	}
});
