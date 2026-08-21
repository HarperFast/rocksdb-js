import {
	RocksDatabase,
	shutdown,
	type RocksDatabaseOptions,
	type StatsCurated,
	type StatsDefault,
} from '../dist/index.mjs';
import { randomBytes } from 'node:crypto';
import { mkdirSync, rmSync, statfsSync } from 'node:fs';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { bench, describe } from 'vitest';

function parseInteger(name: string, raw: string, minimum: number): number {
	const value = Number(raw);
	if (!Number.isInteger(value) || value < minimum) {
		throw new Error(`${name} must be an integer >= ${minimum}; received ${raw}`);
	}
	return value;
}

function environmentInteger(name: string, fallback: number, minimum: number = 1): number {
	const raw = process.env[name];
	return raw === undefined ? fallback : parseInteger(name, raw, minimum);
}

function environmentIntegerList(name: string, fallback: number[]): number[] {
	const raw = process.env[name];
	return raw === undefined
		? fallback
		: raw.split(',').map((value, index) => parseInteger(`${name}[${index}]`, value, 1));
}

const MIB = 1024 * 1024;
const TMPFS_MAGIC = 0x01021994;
const RECORD_COUNT = environmentInteger('BENCH_RECORDS', 256 * 1024);
const VALUE_BYTES = 1024;
const VALUE_POOL_BYTES = 64 * MIB;
const COLUMN_FAMILY_COUNTS = environmentIntegerList('BENCH_CFS', [1, 4, 8, 16]);
const DB_WRITE_BUFFER_SIZES = [32 * MIB, 256 * MIB, 0] as const;
const WRITE_BUFFER_SIZE = environmentInteger('BENCH_WRITE_BUFFER', 16 * MIB);
const MAX_WRITE_BUFFER_SIZE_TO_MAINTAIN = environmentInteger(
	'BENCH_MAX_WRITE_BUFFER_SIZE_TO_MAINTAIN',
	-1,
	-1
);
const BENCH_DATA_DIR = process.env.BENCH_DATA_DIR ?? join('benchmark', 'data');
const SETTLE_TIMEOUT_MS = environmentInteger('BENCH_SETTLE_TIMEOUT_MS', 5 * 60_000);
let valuePool: Buffer | undefined;
let sweepRuns = 0;

type ArmResult = {
	columnFamilies: number;
	dbWriteBufferSize: string;
	flushCount: number;
	flushMedianMs: string;
	ingestMiBPerSecond: string;
	levelFiles: string;
	memtableHistory: string;
	stallMs: string;
	sstMiB: string;
};

function formatMiB(bytes: number): string {
	return (bytes / MIB).toFixed(1);
}

function formatDbWriteBufferSize(size: number): string {
	return size === 0 ? 'disabled' : `${size / MIB} MiB`;
}

function levelFiles(databases: RocksDatabase[]): string {
	const levels: string[] = [];
	for (let level = 0; level < 7; level++) {
		const files = databases.reduce(
			(total, db) => total + Number(db.getDBProperty(`rocksdb.num-files-at-level${level}`) ?? 0),
			0
		);
		if (files > 0) {
			levels.push(`L${level}:${files}`);
		}
	}
	return levels.join(' ') || 'none';
}

function scatteredKey(record: number): string {
	const hash = Math.imul(record, 2654435761) >>> 0;
	return `key-${hash.toString().padStart(10, '0')}-${record.toString().padStart(8, '0')}`;
}

function getValuePool(): Buffer {
	return (valuePool ??= randomBytes(VALUE_POOL_BYTES));
}

function ensureDurableStorage(): void {
	mkdirSync(BENCH_DATA_DIR, { recursive: true });
	if (Number(statfsSync(BENCH_DATA_DIR).type) === TMPFS_MAGIC) {
		throw new Error(
			`BENCH_DATA_DIR must use durable storage; ${BENCH_DATA_DIR} is on a tmpfs filesystem`
		);
	}
}

function flushHistogram(database: RocksDatabase): StatsCurated['rocksdb.db.flush.micros'] {
	const flush = (database.getStats() as StatsCurated)['rocksdb.db.flush.micros'];
	if (!flush) {
		throw new Error('rocksdb.db.flush.micros is unavailable; enableStats did not take effect');
	}
	return flush;
}

async function waitForBackgroundWork(databases: RocksDatabase[]): Promise<void> {
	const deadline = performance.now() + SETTLE_TIMEOUT_MS;
	let idleChecks = 0;
	while (idleChecks < 3) {
		const idle = databases.every((database) => {
			const stats = database.getStats() as StatsDefault;
			return (
				stats['rocksdb.mem-table-flush-pending'] === 0 &&
				stats['rocksdb.compaction-pending'] === 0 &&
				stats['rocksdb.num-running-compactions'] === 0 &&
				stats['rocksdb.num-running-flushes'] === 0
			);
		});
		if (idle) {
			idleChecks++;
		} else {
			idleChecks = 0;
		}
		if (performance.now() >= deadline) {
			throw new Error(`RocksDB background work did not settle within ${SETTLE_TIMEOUT_MS} ms`);
		}
		await delay(100);
	}
}

function cleanupArm(databases: RocksDatabase[], dbPath: string, armError: unknown): void {
	const errors: unknown[] = armError === undefined ? [] : [armError];
	for (let index = databases.length - 1; index >= 0; index--) {
		try {
			databases[index].close();
		} catch (error) {
			errors.push(error);
		}
	}
	try {
		shutdown();
	} catch (error) {
		errors.push(error);
	}
	try {
		rmSync(dbPath, { force: true, recursive: true, maxRetries: 3 });
	} catch (error) {
		errors.push(error);
	}
	if (errors.length > (armError === undefined ? 0 : 1)) {
		throw new AggregateError(errors, `Failed to clean up benchmark arm at ${dbPath}`);
	}
}

async function measureArm(columnFamilies: number, dbWriteBufferSize: number): Promise<ArmResult> {
	ensureDurableStorage();
	const dbPath = join(
		BENCH_DATA_DIR,
		`db-write-buffer-size-multicf-${columnFamilies}-${dbWriteBufferSize}-${randomBytes(8).toString('hex')}`
	);
	const options: RocksDatabaseOptions = {
		dbWriteBufferSize,
		enableStats: true,
		maxWriteBufferNumber: 16,
		maxWriteBufferSizeToMaintain: MAX_WRITE_BUFFER_SIZE_TO_MAINTAIN,
		writeBufferSize: WRITE_BUFFER_SIZE,
	};
	const databases: RocksDatabase[] = [];
	let armError: unknown;

	try {
		for (let index = 0; index < columnFamilies; index++) {
			databases.push(
				RocksDatabase.open(dbPath, index === 0 ? options : { ...options, name: `cf-${index}` })
			);
		}

		const currentValuePool = getValuePool();
		const start = performance.now();
		for (let record = 0; record < RECORD_COUNT; record++) {
			const offset = (record * 4099) % (currentValuePool.length - VALUE_BYTES);
			databases[record % columnFamilies].putSync(
				scatteredKey(record),
				currentValuePool.subarray(offset, offset + VALUE_BYTES)
			);
		}
		const elapsedMs = performance.now() - start;

		databases[0].flushSync();
		await waitForBackgroundWork(databases);

		const stats = databases[0].getStats() as StatsCurated;
		const flush = flushHistogram(databases[0]);
		if (flush.count === 0) {
			throw new Error('The completed arm did not record any memtable flushes');
		}
		const sstBytes = databases.reduce(
			(total, db) => total + (db.getDBIntProperty('rocksdb.total-sst-files-size') ?? 0),
			0
		);
		return {
			columnFamilies,
			dbWriteBufferSize: formatDbWriteBufferSize(dbWriteBufferSize),
			flushCount: flush.count,
			flushMedianMs: (flush.median / 1000).toFixed(2),
			ingestMiBPerSecond: formatMiB((RECORD_COUNT * VALUE_BYTES * 1000) / elapsedMs),
			levelFiles: levelFiles(databases),
			memtableHistory:
				MAX_WRITE_BUFFER_SIZE_TO_MAINTAIN === -1
					? 'derived'
					: formatDbWriteBufferSize(MAX_WRITE_BUFFER_SIZE_TO_MAINTAIN),
			stallMs: ((stats['rocksdb.stall.micros'] ?? 0) / 1000).toFixed(2),
			sstMiB: formatMiB(sstBytes),
		};
	} catch (error) {
		armError = error;
		throw error;
	} finally {
		cleanupArm(databases, dbPath, armError);
	}
}

describe.skipIf(process.env.BENCHMARK_MODE === 'essential' || !!process.env.LMDB_ONLY)(
	'dbWriteBufferSize multi-column-family ingest',
	() => {
		bench(
			'round-robin ingest counters',
			async () => {
				if (++sweepRuns !== 1) {
					throw new Error('The multi-column-family counter sweep must execute exactly once');
				}
				const results: ArmResult[] = [];
				try {
					for (const columnFamilies of COLUMN_FAMILY_COUNTS) {
						for (const dbWriteBufferSize of DB_WRITE_BUFFER_SIZES) {
							results.push(await measureArm(columnFamilies, dbWriteBufferSize));
						}
					}
				} finally {
					console.table(results);
				}
			},
			{ iterations: 1, throws: true, time: 0, warmupIterations: 0, warmupTime: 0 }
		);
	}
);
