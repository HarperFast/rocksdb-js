import {
	RocksDatabase,
	shutdown,
	type RocksDatabaseOptions,
	type StatsDefault,
} from '../dist/index.mjs';
import { randomBytes } from 'node:crypto';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { bench, describe } from 'vitest';

const MIB = 1024 * 1024;
const RECORD_COUNT = 64 * 1024;
const VALUE_BYTES = 1024;
const VALUE_POOL = randomBytes(64 * MIB);
const COLUMN_FAMILY_COUNTS = [1, 4, 16] as const;
const DB_WRITE_BUFFER_SIZES = [32 * MIB, 256 * MIB, 0] as const;

type ArmResult = {
	columnFamilies: number;
	dbWriteBufferSize: string;
	flushCount: number;
	flushMedianMs: string;
	ingestMiBPerSecond: string;
	levelFiles: string;
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
			(total, db) => total + (db.getDBIntProperty(`rocksdb.num-files-at-level${level}`) ?? 0),
			0
		);
		if (files > 0) {
			levels.push(`L${level}:${files}`);
		}
	}
	return levels.join(' ') || 'none';
}

function measureArm(columnFamilies: number, dbWriteBufferSize: number): ArmResult {
	const dbPath = join(
		'benchmark',
		'data',
		`db-write-buffer-size-multicf-${columnFamilies}-${dbWriteBufferSize}-${randomBytes(8).toString('hex')}`
	);
	const options: RocksDatabaseOptions = {
		dbWriteBufferSize,
		enableStats: true,
		maxWriteBufferNumber: 16,
		writeBufferSize: 64 * MIB,
	};
	const databases: RocksDatabase[] = [];

	try {
		for (let index = 0; index < columnFamilies; index++) {
			databases.push(
				RocksDatabase.open(dbPath, index === 0 ? options : { ...options, name: `cf-${index}` })
			);
		}

		const start = performance.now();
		for (let record = 0; record < RECORD_COUNT; record++) {
			const offset = (record * 4099) % (VALUE_POOL.length - VALUE_BYTES);
			databases[record % columnFamilies].putSync(
				`key-${record.toString().padStart(8, '0')}`,
				VALUE_POOL.subarray(offset, offset + VALUE_BYTES)
			);
		}
		const elapsedMs = performance.now() - start;

		for (const db of databases) {
			db.flushSync();
		}

		const stats = databases[0].getStats() as StatsDefault;
		const flush = stats['rocksdb.db.flush.micros'];
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
			stallMs: ((stats['rocksdb.stall.micros'] ?? 0) / 1000).toFixed(2),
			sstMiB: formatMiB(sstBytes),
		};
	} finally {
		for (const db of databases.reverse()) {
			db.close();
		}
		shutdown();
		rmSync(dbPath, { force: true, recursive: true, maxRetries: 3 });
	}
}

describe('dbWriteBufferSize multi-column-family ingest', () => {
	bench(
		'round-robin ingest counters',
		() => {
			const results: ArmResult[] = [];
			for (const columnFamilies of COLUMN_FAMILY_COUNTS) {
				for (const dbWriteBufferSize of DB_WRITE_BUFFER_SIZES) {
					results.push(measureArm(columnFamilies, dbWriteBufferSize));
				}
			}
			console.table(results);
		},
		{ iterations: 1, time: 1, warmupIterations: 0, warmupTime: 0 }
	);
});
