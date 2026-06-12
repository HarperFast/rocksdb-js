import type { TransactionLogStats } from '../src/load-binding.js';
import { dbRunner } from '../test/lib/util.js';
import { stressTest } from './setup.js';
import { describe } from 'vitest';

/**
 * Memory-consumption stress test for the transaction log system.
 *
 * Writes several gigabytes into a single transaction log, keeping the default
 * 16MB max log file size so the data fans out across hundreds of files. Each
 * commit puts a single byte into the database itself — the gigabytes live in
 * the log entries, not the main column family.
 *
 * Once written, it snapshots `log.getStats()` (before), then runs a full
 * `log.query()` and iterates every entry, which maps every log file into
 * memory. A second `log.getStats()` (after) captures the resulting memory-map
 * footprint. The test asserts nothing — it only reports the before/after stats
 * so we can observe how close the load pushes a runner to its memory ceiling.
 */

const GiB = 1024 * 1024 * 1024;

// Total bytes to write into the transaction log. 5GB against the default 16MB
// max file size produces 300+ log files.
const TARGET_BYTES = 5 * GiB;

// Each log entry payload. Must be smaller than the 16MB max file size. 1MB
// keeps the number of commits manageable while still rotating files quickly.
const ENTRY_SIZE = 1024 * 1024;

// Batch several entries per transaction to amortize commit overhead.
const ENTRIES_PER_TXN = 8;

function gib(bytes: number): string {
	return `${(bytes / GiB).toFixed(2)} GiB`;
}

function reportStats(label: string, stats: TransactionLogStats): void {
	console.log(`\n=== Transaction log stats (${label}) ===`);
	console.log(stats);
}

describe('Stress Transaction Log Memory', () => {
	stressTest(
		`should write ${gib(TARGET_BYTES)} into the transaction log and map every file via query()`,
		{ mode: 'essential' },
		() =>
			dbRunner(async ({ db }) => {
				const log = db.useLog('memory');

				// A single reusable payload buffer — addEntry copies it into the log
				// synchronously, so reusing it across entries is safe and avoids
				// allocating gigabytes of throwaway buffers in the test itself.
				const payload = Buffer.alloc(ENTRY_SIZE, 0x61);

				// The database value itself is a single byte.
				const singleByte = Buffer.from([0x01]);

				const totalEntries = Math.ceil(TARGET_BYTES / ENTRY_SIZE);
				let written = 0;
				let key = 0;

				const writeStart = Date.now();
				while (written < totalEntries) {
					const batch = Math.min(ENTRIES_PER_TXN, totalEntries - written);
					await db.transaction((transaction) => {
						for (let i = 0; i < batch; i++) {
							db.putSync(`k${key++}`, singleByte, { transaction });
							log.addEntry(payload, transaction.id);
						}
					});
					written += batch;
				}
				const writeMs = Date.now() - writeStart;
				console.log(
					`\nWrote ${written} entries (${gib(written * ENTRY_SIZE)}) in ${writeMs}ms ` +
						`(${((written * ENTRY_SIZE) / GiB / (writeMs / 1000)).toFixed(2)} GiB/s)`
				);

				// Snapshot before mapping the files. RSS is the OS-reported resident
				// set of the whole process, so it includes any txnlog pages the query
				// below faults in (subject to kernel eviction).
				const before = log.getStats();
				reportStats('before query', before);
				const rssBefore = process.memoryUsage().rss;
				console.log(`\nRSS before query: ${rssBefore} (${gib(rssBefore)})`);

				// Iterate the entire log; this maps every file into memory. The query
				// is zero-copy — entry.data is a subarray view into the mmap and the
				// iterator itself only reads the ~17-byte entry header — so we must
				// touch the payload to actually fault its pages in. Read one byte per
				// 4KB (the smallest page size we run on; macOS arm64 uses 16KB) and
				// fold it into a checksum so the reads can't be optimized away.
				const queryStart = Date.now();
				let count = 0;
				let touchedBytes = 0;
				let checksum = 0;
				for (const { data } of log.query({ start: 0 })) {
					for (let i = 0; i < data.length; i += 4096) {
						checksum = (checksum + data[i]) | 0;
					}
					touchedBytes += data.length;
					if (count % 1000 === 0) {
						const rss = process.memoryUsage().rss;
						console.log(`\nRSS: ${rss} (${gib(rss)}), delta ${gib(rss - rssBefore)}`);
					}
					count++;
				}
				const queryMs = Date.now() - queryStart;
				console.log(
					`\nQueried and iterated ${count} entries (${gib(touchedBytes)} touched, ` +
						`checksum ${checksum}) in ${queryMs}ms`
				);

				// Snapshot after mapping the files.
				const after = log.getStats();
				reportStats('after query', after);
				const rssAfter = process.memoryUsage().rss;
				console.log(
					`\nRSS after query: ${rssAfter} (${gib(rssAfter)}), delta ${gib(rssAfter - rssBefore)}`
				);
			})
	);
});
