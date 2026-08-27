import { RocksDatabase } from '../src/index.ts';
import { generateDBPath } from './lib/util.ts';
import { rmSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

/**
 * POC coverage for the per-database `'writeStall'` event (HarperFast/rocksdb-js).
 *
 * The event is emitted from RocksDB's `EventListener::OnStallConditionsChanged`, which fires when a
 * column family crosses between write-stall conditions (`normal`/`delayed`/`stopped`). We provoke a
 * per-CF stall the same way the `dbWriteBufferSize`-oversubscription pathology does, but at micro
 * scale so it fires in well under a second instead of the ~20-minute production arm:
 *
 *   - tiny per-CF memtables (`writeBufferSize` 64 KiB) that fill in a handful of writes,
 *   - a low queue depth (`maxWriteBufferNumber` 2) so a CF stalls after 2 unflushed memtables,
 *   - several column families sharing a tiny global budget (`dbWriteBufferSize` 256 KiB) so
 *     atomic-flush triggers fire constantly and immutable memtables pile up faster than the single
 *     flush pipeline drains them.
 *
 * A tight synchronous `putSync` burst outruns background flushes and trips the stall; the emit is
 * dispatched asynchronously, so we yield between chunks to let queued events arrive and stop as soon
 * as one is observed (bounding runtime regardless of machine speed).
 */
describe('writeStall event', () => {
	const CF_COUNT = 8;
	const dbPath = generateDBPath();
	const pathological = {
		writeBufferSize: 64 * 1024,
		maxWriteBufferNumber: 2,
		dbWriteBufferSize: 256 * 1024,
	};
	let dbs: RocksDatabase[] = [];

	beforeEach(() => {
		dbs = [];
		for (let i = 0; i < CF_COUNT; i++) {
			const db = new RocksDatabase(
				dbPath,
				i === 0 ? pathological : { ...pathological, name: `cf-${i}` }
			);
			db.open();
			dbs.push(db);
		}
	});

	afterEach(() => {
		for (let i = dbs.length - 1; i >= 0; i--) {
			dbs[i].close();
		}
		rmSync(dbPath, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
	});

	it('emits writeStall with column family and condition transition when a CF stalls', async () => {
		type StallEvent = { cfName: string; prev: string; cur: string };
		const events: StallEvent[] = [];
		dbs[0].addListener('writeStall', (cfName: string, prev: string, cur: string) => {
			events.push({ cfName, prev, cur });
		});

		const value = Buffer.alloc(4096, 0x61);
		const CHUNK = 500;
		const MAX_RECORDS = 40_000; // upper bound; we break as soon as a stall is seen
		let record = 0;
		while (record < MAX_RECORDS && events.length === 0) {
			for (let i = 0; i < CHUNK; i++, record++) {
				dbs[record % CF_COUNT].putSync(`k-${record.toString().padStart(8, '0')}`, value);
			}
			// Yield so the async-dispatched writeStall events can be delivered.
			await delay(0);
		}
		// Give any in-flight emits a moment to land after the final chunk.
		await delay(50);

		expect(events.length).toBeGreaterThan(0);

		const conditions = new Set(['normal', 'delayed', 'stopped']);
		for (const e of events) {
			expect(e.cfName).toBeTypeOf('string');
			expect(conditions.has(e.prev)).toBe(true);
			expect(conditions.has(e.cur)).toBe(true);
			expect(e.prev).not.toBe(e.cur); // only fires on a real transition
		}
		// At least one event must report throttling/blocking, not just a return to normal.
		expect(events.some((e) => e.cur === 'delayed' || e.cur === 'stopped')).toBe(true);
	}, 30_000);
});
