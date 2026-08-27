import { RocksDatabase } from '../src/index.ts';
import { generateDBPath } from './lib/util.ts';
import { rmSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

/**
 * Coverage for the per-database `'writeStall'` event, emitted from RocksDB's
 * `EventListener::OnStallConditionsChanged` when a column family crosses between
 * write-stall conditions (`normal`/`delayed`/`stopped`).
 *
 * We provoke a real per-CF stall the same way the `dbWriteBufferSize`
 * -oversubscription pathology does, but at micro scale so it fires in ~1s instead
 * of the ~20-minute production arm: tiny per-CF memtables (`writeBufferSize`
 * 64 KiB) that fill in a few writes, a queue depth of 2 (`maxWriteBufferNumber`)
 * so a CF stalls after 2 unflushed memtables, and several column families sharing
 * a tiny global budget (`dbWriteBufferSize` 256 KiB) so atomic-flush triggers
 * fire constantly. A tight synchronous `putSync` burst outruns the flush
 * pipeline; the emit is dispatched asynchronously, so we yield between chunks to
 * receive events.
 *
 * The native emit is edge-triggered debounced per CF (falling-edge window default
 * 1000 ms via `ROCKSDB_JS_WRITE_STALL_DEBOUNCE_MS`): the rising edge into a stall
 * fires promptly, the recovery to normal is debounced, and a CF flipping
 * condition thousands of times/sec cannot flood the threadsafe-function queue.
 * The test relies on that default: it drives the stall for well under one window
 * (~500 ms), during which each CF transitions many times but only its rising edge
 * emits — so `count === 1 per CF` proves both the emit and the debounce (without
 * it this would be hundreds per CF), and every emit's `cur` is a stalling state
 * (`delayed`/`stopped`), never a bare `normal`.
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

	it('emits a debounced writeStall per column family with the condition transition', async () => {
		type StallEvent = { cfName: string; prev: string; cur: string };
		const events: StallEvent[] = [];
		dbs[0].addListener('writeStall', (cfName: string, prev: string, cur: string) => {
			events.push({ cfName, prev, cur });
		});

		const value = Buffer.alloc(4096, 0x61);
		// Drive the stall in small chunks, staying strictly inside one debounce
		// window (< 1000 ms) so the debounce permits at most one emit per CF. Small
		// chunks keep any single stalled putSync from overshooting the window.
		const WINDOW_MS = 500;
		const CHUNK = 50;
		const FAIL_DEADLINE_MS = 5000;
		const start = performance.now();
		let record = 0;
		for (;;) {
			for (let i = 0; i < CHUNK; i++, record++) {
				dbs[record % CF_COUNT].putSync(`k-${record.toString().padStart(8, '0')}`, value);
			}
			await delay(0); // yield so async-dispatched events are delivered
			const elapsed = performance.now() - start;
			if (events.length > 0 && elapsed >= WINDOW_MS) break; // stalled, still within one window
			if (elapsed >= FAIL_DEADLINE_MS) break; // fail-loud: no stall materialized
		}
		await delay(50); // let any in-flight emit from the final chunk land

		// A stall must have occurred (fail-loud rather than silently skipping).
		expect(events.length).toBeGreaterThan(0);

		// Debounce: within one <1000 ms window a CF transitions many times but is
		// permitted at most one emit. count===1 proves both the emit and the
		// debounce; without debouncing this would be hundreds per CF.
		const perCf = new Map<string, number>();
		for (const e of events) {
			perCf.set(e.cfName, (perCf.get(e.cfName) ?? 0) + 1);
		}
		for (const [cf, count] of perCf) {
			expect(count, `CF ${cf} emitted ${count}x within one debounce window`).toBe(1);
		}

		const conditions = new Set(['normal', 'delayed', 'stopped']);
		for (const e of events) {
			expect(e.cfName).toBeTypeOf('string');
			expect(conditions.has(e.prev)).toBe(true);
			expect(conditions.has(e.cur)).toBe(true);
			expect(e.prev).not.toBe(e.cur); // fires only on a real transition
			// Every emit here is a rising edge into a stall — never a bare recovery.
			expect(e.cur === 'delayed' || e.cur === 'stopped', `unexpected cur=${e.cur}`).toBe(true);
		}
	}, 30_000);
});
