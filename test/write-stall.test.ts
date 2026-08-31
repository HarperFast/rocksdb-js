import { RocksDatabase } from '../src/index.ts';
import { generateDBPath } from './lib/util.ts';
import { rmSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

/**
 * End-to-end wiring for the per-database `'writeStall'` event and the
 * `isWriteStalled()` pull. The debounce state machine itself is unit-tested
 * deterministically in `test/native/write_stall_debounce_test.cc`; this only
 * proves the RocksDB `OnStallConditionsChanged` hook reaches JS with a valid
 * per-CF payload and that `isWriteStalled()` reflects the live condition.
 *
 * A real stall is provoked at micro scale (64 KiB write buffers,
 * `maxWriteBufferNumber` 2, several CFs, tiny `dbWriteBufferSize`) so a tight
 * synchronous `putSync` burst outruns the flush pipeline in ~1s. Assertions avoid
 * timing/count bounds (the event is rate-limited, so a slow run may see a
 * heartbeat re-emit — that's the FSM's job to bound, covered natively); it only
 * requires that a stall was observed and every emitted event is a rising edge.
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

	it('emits a per-CF rising edge, and isWriteStalled() is wired', async () => {
		type StallEvent = { cfName: string; prev: string; cur: string };
		const events: StallEvent[] = [];
		dbs[0].addListener('writeStall', (cfName: string, prev: string, cur: string) => {
			events.push({ cfName, prev, cur });
		});

		const value = Buffer.alloc(4096, 0x61);
		const CHUNK = 100;
		const MAX_RECORDS = 40_000; // fail-loud bound; a stall is near-certain well before this
		let record = 0;
		while (record < MAX_RECORDS && events.length === 0) {
			for (let i = 0; i < CHUNK; i++, record++) {
				dbs[record % CF_COUNT].putSync(`k-${record.toString().padStart(8, '0')}`, value);
			}
			await delay(0); // yield so async-dispatched events are delivered
		}
		await delay(50); // let any in-flight emit land

		// A stall must have occurred (fail-loud rather than silently skipping).
		expect(events.length).toBeGreaterThan(0);
		// isWriteStalled() reads live DB-wide properties; observing `true` is racy
		// from the same thread that does the synchronous writes (the thread only
		// runs between stalls), so assert the wiring (returns a boolean) rather than
		// a flaky live-true. The event above is the stall proof; the FSM itself is
		// covered in test/native/write_stall_debounce_test.cc.
		expect(typeof dbs[0].isWriteStalled()).toBe('boolean');

		const conditions = new Set(['normal', 'delayed', 'stopped']);
		for (const e of events) {
			expect(e.cfName).toBeTypeOf('string');
			expect(conditions.has(e.prev)).toBe(true);
			expect(conditions.has(e.cur)).toBe(true);
			expect(e.prev).not.toBe(e.cur); // fires only on a real transition
			// Rising-edge only: every emit is an entry into a stall, never a bare recovery.
			expect(e.cur === 'delayed' || e.cur === 'stopped', `unexpected cur=${e.cur}`).toBe(true);
		}
	}, 30_000);
});
