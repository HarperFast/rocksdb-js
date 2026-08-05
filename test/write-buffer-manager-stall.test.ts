import { RocksDatabase } from '../src/index.js';
import { dbRunner } from './lib/util.js';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

/**
 * `allowStall` is fixed when the WriteBufferManager singleton is created and cannot be changed
 * afterwards, so this scenario needs a WBM nothing else in the process has built yet — hence its
 * own file (Vitest isolates each test file in its own worker/process).
 *
 * The regression: retained memtable history is only trimmable back DOWN TO
 * `max_write_buffer_size_to_maintain`, and it is charged to the WriteBufferManager. A derived
 * target far above the manager's budget therefore fills the budget with memory that is never
 * released, and a stalling manager stalls every subsequent write forever — not until a flush
 * catches up. It surfaced as a Harper replication receiver wedging partway through a base copy on
 * a deployment with a small block cache (the WBM is sized from it): the writes neither landed nor
 * failed.
 */
describe('WriteBufferManager stall', () => {
	beforeAll(() => {
		// Budget deliberately far below the derived history target
		// (`maxWriteBufferNumber` 16 * `writeBufferSize` 16MB = 256MB).
		RocksDatabase.config({
			blockCacheSize: 8 * 1024 * 1024,
			writeBufferManagerSize: 4 * 1024 * 1024,
			writeBufferManagerAllowStall: true,
		});
	});

	// Leave no manager attached to databases opened by later files (same reason
	// write-buffer-manager.test.ts resets); `allowStall` itself is not resettable.
	afterAll(() => {
		RocksDatabase.config({
			blockCacheSize: 32 * 1024 * 1024,
			writeBufferManagerSize: 0,
		});
	});

	it(
		'should not stall writes forever on a late-created column family',
		() =>
			dbRunner({ dbOptions: [{}, { name: 'late' }] }, async (_, { db }) => {
				const value = 'x'.repeat(8192);
				// ~8MB written across several flushes: comfortably more than the 4MB budget, so if
				// flushed memtables are pinned as history rather than released, the budget is
				// exhausted and the stall never clears.
				//
				// A regression shows up as a HANG, not as a failed assertion — the manager stalls
				// inside RocksDB's write path, which blocks the calling thread, so no in-test timer
				// can fire to turn it into a clean failure. The test timeout below is what surfaces
				// it. (Measured: ~45ms when history is released, unbounded when it is not.)
				for (let cycle = 0; cycle < 4; cycle++) {
					for (let i = 0; i < 250; i++) {
						await db.put(`k-${cycle}-${i.toString().padStart(6, '0')}`, value);
					}
					await db.flush();
				}
				expect(await db.get('k-3-000249')).toBe(value);
			}),
		60_000
	);
});
