import { RocksDatabase } from '../src/index.js';
import { dbRunner } from './lib/util.js';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

/**
 * `allowStall` is fixed when the WriteBufferManager singleton is created and cannot be changed
 * afterwards, so this scenario needs a WBM nothing else in the process has built yet — hence its
 * own file (Vitest isolates each test file in its own worker/process).
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
				// ~8MB across several flushes: more than the 4MB budget, so pinned-as-history
				// memtables exhaust it and the stall never clears. A regression manifests as a
				// HANG, not a failed assertion — the stall blocks the calling thread, so no
				// in-test timer can fire; the test timeout is what surfaces it.
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

	// Zeroing history is only safe because RocksDB's fallback is conservative: with no history to
	// check against it reports "cannot determine" rather than passing the commit. A change that
	// turned that into a silent accept would be a lost update, and the stall test above would
	// still pass, so assert the conflict is refused here.
	it('should still refuse a conflicting commit with no retained history', () =>
		dbRunner({ dbOptions: [{}, { name: 'late' }] }, async (_, { db }) => {
			await db.put('conflict', 'initial');
			setTimeout(() => db?.put('conflict', 'concurrent'));

			let committed = false;
			try {
				await db.transaction(async (txn) => {
					await txn.get('conflict');
					await new Promise((resolve) => setTimeout(resolve, 100));
					await txn.put('conflict', 'transactional');
				});
				committed = true;
			} catch (error) {
				expect((error as { code?: string }).code).toMatch(/ERR_BUSY|ERR_TRY_AGAIN/);
			}
			expect(committed).toBe(false);
			expect(await db.get('conflict')).toBe('concurrent');
		}));
});
