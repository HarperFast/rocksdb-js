import { RocksDatabase } from '../src/index.ts';
import { dbRunner } from './lib/util.ts';
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

	// Plumbing: the option survives a real stalling-WBM configuration. It does NOT prove the flag
	// changes RocksDB's behavior — that needs a reliably-reachable stall to time the arms against,
	// which #755 removed, and whose failure mode would be a wedged libuv thread rather than a red
	// assertion. Both arms are expected to complete.
	it(
		'should accept allowWriteStall against a stalling WriteBufferManager (plumbing, not behavior)',
		() =>
			dbRunner({ dbOptions: [{}, { name: 'late' }] }, async (_, { db }) => {
				const value = 'y'.repeat(8192);
				for (let i = 0; i < 250; i++) {
					await db.put(`stall-${i.toString().padStart(6, '0')}`, value);
				}
				await db.flush({ allowWriteStall: true });
				expect(await db.get('stall-000249')).toBe(value);

				await db.put('stall-after', value);
				await db.flush();
				expect(await db.get('stall-after')).toBe(value);
			}),
		60_000
	);

	// Zeroing history is only safe because RocksDB's fallback is conservative: asked to validate a
	// sequence it no longer holds, it refuses the commit rather than passing it. A change that
	// turned that into a silent accept would be a lost update, and the stall test above would
	// still pass. The flush is what makes this the zero-history path rather than an ordinary
	// active-memtable conflict — it discards the sequence the check would otherwise have found.
	it('should still refuse a conflicting commit whose sequence was flushed out of history', () =>
		dbRunner({ dbOptions: [{}, { name: 'late' }] }, async (_, { db }) => {
			await db.put('conflict', 'initial');

			let committed = false;
			try {
				await db.transaction(async (txn) => {
					await txn.get('conflict');
					await db.put('conflict', 'concurrent');
					await db.flush();
					await txn.put('conflict', 'transactional');
				});
				committed = true;
			} catch (error) {
				// ERR_TRY_AGAIN, not ERR_BUSY: the flush is what puts this on the zero-history
				// path, so the code also confirms the scenario is the intended one.
				expect((error as { code?: string }).code).toBe('ERR_TRY_AGAIN');
			}
			expect(committed).toBe(false);
			expect(await db.get('conflict')).toBe('concurrent');
		}));
});
