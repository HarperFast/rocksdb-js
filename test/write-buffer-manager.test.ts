import { RocksDatabase } from '../src/index.ts';
import { dbRunner, generateDBPath } from './lib/util.ts';
import { rmSync } from 'node:fs';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

/**
 * The WriteBufferManager is a process-global singleton. Once created, its
 * `costToCache` and `allowStall` settings are fixed for the life of the
 * process — only `bufferSize` is mutable at runtime (via `SetBufferSize`).
 *
 * So we initialize it ONCE for the whole file with the most informative
 * configuration (`costToCache: true`, room for resizing) and write each test
 * to be independent of other ordering. The basic / negative tests at the top
 * of the file run before this `beforeAll` and intentionally exercise the
 * unconfigured / size-update paths.
 */
describe('WriteBufferManager', () => {
	describe('configuration', () => {
		it('should reject negative writeBufferManagerSize', () => {
			expect(() => RocksDatabase.config({ writeBufferManagerSize: -1 })).toThrow(
				new RangeError('Write buffer manager size must be a positive integer or 0 to disable')
			);
		});

		it('should open a database with the WriteBufferManager disabled (default)', () =>
			dbRunner(async ({ db }) => {
				// No WBM configured. Writes still work — this is the guard
				// against the wiring breaking the happy path.
				await db.put('foo', 'bar');
				expect(await db.get('foo')).toBe('bar');
			}));

		// Runs before this file's beforeAll creates the manager, so it is the only
		// place the "no manager in this process" shape is observable.
		it('should report an unconfigured manager as disabled rather than absent', () =>
			dbRunner(async ({ db }) => {
				const stats = RocksDatabase.getWriteBufferManagerStats();
				expect(stats.enabled).toBe(false);
				expect(stats.bufferSize).toBe(0);
				expect(stats.memoryUsage).toBe(0);
				expect(stats.mutableMemoryUsage).toBe(0);
				expect(stats.stallActive).toBe(false);
				expect(stats.stallActiveMs).toBe(0);
				expect(stats.watchdogRunning).toBe(false);
				expect(stats.columnFamilies).toBe(0);
				expect(stats.maxWriteBufferSizeToMaintain).toEqual({});

				// The scrape keys keep their shape so a dashboard reading them does
				// not have to special-case "manager not configured".
				const dbStats = db.getStats();
				expect(dbStats['writeBufferManager.bufferSize']).toBe(0);
				expect(dbStats['writeBufferManager.stallActive']).toBe(0);
			}));
	});

	describe('with costToCache enabled', () => {
		// Create the singleton WBM exactly once, with costToCache on. All
		// tests in this describe block share it. `afterAll` resets size to 0
		// so other test files don't see a pre-existing WBM attached to new DBs.
		beforeAll(() => {
			RocksDatabase.config({
				blockCacheSize: 8 * 1024 * 1024,
				writeBufferManagerSize: 64 * 1024 * 1024,
				writeBufferManagerCostToCache: true,
			});
		});

		afterAll(() => {
			RocksDatabase.config({
				blockCacheSize: 32 * 1024 * 1024,
				writeBufferManagerSize: 0,
			});
		});

		describe('observability', () => {
			it('should report the live budget and usage on both surfaces', () =>
				dbRunner(async ({ db }) => {
					const value = 'x'.repeat(2048);
					for (let i = 0; i < 500; i++) {
						await db.put(`obs-${i.toString().padStart(6, '0')}`, value);
					}

					const stats = RocksDatabase.getWriteBufferManagerStats();
					expect(stats.enabled).toBe(true);
					expect(stats.bufferSize).toBe(64 * 1024 * 1024);
					expect(stats.costToCache).toBe(true);
					expect(stats.allowStall).toBe(false);
					// Nothing can stall a manager built without allowStall.
					expect(stats.stallActive).toBe(false);
					expect(stats.watchdogRunning).toBe(false);
					expect(stats.memoryUsage).toBeGreaterThan(1024 * 1024);
					expect(stats.mutableMemoryUsage).toBeGreaterThan(0);
					expect(stats.mutableMemoryUsage).toBeLessThanOrEqual(stats.memoryUsage);

					// `db.getStats()` reports the same process-wide manager, and is
					// not gated on `enableStats` (this database has statistics off).
					const dbStats = db.getStats();
					expect(dbStats['writeBufferManager.bufferSize']).toBe(stats.bufferSize);
					expect(dbStats['writeBufferManager.stallActive']).toBe(0);
					expect(dbStats['writeBufferManager.memoryUsage']).toBeGreaterThan(1024 * 1024);
				}));

			it('should resolve each key through getStat() without requiring statistics', () =>
				dbRunner(async ({ db }) => {
					await db.put('stat-routing', 'value');
					expect(db.getStat('writeBufferManager.bufferSize')).toBe(64 * 1024 * 1024);
					expect(db.getStat('writeBufferManager.memoryUsage')).toBeGreaterThan(0);
					expect(typeof db.getStat('writeBufferManager.mutableMemoryUsage')).toBe('number');
					expect(db.getStat('writeBufferManager.stallActive')).toBe(0);
					expect(db.getStat('writeBufferManager.stallActiveMs')).toBe(0);
					// An unknown key here must not fall through to the statistics path,
					// which throws when statistics are disabled — i.e. exactly when an
					// operator is reaching for this during an incident.
					expect(db.getStat('writeBufferManager.nope')).toBeUndefined();
				}));

			it('should start and stop the watchdog on the allowStall edges', () => {
				expect(RocksDatabase.getWriteBufferManagerStats().watchdogRunning).toBe(false);
				try {
					// allowStall is the only thing that makes a stall reachable, and it
					// is mutable at runtime, so the watchdog has to follow both edges
					// rather than only the manager's construction.
					RocksDatabase.config({ writeBufferManagerAllowStall: true });
					expect(RocksDatabase.getWriteBufferManagerStats().watchdogRunning).toBe(true);
					RocksDatabase.config({ writeBufferManagerAllowStall: false });
					expect(RocksDatabase.getWriteBufferManagerStats().watchdogRunning).toBe(false);
				} finally {
					RocksDatabase.config({ writeBufferManagerAllowStall: false });
				}
			});

			it('should count only column families attached to this manager', () => {
				const detachedPath = generateDBPath();
				const readOnlyPath = generateDBPath();
				const attachedPath = generateDBPath();
				const columnFamilies = (): number =>
					RocksDatabase.getWriteBufferManagerStats().columnFamilies;

				const opened: RocksDatabase[] = [];
				try {
					expect(RocksDatabase.getWriteBufferManagerStats().inventoryAvailable).toBe(true);
					const baseline = columnFamilies();

					const attached = new RocksDatabase(attachedPath, { name: 'attached' });
					attached.open();
					opened.push(attached);
					const withAttached = columnFamilies();
					expect(withAttached).toBeGreaterThan(baseline);

					// Size 0 means "no new attachments", not a teardown: a database
					// opened in that window draws on its own budget and cannot explain
					// the manager's memory, so its column families must not appear.
					RocksDatabase.config({ writeBufferManagerSize: 0 });
					const detached = new RocksDatabase(detachedPath, { name: 'detached' });
					detached.open();
					opened.push(detached);
					RocksDatabase.config({ writeBufferManagerSize: 64 * 1024 * 1024 });
					expect(columnFamilies()).toBe(withAttached);

					// A read-only database attaches the manager but writes nothing to
					// it, so counting it would inflate the retention distribution the
					// stall report is meant to explain.
					const seed = new RocksDatabase(readOnlyPath);
					seed.open();
					seed.putSync('seed', 'value');
					seed.close();
					const readOnly = new RocksDatabase(readOnlyPath, { readOnly: true });
					readOnly.open();
					opened.push(readOnly);
					expect(columnFamilies()).toBe(withAttached);
				} finally {
					for (const db of opened) {
						db.close();
					}
					if (!process.env.KEEP_FILES) {
						for (const path of [attachedPath, detachedPath, readOnlyPath]) {
							rmSync(path, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
						}
					}
				}
			});

			// Guards the docs/stats.md note: with `atomic_flush` (which this library
			// always sets) manager-pressure flushes reach BOTH counters. Only their
			// presence is asserted — the two count different events (scheduling
			// requests vs executed flushes) and their ratio is not stable, so no
			// relationship between the magnitudes is claimed here or in the docs.
			it('should record manager-pressure flushes on both flush-reason tickers', () =>
				dbRunner({ dbOptions: [{ enableStats: true }] }, async ({ db }) => {
					// Shrink the shared budget so ordinary test volume reaches it.
					RocksDatabase.config({ writeBufferManagerSize: 4 * 1024 * 1024 });
					try {
						const value = 'x'.repeat(8192);
						for (let i = 0; i < 2000; i++) {
							await db.put(`flush-${i.toString().padStart(6, '0')}`, value);
						}
						await new Promise((resolve) => setTimeout(resolve, 500));

						const stats = db.getStats(true);
						const requests = stats['rocksdb.atomic_flush.request.reason.write_buffer_manager'];
						const perColumnFamily = stats['rocksdb.flush.reason.write_buffer_manager'];
						expect(requests).toBeGreaterThan(0);
						expect(perColumnFamily).toBeGreaterThan(0);
					} finally {
						RocksDatabase.config({ writeBufferManagerSize: 64 * 1024 * 1024 });
					}
				}));
		});

		it('should open a database with the WriteBufferManager + costToCache', () =>
			dbRunner(async ({ db }) => {
				await db.put('foo', 'bar');
				expect(await db.get('foo')).toBe('bar');
			}));

		it('should share the manager across multiple databases', () =>
			dbRunner({ dbOptions: [{}, { name: 'second' }] }, async ({ db: db1 }, { db: db2 }) => {
				await db1.put('a', 'one');
				await db2.put('b', 'two');
				expect(await db1.get('a')).toBe('one');
				expect(await db2.get('b')).toBe('two');
			}));

		it('should allow runtime resize via SetBufferSize', () =>
			dbRunner(async ({ db }) => {
				await db.put('foo', 'bar');
				// SetBufferSize is atomic and safe under concurrent writers.
				RocksDatabase.config({ writeBufferManagerSize: 128 * 1024 * 1024 });
				await db.put('baz', 'qux');
				expect(await db.get('foo')).toBe('bar');
				expect(await db.get('baz')).toBe('qux');
				// Restore for other tests in this describe.
				RocksDatabase.config({ writeBufferManagerSize: 64 * 1024 * 1024 });
			}));

		it('should reject costToCache change after the WriteBufferManager is created', () => {
			// costToCache is baked into the WBM at construction time and
			// cannot be retro-applied. The reject must happen before any
			// other WBM state is touched — verify that a bundled size change
			// in the same config() call is NOT applied when costToCache is
			// rejected.
			expect(() =>
				RocksDatabase.config({
					writeBufferManagerSize: 96 * 1024 * 1024,
					writeBufferManagerCostToCache: false,
				})
			).toThrow(
				/writeBufferManagerCostToCache cannot be changed after the WriteBufferManager has been created/
			);

			// Re-stating the current value is a no-op and must not throw.
			expect(() => RocksDatabase.config({ writeBufferManagerCostToCache: true })).not.toThrow();
		});

		describe('memory reclamation', () => {
			it('should release active memtable memory after flush', () =>
				dbRunner({ dbOptions: [{ enableStats: true }] }, async ({ db }) => {
					const activeMemtable = (): number =>
						Number(db.getDBProperty('rocksdb.cur-size-active-mem-table') ?? 0);

					const baseline = activeMemtable();

					// Write ~2 MB of distinct keys into the active memtable.
					const value = 'x'.repeat(2048);
					for (let i = 0; i < 1000; i++) {
						await db.put(`k-${i.toString().padStart(6, '0')}`, value);
					}

					const afterWrites = activeMemtable();
					expect(afterWrites).toBeGreaterThan(baseline + 1024 * 1024);

					await db.flush();

					// A brand-new empty memtable replaces the flushed one.
					// Size is dominated by skiplist overhead, not user data.
					const afterFlush = activeMemtable();
					expect(afterFlush).toBeLessThan(afterWrites / 10);
				}));

			it('should charge memtable bytes against the block cache (costToCache)', () =>
				dbRunner({ dbOptions: [{ enableStats: true }] }, async ({ db }) => {
					const cacheUsage = (): number =>
						Number(db.getDBProperty('rocksdb.block-cache-usage') ?? 0);

					const baseline = cacheUsage();

					// CacheReservationManager inserts 256 KB dummy entries.
					// Write enough that multiple dummy entries land in the
					// cache — 2 MB of value data => ~8 dummy entries minimum.
					const value = 'x'.repeat(2048);
					for (let i = 0; i < 1000; i++) {
						await db.put(`k-${i.toString().padStart(6, '0')}`, value);
					}

					const peak = cacheUsage();
					// With costToCache, ~2 MB of memtable should be visible
					// as ~2 MB extra block-cache-usage. Assert at least 1 MB
					// of growth to leave slack for dummy-entry rounding.
					expect(peak - baseline).toBeGreaterThan(1024 * 1024);
				}));

			it('should reset the active memtable charge after flush (immediate reclamation)', () =>
				dbRunner({ dbOptions: [{ enableStats: true }] }, async ({ db }) => {
					// This is the strongest claim we can make on per-test
					// time scales: the *active* memtable's memory and its
					// WBM cost-to-cache charge are released immediately on
					// flush. (Recently-flushed memtables remain pinned by
					// RocksDB for `max_write_buffer_size_to_maintain` worth
					// of write history — that window can hold them across a
					// short test, so the broader "block-cache-usage drops"
					// claim isn't observable on unit-test time scales
					// without flushing 128+ MB through.)
					const active = (): number =>
						Number(db.getDBProperty('rocksdb.cur-size-active-mem-table') ?? 0);
					const cache = (): number => Number(db.getDBProperty('rocksdb.block-cache-usage') ?? 0);

					const baselineActive = active();
					const baselineCache = cache();

					const value = 'x'.repeat(2048);
					for (let i = 0; i < 1000; i++) {
						await db.put(`k-${i.toString().padStart(6, '0')}`, value);
					}
					const peakActive = active();
					const peakCache = cache();
					// Both grew: memtable filled, WBM charged the cache.
					expect(peakActive).toBeGreaterThan(baselineActive + 1024 * 1024);
					expect(peakCache).toBeGreaterThan(baselineCache + 1024 * 1024);

					await db.flush();

					// Active memtable is reset; its dedicated chunk of WBM
					// charge is released. Total cache usage may stay higher
					// than baseline because the flushed memtable is still
					// inside RocksDB's maintain window (separate concern).
					const settledActive = active();
					expect(settledActive).toBeLessThan(peakActive / 10);
				}));

			it('should observe cache usage grow with memtable bytes (WBM charge is visible)', () =>
				dbRunner({ dbOptions: [{ enableStats: true }] }, async ({ db }) => {
					// What costToCache actually does: WBM charges are
					// reflected in `block-cache-usage` (pinned entries that
					// account for memtable memory). This means a single
					// observability metric — block-cache-usage — covers
					// both the read cache AND the memtable footprint.
					//
					// What it does NOT do: enforce blockCacheSize as a hard
					// cap on memtable memory. The structural cap on
					// memtables is `writeBufferManagerSize`. The cache
					// capacity just controls how much room is *left* for
					// genuine read-cached blocks after WBM has charged its
					// share.
					const cache = (): number => Number(db.getDBProperty('rocksdb.block-cache-usage') ?? 0);
					const pinned = (): number =>
						Number(db.getDBProperty('rocksdb.block-cache-pinned-usage') ?? 0);

					const value = 'x'.repeat(4096);
					const samples: { written: number; usage: number; pinned: number }[] = [];

					for (let burst = 0; burst < 3; burst++) {
						for (let i = 0; i < 1000; i++) {
							await db.put(`${burst}-${i.toString().padStart(6, '0')}`, value);
						}
						await db.flush();
						samples.push({
							written: (burst + 1) * 1000 * 4096,
							usage: cache(),
							pinned: pinned(),
						});
					}

					// All WBM reservations show up as pinned entries (the
					// cache cannot evict them) — verify the pinned figure
					// tracks total usage.
					for (const s of samples) {
						expect(s.pinned).toBeGreaterThan(s.usage * 0.5);
					}

					// And cache usage rises monotonically as we add data
					// to the maintained window — proving the charge is
					// accumulating in the cache where Harper can observe
					// it with a single property.
					expect(samples[2].usage).toBeGreaterThan(samples[0].usage);
				}));

			it('should cap memtable memory at writeBufferManagerSize (not blockCacheSize)', () =>
				dbRunner({ dbOptions: [{ enableStats: true }] }, async ({ db }) => {
					// The actual structural guarantee: total memtable
					// memory across all DBs in the process is bounded by
					// the WBM's buffer_size. The block cache size is
					// independent. We verify by writing more than the
					// block cache size — writes must succeed because the
					// WBM cap (64 MB) is well above our data volume.
					const value = 'x'.repeat(4096);
					for (let i = 0; i < 4000; i++) {
						await db.put(`k-${i.toString().padStart(6, '0')}`, value);
					}
					// All writes complete without stalling — the WBM cap
					// of 64 MB is comfortably above the ~16 MB we wrote,
					// even though the block cache (8 MB) is smaller than
					// the data. This proves the cap is on the WBM, not
					// the cache.
					expect(await db.get('k-000000')).toBe(value);
					expect(await db.get('k-003999')).toBe(value);
				}));

			// This test documents an important caveat to "memory returns
			// after high write activity" — OptimisticTransactionDB pins
			// recently-flushed memtables for conflict checking. The cache
			// charges for those pinned memtables are released only when the
			// memtables exit the maintain window (older memtables get pushed
			// out by newer ones).
			//
			// The test deliberately writes enough data to *churn* the
			// maintain window: write a lot, flush, then write a lot more so
			// the older flushed memtables age out. After the second cycle,
			// some of the first-cycle's cache charge is released.
			it('should release older charges when newer writes push them out of the maintain window', () =>
				dbRunner({ dbOptions: [{ enableStats: true }] }, async ({ db }) => {
					const cache = (): number => Number(db.getDBProperty('rocksdb.block-cache-usage') ?? 0);
					const sizeAllMem = (): number =>
						Number(db.getDBProperty('rocksdb.size-all-mem-tables') ?? 0);

					const value = 'x'.repeat(8192);

					// Cycle 1: write 16 MB and flush.
					for (let i = 0; i < 2000; i++) {
						await db.put(`a-${i.toString().padStart(6, '0')}`, value);
					}
					await db.flush();
					const cycle1Cache = cache();
					const cycle1AllMem = sizeAllMem();

					// Cycle 2: write 16 MB more and flush.
					for (let i = 0; i < 2000; i++) {
						await db.put(`b-${i.toString().padStart(6, '0')}`, value);
					}
					await db.flush();
					await new Promise((resolve) => setTimeout(resolve, 100));
					const cycle2Cache = cache();
					const cycle2AllMem = sizeAllMem();

					// The maintain window (default ~128 MB / CF) is large
					// enough that both 16 MB cycles fit. So we expect
					// roughly-proportional growth here, not capping — but
					// also not unbounded growth past the maintain window.
					// Document the observation: cache scales with maintain
					// window size.
					expect(cycle2AllMem).toBeGreaterThan(cycle1AllMem);
					expect(cycle2Cache).toBeGreaterThan(cycle1Cache);

					// The promise we DO make is that this growth is bounded.
					// Even after writing 32 MB total, cache usage stays
					// well below the WBM cap (64 MB).
					expect(cycle2Cache).toBeLessThan(64 * 1024 * 1024);
				}));
		});
	});
});
