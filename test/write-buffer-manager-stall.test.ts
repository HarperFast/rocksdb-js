import { RocksDatabase } from '../src/index.ts';
import { dbRunner, generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { mkdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

const reopenFixturePath = join(__dirname, 'fixtures', 'fork-wbm-reopen-stall.mts');

// The manager is a native process-global and Vitest's `threads` pool runs every file in one
// process, so a stalling manager left behind here follows later files into their own databases —
// and under one, `resolveMaxWriteBufferSizeToMaintain` clamps their retained-history window.
// `allowStall` is resettable (`config()` propagates it through `SetAllowStall`), so reset it here
// rather than leaving the next file's behavior to depend on which file ran first.
afterAll(() => {
	RocksDatabase.config({ writeBufferManagerSize: 0, writeBufferManagerAllowStall: false });
});

/**
 * The WriteBufferManager is a process-wide singleton built on first use, and its size and
 * `costToCache` are fixed from then on, so this scenario needs a WBM nothing else in the process has
 * built yet — hence its own file (Vitest isolates each test file in its own worker/process).
 * `allowStall` itself is not in that set: `config()` propagates it to a live manager through
 * `SetAllowStall` (see db_settings.cpp), which is why enabling it late is only a warning and not a
 * safeguard (#821).
 */
describe('WriteBufferManager stall', () => {
	beforeAll(() => {
		// Budget deliberately far below the derived history target
		// (`maxWriteBufferNumber` 16 * `writeBufferSize` 16MB = 256MB).
		//
		// `costToCache` matches write-buffer-manager.test.ts because the manager is a native
		// process-global and Vitest's `threads` pool runs every file in one process, so whichever of
		// the two files runs first creates the singleton for both — and `costToCache` is the one
		// setting `config()` refuses to change afterwards. Disagreeing here makes the other file
		// throw in its `beforeAll` whenever the file order puts this one first.
		RocksDatabase.config({
			blockCacheSize: 8 * 1024 * 1024,
			writeBufferManagerSize: 4 * 1024 * 1024,
			writeBufferManagerAllowStall: true,
			writeBufferManagerCostToCache: true,
		});
	});

	// Leave no manager attached to databases opened by later files (same reason
	// write-buffer-manager.test.ts resets); the file-level `afterAll` above clears `allowStall`.
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
	// and its failure mode would be a wedged libuv thread rather than a red assertion. Both arms
	// are expected to complete.
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

	// A near-zero history window is only safe because RocksDB's fallback is conservative: asked to
	// validate a sequence it no longer holds, it refuses the commit rather than passing it. A change
	// that turned that into a silent accept would be a lost update, and the stall test above would
	// still pass. The flush plus the two writes after it are what put the transaction on that path —
	// they discard the sequence the check would otherwise have found. Both writes are needed: a write
	// marks its family for a history trim only after its own insert, and the trim is drained by the
	// next write's preprocessing.
	it('should still refuse a conflicting commit whose sequence was flushed out of history', () =>
		dbRunner({ dbOptions: [{}, { name: 'late' }] }, async (_, { db }) => {
			await db.put('conflict', 'initial');

			let committed = false;
			try {
				await db.transaction(async (txn) => {
					await txn.get('conflict');
					await db.put('conflict', 'concurrent');
					await db.flush();
					await db.put('unrelated-1', 'x');
					await db.put('unrelated-2', 'x');
					await txn.put('conflict', 'transactional');
				});
				committed = true;
			} catch (error) {
				// ERR_TRY_AGAIN, not ERR_BUSY: the retained memtable is gone, so the check reports
				// "cannot determine" rather than finding the conflict — which is what makes this the
				// minimal-history path and not an ordinary conflict.
				expect((error as { code?: string }).code).toBe('ERR_TRY_AGAIN');
			}
			expect(committed).toBe(false);
			expect(await db.get('conflict')).toBe('concurrent');
		}));
});

/**
 * Runs the reopen fixture and resolves with what it printed. The deadline is enforced by killing
 * the child, because a regression here is a hang: `put()` runs `store.putSync()` before returning
 * its promise, so a stalled write blocks the JS thread and neither an in-test timer nor Vitest's own
 * timeout can fire (#781).
 */
function runFixture(
	fixturePath: string,
	fixtureArgs: string[]
): Promise<{ code: number | null; signal: NodeJS.Signals | null; stdout: string; stderr: string }> {
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, [fixturePath, ...fixtureArgs]);
		let stdout = '';
		let stderr = '';
		child.stdout?.on('data', (chunk) => {
			stdout += chunk.toString();
		});
		child.stderr?.on('data', (chunk) => {
			stderr += chunk.toString();
		});
		// Generous, because it also has to cover node boot, addon load and 160MB of writes on a busy
		// CI machine — and because the property that matters is that a regression fails *bounded*
		// rather than hanging the job, not that it fails quickly.
		const timer = setTimeout(() => child.kill('SIGKILL'), 60_000);
		child.on('close', (code, signal) => {
			clearTimeout(timer);
			resolve({ code, signal, stdout, stderr });
		});
		child.on('error', (error) => {
			clearTimeout(timer);
			reject(error);
		});
	});
}

function runReopenFixture(
	dbPath: string,
	phase: 'create' | 'reopen',
	mode: 'optimistic' | 'pessimistic',
	stall: 'stall' | 'nostall' = 'stall',
	maintain?: number
) {
	const args = [dbPath, phase, mode, stall];
	if (maintain !== undefined) args.push(String(maintain));
	return runFixture(reopenFixturePath, args);
}

function maintainValues(stdout: string): number[] {
	const line = /^MAINTAIN (.*)$/m.exec(stdout);
	if (!line) throw new Error(`fixture printed no MAINTAIN line; stdout was:\n${stdout}`);
	return Object.values(JSON.parse(line[1]) as Record<string, number>);
}

/**
 * #821: the safeguard is applied when a column family is created, and RocksDB's transaction wrappers
 * undo it at every open — `OptimisticTransactionDB::Open` and `TransactionDB::PrepareWrap` both
 * rewrite a `0` target back to `-1`, which sanitizes to `maxWriteBufferNumber * writeBufferSize`.
 * Only families created *after* an open kept the `0`, so the fresh-database cases above passed while
 * production wedged on the first restart. These reopen instead.
 */
describe('WriteBufferManager stall — reopen (#821)', () => {
	const dbPaths: string[] = [];

	afterAll(() => {
		if (process.env.KEEP_FILES) return;
		for (const dbPath of dbPaths) {
			rmSync(dbPath, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
		}
	});

	function freshPath(): string {
		const dbPath = generateDBPath();
		mkdirSync(dbPath, { recursive: true });
		dbPaths.push(dbPath);
		return dbPath;
	}

	for (const mode of ['optimistic', 'pessimistic'] as const) {
		// Both modes need their own case: they go through different RocksDB wrappers, and each
		// carries its own copy of the rewrite.
		it(`keeps the safeguard across a reopen and does not stall (${mode})`, async () => {
			const dbPath = freshPath();
			const created = await runReopenFixture(dbPath, 'create', mode);
			expect(created.code, created.stderr).toBe(0);

			const reopened = await runReopenFixture(dbPath, 'reopen', mode);
			// Before the fix this is a SIGKILL with no `WROTE`: every family reopened at the
			// derived 256MB of retained history against a 128MB budget, and putSync never
			// returned.
			expect(reopened.signal, reopened.stderr).toBeNull();
			expect(reopened.stdout, reopened.stderr).toContain('WROTE');
			expect(reopened.code, reopened.stderr).toBe(0);
			expect(maintainValues(reopened.stdout)).toEqual([1, 1, 1, 1]);
		}, 150_000);
	}

	// The clamp keys off a WriteBufferManager being attached, not off it being a stalling one:
	// `allowStall` is mutable at runtime while this target is fixed when a family is created, and a
	// non-stalling manager still never reclaims the history it is charged for.
	it('clamps under a non-stalling manager too', async () => {
		const dbPath = freshPath();
		const created = await runReopenFixture(dbPath, 'create', 'optimistic', 'nostall');
		expect(created.code, created.stderr).toBe(0);

		const reopened = await runReopenFixture(dbPath, 'reopen', 'optimistic', 'nostall');
		expect(reopened.signal, reopened.stderr).toBeNull();
		expect(reopened.stdout, reopened.stderr).toContain('WROTE');
		expect(reopened.code, reopened.stderr).toBe(0);
		expect(maintainValues(reopened.stdout)).toEqual([1, 1, 1, 1]);
	}, 150_000);

	// An explicit 0 reads as "retain no history", and the wrappers turn exactly that value into the
	// largest history there is, so it cannot be passed through either.
	it('normalizes an explicitly requested 0 rather than passing it to the wrappers', async () => {
		const dbPath = freshPath();
		const created = await runReopenFixture(dbPath, 'create', 'optimistic', 'stall', 0);
		expect(created.code, created.stderr).toBe(0);

		const reopened = await runReopenFixture(dbPath, 'reopen', 'optimistic', 'stall', 0);
		expect(reopened.signal, reopened.stderr).toBeNull();
		expect(reopened.stdout, reopened.stderr).toContain('WROTE');
		expect(reopened.code, reopened.stderr).toBe(0);
		expect(maintainValues(reopened.stdout)).toEqual([1, 1, 1, 1]);
	}, 150_000);
});

/**
 * The over-budget warning. It can only ever count the column families known at the open that emits
 * it — families are created lazily at runtime — so it reports a configuration that is already over
 * budget and is never a proof of safety.
 */
describe('WriteBufferManager stall — over-budget warning (#821)', () => {
	const BUDGET = 4 * 1024 * 1024;

	// The describe above zeroes the budget when it finishes, and `writeBufferManagerSize` is what
	// gates the warning. `allowStall` is still on from its `beforeAll`: the manager is a singleton,
	// so this resizes it rather than building a second one.
	beforeAll(() => {
		RocksDatabase.config({ writeBufferManagerSize: BUDGET, writeBufferManagerAllowStall: true });
	});

	afterAll(() => {
		RocksDatabase.config({ writeBufferManagerSize: 0 });
	});

	async function warningsFromOpen(maintain: number): Promise<string[]> {
		const warnings: string[] = [];
		const onWarning = (message: string) => {
			if (message.includes('WriteBufferManager budget')) warnings.push(message);
		};
		RocksDatabase.on('log.warn', onWarning);
		try {
			await dbRunner(
				{ dbOptions: [{ maxWriteBufferSizeToMaintain: maintain }] },
				async (_database) => {
					// The warning is emitted during open and delivered through the global event
					// emitter's threadsafe function, so it lands on a later turn of the loop.
					await delay(50);
				}
			);
		} finally {
			RocksDatabase.off('log.warn', onWarning);
		}
		return warnings;
	}

	it('warns when the known families already reach the budget, and says so', async () => {
		const warnings = await warningsFromOpen(BUDGET * 2);
		expect(warnings).toHaveLength(1);
		expect(warnings[0]).toContain('stall permanently');
		expect(warnings[0]).toContain('Only the column families known at this open are counted');
	});

	// RocksDB stalls at `memory_usage() >= buffer_size()`, so a configuration that exactly reaches
	// the budget wedges just as a larger one does.
	it('warns when the total exactly equals the budget', async () => {
		expect(await warningsFromOpen(BUDGET)).toHaveLength(1);
	});

	it('stays quiet below the budget', async () => {
		expect(await warningsFromOpen(BUDGET / 2)).toHaveLength(0);
	});

	// The resolved safeguard target is 1 byte, so this arm can never trip the comparison. That is the
	// guard's limit, not a proof: what a family actually retains at that target is one flushed
	// memtable, and this check does not see it — see the residual note on AGENTS.md invariant 10.
	it("stays quiet for the resolved safeguard target, which is the guard's blind spot", async () => {
		expect(await warningsFromOpen(-1)).toHaveLength(0);
	});
});
