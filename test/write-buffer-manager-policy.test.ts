import { RocksDatabase } from '../src/index.ts';
import { dbRunner, generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { mkdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { afterAll, describe, expect, it } from 'vitest';

const KiB = 1024;
const MiB = 1024 * KiB;
const arenaFixturePath = join(__dirname, 'fixtures', 'fork-wbm-arena-block-size.mts');
const policyFixturePath = join(__dirname, 'fixtures', 'fork-wbm-flush-policy.mts');
const crossDbStallFixturePath = join(__dirname, 'fixtures', 'fork-wbm-cross-db-stall.mts');

type FixtureResult = {
	code: number | null;
	signal: NodeJS.Signals | null;
	stdout: string;
	stderr: string;
};

/**
 * Runs a fixture in its own process. The manager is process-global, and a stalled `putSync` blocks
 * the JS thread, so only killing the child bounds a regression.
 */
function runFixture(
	fixturePath: string,
	args: string[],
	deadlineMs: number
): Promise<FixtureResult> {
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, [fixturePath, ...args]);
		let stdout = '';
		let stderr = '';
		child.stdout?.on('data', (chunk) => {
			stdout += chunk.toString();
		});
		child.stderr?.on('data', (chunk) => {
			stderr += chunk.toString();
		});
		const timer = setTimeout(() => child.kill('SIGKILL'), deadlineMs);
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

function parseLine(stdout: string, tag: string): any {
	const line = new RegExp(`^${tag} (.*)$`, 'm').exec(stdout);
	if (!line) throw new Error(`fixture printed no ${tag} line; stdout was:\n${stdout}`);
	return JSON.parse(line[1]);
}

const dbPaths: string[] = [];
function freshPath(): string {
	const dbPath = generateDBPath();
	mkdirSync(dbPath, { recursive: true });
	dbPaths.push(dbPath);
	return dbPath;
}

afterAll(() => {
	if (process.env.KEEP_FILES) return;
	for (const dbPath of dbPaths) {
		rmSync(dbPath, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
	}
});

describe('arenaBlockSize', () => {
	it.each([NaN, Infinity, -1, 1.5, 2 ** 53])('should reject %s', (arenaBlockSize) =>
		dbRunner({ dbOptions: [{ arenaBlockSize }], skipOpen: true }, async ({ db }) => {
			expect(() => db.open()).toThrow('arenaBlockSize must be a non-negative safe integer');
		})
	);

	it('should scale with the attached WriteBufferManager budget and honor explicit values', async () => {
		const result = await runFixture(arenaFixturePath, [freshPath()], 60_000);
		expect(result.code, result.stderr).toBe(0);
		const arena = parseLine(result.stdout, 'ARENA');

		// No manager: RocksDB's own min(1 MiB, writeBufferSize / 8), unchanged.
		expect(arena.noManager).toEqual({ default: 1 * MiB });
		expect(arena.noManagerSmallWriteBuffer).toEqual({ default: 32 * KiB });
		// 64 MiB / 2048 = 32 KiB, raised to the 64 KiB floor.
		expect(arena.budget64MiB).toEqual({ default: 64 * KiB });
		// The writeBufferSize / 8 cap wins over the floor.
		expect(arena.budget64MiBSmallWriteBuffer).toEqual({ default: 32 * KiB });
		expect(arena.budget64MiBUnalignedWriteBuffer).toEqual({ default: 12_500 });
		expect(arena.budget64MiBExplicit).toEqual({ default: 200_000 });
		expect(arena.budget512MiB).toEqual({ default: 256 * KiB });
		expect(arena.budget4GiB).toEqual({ default: 1 * MiB });
		// Created after the live manager was resized to 160 MiB; the open family is not retuned.
		expect(arena.lateFamily).toEqual({ default: 256 * KiB, late: 80 * KiB });
		// `writeBufferManagerSize: 0` leaves the manager attached to this database at 160 MiB.
		expect(arena.afterDisableFamily).toEqual({
			default: 256 * KiB,
			late: 80 * KiB,
			afterDisable: 80 * KiB,
		});
		expect(arena.afterDisableNewDatabase).toEqual({ default: 1 * MiB });
		expect(arena.afterDisableExplicit).toEqual({ default: 300_000 });
	}, 90_000);
});

describe('writeBufferManagerFlushPolicy', () => {
	it.each([
		['newest', RangeError],
		['largestAcrossDBsX', RangeError],
		['largestAcross', RangeError],
		['', RangeError],
		['x'.repeat(100), RangeError],
		[42, TypeError],
		[true, TypeError],
	])('should reject %j', (value, errorType) => {
		expect(() => RocksDatabase.config({ writeBufferManagerFlushPolicy: value as any })).toThrow(
			errorType
		);
	});

	it('should follow allowStall by default and let an explicit value win', async () => {
		const result = await runFixture(
			policyFixturePath,
			[join(freshPath(), 'db'), 'default'],
			60_000
		);
		expect(result.code, result.stderr).toBe(0);
		expect(parseLine(result.stdout, 'POLICY')).toEqual({
			unconfigured: 'oldest',
			unconfiguredStallOn: 'largestAcrossDBs',
			created: 'oldest',
			stallOn: 'largestAcrossDBs',
			rejected: 'RangeError',
			afterRejected: 'largestAcrossDBs',
			allowStallAfterRejected: 'true',
			rejectedByOtherField: 'RangeError',
			afterRejectedByOtherField: 'largestAcrossDBs',
			stallOff: 'oldest',
			explicitLargest: 'largest',
			explicitLargestStallOn: 'largest',
			explicitOldestStallOn: 'oldest',
			explicitAcrossStallOff: 'largestAcrossDBs',
			undefinedKeepsExplicit: 'largestAcrossDBs',
			value: 'v3',
		});
	}, 90_000);

	it('should create the manager with a policy configured before the first open', async () => {
		const result = await runFixture(
			policyFixturePath,
			[join(freshPath(), 'db'), 'explicitFirst'],
			60_000
		);
		expect(result.code, result.stderr).toBe(0);
		expect(parseLine(result.stdout, 'POLICY')).toEqual({ created: 'largest', stallOff: 'largest' });
	}, 90_000);

	/**
	 * rocksdb-js#822 class: with stalls allowed, a writer to one database waits for memory held by
	 * idle databases. The explicit `'oldest'` run is the control showing the fixture builds that
	 * state: it must reach setup and then never write.
	 */
	it('should not stall forever on memory held by idle databases (default policy)', async () => {
		const result = await runFixture(crossDbStallFixturePath, [freshPath(), 'default'], 60_000);
		expect(result.signal, result.stderr).toBeNull();
		expect(parseLine(result.stdout, 'SETUP').flushPolicy).toBe('largestAcrossDBs');
		expect(result.stdout).toContain(`WROTE ${4096}`);
		expect(result.code, result.stderr).toBe(0);
		expect(parseLine(result.stdout, 'FINAL').memoryUsage).toBeLessThan(16 * MiB);
	}, 90_000);

	it.skipIf(process.env.ROCKSDB_JS_WBM_STALL_WARN_MS === '0')(
		'should stall forever under an explicit oldest policy (control)',
		async () => {
			// Past the stall watchdog's 5 s report, which is the positive evidence of a stall (as opposed
			// to the child dying some other way before it wrote).
			const result = await runFixture(crossDbStallFixturePath, [freshPath(), 'oldest'], 12_000);
			const setup = parseLine(result.stdout, 'SETUP');
			expect(setup.flushPolicy).toBe('oldest');
			expect(setup.memoryUsage).toBeGreaterThanOrEqual(setup.bufferSize);
			expect(result.stdout).not.toContain('WROTE');
			expect(result.stderr).toContain('WriteBufferManager write stall active');
			expect(result.code).not.toBe(0);
		},
		30_000
	);
});
