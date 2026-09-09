import { generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { closeSync, openSync, readdirSync, rmSync, writeFileSync, writeSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, describe, expect, it } from 'vitest';

const fixture = join(dirname(fileURLToPath(import.meta.url)), 'fixtures', 'fork-clock-floor.mts');
const LOG = 'clock';
const TXNLOG_FILE_HEADER = 13;
const TXNLOG_ENTRY_HEADER = 13;

const paths: string[] = [];

afterEach(() => {
	if (!process.env.KEEP_FILES) {
		for (const path of paths.splice(0)) {
			rmSync(path, { recursive: true, force: true });
		}
	}
});

function newDBPath(): string {
	const path = generateDBPath();
	paths.push(path);
	return path;
}

function runFixture(
	mode: string,
	dbPath: string,
	key: number,
	log = LOG,
	env: Record<string, string> = {},
	expectedWarning?: string
): Promise<{ code: number | null; stdout: string; stderr: string }> {
	return new Promise((resolve, reject) => {
		const child = spawn(
			process.execPath,
			[fixture, mode, dbPath, String(key), log, expectedWarning ?? ''],
			{
				env: { ...process.env, ...env },
			}
		);
		let stdout = '';
		let stderr = '';
		child.stdout.on('data', (chunk) => (stdout += chunk));
		child.stderr.on('data', (chunk) => (stderr += chunk));
		child.on('error', reject);
		child.on('close', (code) => resolve({ code, stdout, stderr }));
	});
}

describe('monotonic clock floor', () => {
	const aheadOfNow = () => Date.now() + 60 * 60 * 1000;

	it('resumes above the largest durable key in the named log', async () => {
		const dbPath = newDBPath();
		const key = aheadOfNow();

		const write = await runFixture('write', dbPath, key);
		expect(write.code, write.stderr).toBe(0);

		const read = await runFixture('read', dbPath, key);
		expect(read.code, read.stderr).toBe(0);

		const { clock, txnTimestamp, now } = JSON.parse(read.stdout);
		expect(clock).toBeGreaterThan(key);
		expect(txnTimestamp).toBeGreaterThan(key);
		expect(clock).toBeLessThan(key + 60 * 1000);
		expect(now).toBeLessThan(key);
	}, 60000);

	it('takes the largest key, not the last written one', async () => {
		const dbPath = newDBPath();
		const highest = aheadOfNow();

		expect((await runFixture('write', dbPath, highest)).code).toBe(0);
		expect((await runFixture('write', dbPath, highest - 30 * 60 * 1000)).code).toBe(0);

		const read = await runFixture('read', dbPath, highest);
		expect(read.code, read.stderr).toBe(0);
	}, 60000);

	it('finds the largest key in an older segment', async () => {
		const dbPath = newDBPath();
		const highest = aheadOfNow();

		// The largest key lands in the first segment; the lower-keyed batches do not
		// fit beside it and rotate. Nothing in the newest segment names the largest
		// key — its header word is the store's `latestTimestamp` at creation, which
		// starts at 0 in each new process — so only a walk of the older segments
		// finds it.
		expect((await runFixture('write-rotate', dbPath, highest)).code).toBe(0);
		let segments = 1;
		for (let i = 1; i <= 2; i++) {
			const write = await runFixture('write-rotate', dbPath, highest - i * 60 * 1000);
			expect(write.code).toBe(0);
			segments = JSON.parse(write.stdout).segments;
		}
		expect(segments).toBeGreaterThan(1);

		const read = await runFixture('read', dbPath, highest);
		expect(read.code, read.stderr).toBe(0);
	}, 90000);

	it('does not seed from a log the caller did not name', async () => {
		const dbPath = newDBPath();
		const key = aheadOfNow();

		expect((await runFixture('write', dbPath, key)).code).toBe(0);

		// A log keyed by another node's clock is exactly this shape: durable keys
		// ahead of this node's wall clock, in a log it does not originate.
		const other = await runFixture('read-unseeded', dbPath, key, 'some-other-log');
		expect(other.code, other.stderr).toBe(0);
		expect(JSON.parse(other.stdout).clock).toBeLessThan(key);
	}, 60000);

	it('refuses to open when a key is implausibly far ahead of the wall clock', async () => {
		const dbPath = newDBPath();
		const far = Date.now() + 20 * 365 * 24 * 60 * 60 * 1000;

		expect((await runFixture('write', dbPath, far)).code).toBe(0);

		const refused = await runFixture('warn', dbPath, far);
		expect(refused.code).not.toBe(0);
		expect(refused.stderr).toContain('ahead of the wall clock');
	}, 60000);

	it('cleans up a rejected timestamp-floor open before a same-process reopen', async () => {
		const dbPath = newDBPath();
		const far = Date.now() + 20 * 365 * 24 * 60 * 60 * 1000;

		expect((await runFixture('write', dbPath, far)).code).toBe(0);

		const reopened = await runFixture('refuse-then-unseeded', dbPath, far);
		expect(reopened.code, reopened.stderr).toBe(0);
		expect(JSON.parse(reopened.stdout).recovered).toBe(true);
	}, 60000);

	it('refuses to open when a segment cannot be read', async () => {
		const dbPath = newDBPath();
		const highest = aheadOfNow();

		expect((await runFixture('write-rotate', dbPath, highest)).code).toBe(0);
		expect((await runFixture('write-rotate', dbPath, highest - 60 * 1000)).code).toBe(0);

		// Break the token of a segment that is not the current one: the scan cannot
		// read its keys, and the floor it reports may sit below one of them.
		const logDir = join(dbPath, 'transaction_logs', LOG);
		const segments = readdirSync(logDir)
			.filter((name) => name.endsWith('.txnlog'))
			.sort();
		expect(segments.length).toBeGreaterThan(1);
		const fd = openSync(join(logDir, segments[0]), 'r+');
		try {
			writeSync(fd, Buffer.from([0xde, 0xad, 0xbe, 0xef]), 0, 4, 0);
		} finally {
			closeSync(fd);
		}

		const refused = await runFixture('warn', dbPath, highest);
		expect(refused.code).not.toBe(0);
		expect(refused.stderr).toContain('could not be read at open');
	}, 90000);

	it('refuses to open when the scan budget runs out', async () => {
		const dbPath = newDBPath();
		const highest = aheadOfNow();

		expect((await runFixture('write', dbPath, highest)).code).toBe(0);

		const refused = await runFixture('warn', dbPath, highest, LOG, {
			ROCKSDB_JS_TIMESTAMP_FLOOR_SCAN_MS: '0',
		});
		expect(refused.code).not.toBe(0);
		expect(refused.stderr).toContain('scan budget');
	}, 120000);

	it('warns when the named log is not one this database has', async () => {
		const dbPath = newDBPath();
		const key = aheadOfNow();

		expect((await runFixture('write', dbPath, key)).code).toBe(0);

		const warned = await runFixture('warn', dbPath, key, 'clcok');
		expect(warned.code, warned.stderr).toBe(0);
		expect(JSON.parse(warned.stdout).warnings.join(' ')).toContain('does not have');
	}, 60000);

	it('refuses to open when discovery skips a named-log segment', async () => {
		const dbPath = newDBPath();
		const key = aheadOfNow();

		expect((await runFixture('write', dbPath, key)).code).toBe(0);
		writeFileSync(join(dbPath, 'transaction_logs', LOG, 'bad.txnlog'), 'not a log');

		const refused = await runFixture('warn', dbPath, key, LOG, {}, 'discovery skipped');
		expect(refused.code).not.toBe(0);
		expect(refused.stderr).toContain('discovery skipped');
	}, 60000);

	it('refuses a later open that cannot apply timestampFloorLog', async () => {
		const dbPath = newDBPath();
		const key = aheadOfNow();

		expect((await runFixture('write', dbPath, key)).code).toBe(0);

		const refused = await runFixture('reopen-refuse', dbPath, key);
		expect(refused.code, refused.stderr).toBe(0);
		expect(JSON.parse(refused.stdout).error).toContain('monotonic timestamp floor was not seeded');
	}, 60000);

	it('refuses to open when a mid-file framing break hides durable keys', async () => {
		const dbPath = newDBPath();
		const key = aheadOfNow();

		const wrote = await runFixture('write-frames', dbPath, key);
		expect(wrote.code).toBe(0);
		const { payload } = JSON.parse(wrote.stdout);

		// Overrun frame 1's declared length. Entries keep resyncing after it, so the
		// scan classifies a mid-file break — and the entries past it, which stay
		// durable and which a query resyncs to, are the ones carrying the high keys.
		const logDir = join(dbPath, 'transaction_logs', LOG);
		const segment = readdirSync(logDir).find((name) => name.endsWith('.txnlog'))!;
		const frameStride = TXNLOG_ENTRY_HEADER + payload;
		const lengthField = TXNLOG_FILE_HEADER + frameStride + 8;
		const fd = openSync(join(logDir, segment), 'r+');
		try {
			const overrun = Buffer.alloc(4);
			overrun.writeUInt32BE(0xfffffff0, 0);
			writeSync(fd, overrun, 0, 4, lengthField);
		} finally {
			closeSync(fd);
		}

		const refused = await runFixture(
			'warn',
			dbPath,
			key,
			LOG,
			{},
			'framing breaks partway through'
		);
		expect(refused.code).not.toBe(0);
		expect(refused.stderr).toContain('framing breaks partway through');
	}, 120000);

	it('distinguishes an unrecovered read-only torn tail from a framing break', async () => {
		const dbPath = newDBPath();
		const key = aheadOfNow();

		expect((await runFixture('write', dbPath, key)).code).toBe(0);
		const logDir = join(dbPath, 'transaction_logs', LOG);
		const segment = readdirSync(logDir).find((name) => name.endsWith('.txnlog'))!;
		const fd = openSync(join(logDir, segment), 'a');
		try {
			writeSync(fd, Buffer.from([1]));
		} finally {
			closeSync(fd);
		}

		const warned = await runFixture(
			'warn-read-only',
			dbPath,
			key,
			LOG,
			{},
			'partial entry this handle cannot recover'
		);
		expect(warned.code, warned.stderr).toBe(0);
		const { warnings, clock } = JSON.parse(warned.stdout);
		expect(warnings.join(' ')).toContain('partial entry this handle cannot recover');
		expect(warnings.join(' ')).not.toContain('framing breaks partway through');
		expect(clock).toBeGreaterThan(key);
	}, 60000);

	it('recovers a writable torn tail before scanning the floor', async () => {
		const dbPath = newDBPath();
		const key = aheadOfNow();

		expect((await runFixture('write', dbPath, key)).code).toBe(0);
		const logDir = join(dbPath, 'transaction_logs', LOG);
		const segment = readdirSync(logDir).find((name) => name.endsWith('.txnlog'))!;
		const fd = openSync(join(logDir, segment), 'a');
		try {
			writeSync(fd, Buffer.from([1]));
		} finally {
			closeSync(fd);
		}

		const read = await runFixture('read', dbPath, key);
		expect(read.code, read.stderr).toBe(0);
	}, 60000);

	it('still scans under a budget too large for the clock to add', async () => {
		const dbPath = newDBPath();
		const key = aheadOfNow();

		expect((await runFixture('write', dbPath, key)).code).toBe(0);

		const read = await runFixture('read', dbPath, key, LOG, {
			ROCKSDB_JS_TIMESTAMP_FLOOR_SCAN_MS: '99999999999999',
		});
		expect(read.code, read.stderr).toBe(0);
		expect(JSON.parse(read.stdout).clock).toBeGreaterThan(key);
	}, 60000);

	it('leaves the clock alone when no log is named', async () => {
		const dbPath = newDBPath();
		const key = aheadOfNow();

		expect((await runFixture('write-unseeded', dbPath, key)).code).toBe(0);

		const read = await runFixture('read-unseeded', dbPath, key, '');
		expect(read.code, read.stderr).toBe(0);
	}, 60000);
});
