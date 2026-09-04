import { generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { closeSync, openSync, readdirSync, rmSync, writeSync } from 'node:fs';
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

/** Runs one fixture mode in its own process, so it starts from a fresh process clock. */
function runFixture(
	mode: string,
	dbPath: string,
	key: number,
	log = LOG,
	env: Record<string, string> = {}
): Promise<{ code: number | null; stdout: string; stderr: string }> {
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, [fixture, mode, dbPath, String(key), log], {
			env: { ...process.env, ...env },
		});
		let stdout = '';
		let stderr = '';
		child.stdout.on('data', (chunk) => (stdout += chunk));
		child.stderr.on('data', (chunk) => (stderr += chunk));
		child.on('error', reject);
		child.on('close', (code) => resolve({ code, stdout, stderr }));
	});
}

describe('monotonic clock floor', () => {
	// The durable keys sit an hour ahead of the wall clock, which is what a
	// backward clock step across a restart leaves behind.
	const aheadOfNow = () => Date.now() + 60 * 60 * 1000;

	it('resumes above the largest durable key in the named log', async () => {
		const dbPath = newDBPath();
		const key = aheadOfNow();

		const write = await runFixture('write', dbPath, key);
		expect(write.stderr).toBe('');
		expect(write.code).toBe(0);

		const read = await runFixture('read', dbPath, key);
		expect(read.stderr).toBe('');
		expect(read.code).toBe(0);

		const { clock, txnTimestamp, now } = JSON.parse(read.stdout);
		expect(clock).toBeGreaterThan(key);
		expect(txnTimestamp).toBeGreaterThan(key);
		// The seed is the key itself, not an unbounded jump past it.
		expect(clock).toBeLessThan(key + 60 * 1000);
		expect(now).toBeLessThan(key);
	}, 60000);

	it('takes the largest key, not the last written one', async () => {
		const dbPath = newDBPath();
		const highest = aheadOfNow();

		expect((await runFixture('write', dbPath, highest)).code).toBe(0);
		// Batch keys are not ordered within a log: a later batch can carry a
		// smaller key, and the floor must still clear the larger one.
		expect((await runFixture('write', dbPath, highest - 30 * 60 * 1000)).code).toBe(0);

		const read = await runFixture('read', dbPath, highest);
		expect(read.stderr).toBe('');
		expect(read.code).toBe(0);
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
		expect(read.stderr).toBe('');
		expect(read.code).toBe(0);
	}, 90000);

	it('does not seed from a log the caller did not name', async () => {
		const dbPath = newDBPath();
		const key = aheadOfNow();

		expect((await runFixture('write', dbPath, key)).code).toBe(0);

		// A log keyed by another node's clock is exactly this shape: durable keys
		// ahead of this node's wall clock, in a log it does not originate.
		const other = await runFixture('read-unseeded', dbPath, key, 'some-other-log');
		expect(other.stderr).toBe('');
		expect(other.code).toBe(0);
		expect(JSON.parse(other.stdout).clock).toBeLessThan(key);
	}, 60000);

	it('refuses a key implausibly far ahead of the wall clock, and says so', async () => {
		const dbPath = newDBPath();
		// Twenty years ahead: inside the timestamp domain, so a caller can assign
		// it, but far past any rollback this is meant to recover from.
		const far = Date.now() + 20 * 365 * 24 * 60 * 60 * 1000;

		expect((await runFixture('write', dbPath, far)).code).toBe(0);

		const warned = await runFixture('warn', dbPath, far);
		expect(warned.stderr).toBe('');
		expect(warned.code).toBe(0);

		const { warnings, clock } = JSON.parse(warned.stdout);
		expect(warnings.join(' ')).toContain('ahead of the wall clock');
		expect(clock).toBeLessThan(far);
	}, 60000);

	it('still opens, and says so, when a segment cannot be read', async () => {
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

		const warned = await runFixture('warn', dbPath, highest);
		expect(warned.stderr).toBe('');
		expect(warned.code).toBe(0);

		const { warnings, clock } = JSON.parse(warned.stdout);
		expect(warnings.join(' ')).toContain('could not be read at open');
		// The walk continues past the failure: the healthy segment's key still
		// seeds the floor, and the unreadable segment's higher key does not.
		expect(clock).toBeGreaterThan(highest - 60 * 1000);
		expect(clock).toBeLessThan(highest);
	}, 90000);

	it('stops at the scan budget rather than stalling open, and says so', async () => {
		const dbPath = newDBPath();
		const highest = aheadOfNow();

		expect((await runFixture('write', dbPath, highest)).code).toBe(0);

		// A budget of zero is honored literally, which is what makes this
		// deterministic: nothing is scanned, so nothing seeds the floor.
		const warned = await runFixture('warn', dbPath, highest, LOG, {
			ROCKSDB_JS_TIMESTAMP_FLOOR_SCAN_MS: '0',
		});
		expect(warned.stderr).toBe('');
		expect(warned.code).toBe(0);

		const { warnings, clock } = JSON.parse(warned.stdout);
		expect(warnings.join(' ')).toContain('scan budget');
		expect(clock).toBeLessThan(highest);
	}, 120000);

	it('warns when the named log is not one this database has', async () => {
		const dbPath = newDBPath();
		const key = aheadOfNow();

		expect((await runFixture('write', dbPath, key)).code).toBe(0);

		// A typo protects nothing while looking configured, so it is not silent.
		const warned = await runFixture('warn', dbPath, key, 'clcok');
		expect(warned.stderr).toBe('');
		expect(warned.code).toBe(0);
		expect(JSON.parse(warned.stdout).warnings.join(' ')).toContain('does not have');
	}, 60000);

	it('warns when a later open of the same path names a log', async () => {
		const dbPath = newDBPath();
		const key = aheadOfNow();

		expect((await runFixture('write', dbPath, key)).code).toBe(0);

		// The first open in the process fixes the option; a second one that names a
		// log cannot re-run the seed, so it must not look applied.
		const warned = await runFixture('reopen-warn', dbPath, key);
		expect(warned.stderr).toBe('');
		expect(warned.code).toBe(0);
		expect(JSON.parse(warned.stdout).warnings.join(' ')).toContain('was ignored');
	}, 60000);

	it('reports the keys a mid-file framing break hides, and does not claim them', async () => {
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

		const warned = await runFixture('warn', dbPath, key);
		expect(warned.stderr).toBe('');
		expect(warned.code).toBe(0);

		const { warnings, clock } = JSON.parse(warned.stdout);
		expect(warnings.join(' ')).toContain('framing breaks mid-file');
		// The floor stops at the key before the break rather than silently claiming
		// to have covered the ones after it.
		expect(clock).toBeLessThan(key);
	}, 120000);

	it('leaves the clock alone when no log is named', async () => {
		const dbPath = newDBPath();
		const key = aheadOfNow();

		expect((await runFixture('write-unseeded', dbPath, key)).code).toBe(0);

		const read = await runFixture('read-unseeded', dbPath, key, '');
		expect(read.stderr).toBe('');
		expect(read.code).toBe(0);
	}, 60000);
});
