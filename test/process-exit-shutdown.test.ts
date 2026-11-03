import { generateDBPath } from './lib/util.ts';
import { spawnSync } from 'node:child_process';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const fixture = join(__dirname, 'fixtures', 'fork-process-exit-shutdown.mts');
// Exit semantics are the runtime's, so run the fixture under the runtime running the tests.
const runtimeArgs = process.versions.deno ? ['run', '-A'] : [];

function run(mode: string, path: string, env?: NodeJS.ProcessEnv) {
	return spawnSync(process.execPath, [...runtimeArgs, fixture, mode, path], {
		encoding: 'utf8',
		env: { ...process.env, ...env },
		timeout: 20_000,
	});
}

/** Runs `mode`, asserts how it exited, then reads back what reached disk. */
function exitThenVerify(
	mode: string,
	{ env, status = 0 }: { env?: NodeJS.ProcessEnv; status?: number } = {}
) {
	const path = generateDBPath();
	try {
		const exited = run(mode, path, env);
		expect(exited.signal, exited.stderr).toBeNull();
		expect(exited.status, exited.stderr).toBe(status);
		const verified = run('verify', path);
		expect(verified.status, verified.stderr).toBe(0);
		return { exited, values: JSON.parse(verified.stdout.trim().split(/\r?\n/).at(-1)!) };
	} finally {
		if (!process.env.KEEP_FILES) {
			rmSync(path, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
			rmSync(`${path}-late`, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
		}
	}
}

const mainOnly = { main: 'value', worker: null, late: null };

describe('Main-thread exit shutdown (#1004)', () => {
	it('flushes a disableWAL write on process.exit()', () => {
		expect(exitThenVerify('exit').values).toEqual(mainOnly);
	});

	it('flushes a disableWAL write on natural exit', () => {
		expect(exitThenVerify('natural').values).toEqual(mainOnly);
	});

	it('flushes a disableWAL write when an uncaught exception ends the process', () => {
		expect(exitThenVerify('uncaught', { status: 1 }).values).toEqual(mainOnly);
	});

	it('skips the flush when shutdownOnExit is false', () => {
		expect(exitThenVerify('opt-out').values).toEqual({ main: null, worker: null, late: null });
	});

	it('runs a prepended exit listener while the database is still open', () => {
		expect(exitThenVerify('prepend').values).toEqual({ ...mainOnly, late: 'value' });
	});

	it('rejects opens from exit listeners that run after the shutdown', () => {
		const { exited, values } = exitThenVerify('fenced');
		expect(values).toEqual(mainOnly);
		const lines = exited.stdout.trim().split(/\r?\n/);
		expect(JSON.parse(lines[0])).toEqual({ open: false });
		expect(lines[1]).toContain('the process is exiting and rocksdb-js has shut down');
	});

	it('tolerates a caller also calling shutdown() from its own exit listeners', () => {
		const { exited, values } = exitThenVerify('caller-shutdown');
		expect(values).toEqual(mainOnly);
		expect(exited.stderr).not.toContain('rocksdb-js');
	});

	it('releases a descriptor still quarantined at process.exit()', () => {
		const path = generateDBPath();
		try {
			const exited = run('quarantined', path, { ROCKSDB_JS_CLOSE_FLUSH_FAILURE: '2' });
			expect(exited.signal, exited.stderr).toBeNull();
			expect(exited.status, exited.stderr).toBe(0);
			expect(exited.stderr).toContain('rocksdb-js database registry cleanup failed');
			expect(exited.stdout).toContain('later listener ran');
		} finally {
			if (!process.env.KEEP_FILES) {
				rmSync(path, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
			}
		}
	});

	it('exits without a signal when process.exit() lands during a compaction', () => {
		expect(exitThenVerify('compact').values).toEqual(mainOnly);
	});

	it.each(['0', '1', '2'])(
		'exits cleanly with an async commit in flight (ROCKSDB_JS_COMMIT_THREAD=%s)',
		(mode) => {
			expect(exitThenVerify('commit', { env: { ROCKSDB_JS_COMMIT_THREAD: mode } }).values).toEqual(
				mainOnly
			);
		}
	);

	it.skipIf(process.versions.deno)(
		'flushes main-thread and worker databases on process.exit() with a worker running',
		() => {
			expect(exitThenVerify('worker').values).toEqual({ ...mainOnly, worker: 'value' });
		}
	);

	it.skipIf(process.versions.deno)(
		"leaves the main thread's databases open when a worker exits",
		() => {
			expect(exitThenVerify('worker-exit').values).toEqual({ ...mainOnly, worker: 'value' });
		}
	);
});
