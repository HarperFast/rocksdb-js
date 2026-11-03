import { RocksDatabase } from '../src/index.ts';
import { generateDBPath } from './lib/util.ts';
import { spawnSync } from 'node:child_process';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const fixture = join(__dirname, 'fixtures', 'fork-process-exit-shutdown.mts');
// Exit semantics are the runtime's, so run the fixture under the runtime running the tests.
const runtimeArgs = process.versions.deno ? ['run', '-A'] : [];
const derivedPaths = ['-late', '-churn-0', '-churn-1', '-churn-2'];

function run(mode: string, path: string, env?: NodeJS.ProcessEnv) {
	return spawnSync(process.execPath, [...runtimeArgs, fixture, mode, path], {
		encoding: 'utf8',
		env: { ...process.env, ...env },
		timeout: 20_000,
	});
}

function exitThenVerify(
	mode: string,
	{
		env,
		status = 0,
		verifySuffix = '',
	}: { env?: NodeJS.ProcessEnv; status?: number; verifySuffix?: string } = {}
) {
	const path = generateDBPath();
	try {
		const started = performance.now();
		const exited = run(mode, path, env);
		const elapsed = performance.now() - started;
		expect(exited.signal, exited.stderr).toBeNull();
		expect(exited.status, exited.stderr).toBe(status);
		const verified = run('verify', `${path}${verifySuffix}`);
		expect(verified.status, verified.stderr).toBe(0);
		return {
			elapsed,
			exited,
			values: JSON.parse(verified.stdout.trim().split(/\r?\n/).at(-1)!),
		};
	} finally {
		if (!process.env.KEEP_FILES) {
			for (const suffix of ['', ...derivedPaths]) {
				rmSync(`${path}${suffix}`, {
					force: true,
					recursive: true,
					maxRetries: 3,
					retryDelay: 500,
				});
			}
		}
	}
}

const mainOnly = { main: 'value', worker: null, late: null, commit: null };
const nothing = { main: null, worker: null, late: null, commit: null };

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

	it('skips the exit listener when shutdownOnExit is false', () => {
		// Bun runs the main env's native cleanup hook on process.exit(), which flushes regardless.
		expect(exitThenVerify('opt-out').values).toEqual(process.versions.bun ? mainOnly : nothing);
	});

	it('rejects a non-boolean shutdownOnExit', () => {
		expect(() => RocksDatabase.config({ shutdownOnExit: 'false' as unknown as boolean })).toThrow(
			'shutdownOnExit must be a boolean'
		);
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

	// Relies on Node stopping the worker after the exit listeners. Bun runs no worker cleanup on
	// process.exit(), and a worker still inside a native close at that point crashed Bun 1.4.0.
	it.skipIf(process.versions.deno || process.versions.bun)(
		'closes what a worker close leaves open or quarantined after the main thread exits',
		() => {
			const { exited, values } = exitThenVerify('concurrent-close', {
				env: { ROCKSDB_JS_CLOSE_FLUSH_FAILURE: '2', ROCKSDB_JS_CLOSE_RETRY_DELAY_MS: '3000' },
				verifySuffix: '-late',
			});
			expect(exited.stderr).toContain('Timed out waiting for another database shutdown to finish');
			expect(values).toEqual({ ...nothing, late: 'value' });
		}
	);

	it('cancels a manual compaction in flight at process.exit()', () => {
		const { elapsed, values } = exitThenVerify('compact', {
			env: { ROCKSDB_JS_COMPACT_DELAY_MS: '15000' },
		});
		expect(values).toEqual(mainOnly);
		expect(elapsed).toBeLessThan(10_000);
	});

	it.each(['0', '1', '2'])(
		'applies an async commit in flight at process.exit() (ROCKSDB_JS_COMMIT_THREAD=%s)',
		(mode) => {
			const { values } = exitThenVerify('commit', {
				env: { ROCKSDB_JS_COMMIT_THREAD: mode, ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS: '500' },
			});
			expect(values).toEqual({ ...mainOnly, commit: 'value' });
		}
	);

	it.skipIf(process.versions.deno)(
		'flushes main-thread and worker databases on process.exit() with a worker running',
		() => {
			expect(exitThenVerify('worker').values).toEqual({ ...mainOnly, worker: 'value' });
		}
	);

	it.skipIf(process.versions.deno)(
		'exits cleanly while workers keep opening databases during the exit shutdown',
		() => {
			for (let i = 0; i < 5; i++) {
				expect(exitThenVerify('worker-churn').values).toEqual(mainOnly);
			}
		}
	);

	it.skipIf(process.versions.deno)(
		"leaves the main thread's databases open when a worker exits",
		() => {
			expect(exitThenVerify('worker-exit').values).toEqual({ ...mainOnly, worker: 'value' });
		}
	);
});
