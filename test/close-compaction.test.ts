import { generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

// Close cancels background compaction instead of waiting for it (rocksdb-js
// #1001). Each case runs test/fixtures/fork-close-compaction.mts in a child:
// the test seams are read once at module load, and without the fix a close
// blocks for the slowed compaction's full duration (tens of seconds), which
// only a parent-side deadline can turn into a failure.

const fixture = join(__dirname, 'fixtures', 'fork-close-compaction.mts');
const nodeExecutable =
	process.env.NODE_BINARY ??
	(process.versions.bun || process.versions.deno
		? (process.env.npm_node_execpath ?? 'node')
		: process.execPath);
// Every compacted key sleeps this long, so one compaction of the fixture's
// 40,000 keys takes over 40 seconds.
const SLOW_COMPACTION = { ROCKSDB_JS_COMPACTION_KEY_DELAY_MS: '1' };
const DEADLINE_MS = 20_000;

function runFixture(dbPath: string, mode: string, env: NodeJS.ProcessEnv = {}): Promise<string> {
	return new Promise((resolve, reject) => {
		const child = spawn(nodeExecutable, ['--expose-gc', fixture, dbPath, mode], {
			env: { ...process.env, ...env },
		});
		let stdout = '';
		let stderr = '';
		child.stdout.on('data', (chunk) => {
			stdout += chunk.toString();
		});
		child.stderr.on('data', (chunk) => {
			stderr += chunk.toString();
		});
		const timeout = setTimeout(() => {
			child.kill('SIGKILL');
			reject(new Error(`${mode} fixture did not finish in ${DEADLINE_MS}ms\n${stdout}${stderr}`));
		}, DEADLINE_MS);
		child.on('error', (error) => {
			clearTimeout(timeout);
			reject(new Error(`Unable to run the close fixture with Node (${nodeExecutable}): ${error}`));
		});
		child.on('close', (code, signal) => {
			clearTimeout(timeout);
			if (code === 0 && signal === null) {
				resolve(stdout);
			} else {
				reject(
					new Error(`${mode} fixture failed (code=${code}, signal=${signal})\n${stdout}${stderr}`)
				);
			}
		});
	});
}

async function withPath(test: (dbPath: string) => Promise<void>): Promise<void> {
	const dbPath = generateDBPath();
	try {
		await test(dbPath);
	} finally {
		if (!process.env.KEEP_FILES) {
			rmSync(dbPath, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
		}
	}
}

describe('Close during background compaction', () => {
	it(
		'close() aborts a running compaction, and the reopened database has every flushed key',
		() =>
			withPath(async (dbPath) => {
				await runFixture(dbPath, 'close', SLOW_COMPACTION);
			}),
		30_000
	);

	it(
		'process exit aborts a running compaction and still flushes',
		() =>
			withPath(async (dbPath) => {
				const started = performance.now();
				const stdout = await runFixture(dbPath, 'exit', SLOW_COMPACTION);
				expect(stdout).toContain('"compacting":true');
				expect(performance.now() - started).toBeLessThan(10_000);
				await runFixture(dbPath, 'verify');
			}),
		40_000
	);

	it(
		'terminating the worker that owns the database aborts its running compaction',
		() =>
			withPath(async (dbPath) => {
				await runFixture(dbPath, 'worker', SLOW_COMPACTION);
			}),
		30_000
	);

	it(
		'compactOnClose still runs its compaction to completion',
		() =>
			withPath(async (dbPath) => {
				await runFixture(dbPath, 'compactOnClose', SLOW_COMPACTION);
			}),
		30_000
	);

	it(
		'records the flushed transaction-log position before the flush listener runs',
		() =>
			withPath(async (dbPath) => {
				await runFixture(dbPath, 'checkpoint', { ROCKSDB_JS_FLUSH_COMPLETED_DELAY_MS: '1500' });
			}),
		30_000
	);

	it(
		'a shutdown() retry after a post-cancellation failure does not flush again',
		() =>
			withPath(async (dbPath) => {
				await runFixture(dbPath, 'retryAfterCancel', {
					ROCKSDB_JS_CLOSE_FAILURE_AFTER_CANCEL: '1',
				});
			}),
		30_000
	);
});
