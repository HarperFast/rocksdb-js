import { type ChildResult, runChild } from './lib/child-process.ts';
import { generateDBPath } from './lib/util.ts';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { describe, it } from 'vitest';

// A backup's txn.state must match its table files (HarperFast/harper#4507). Each
// case runs test/fixtures/fork-backup-replay-position.mts in a child because
// the delay seams are read once at module load.

const fixture = join(__dirname, 'fixtures', 'fork-backup-replay-position.mts');
const nodeExecutable =
	process.env.NODE_BINARY ??
	(process.versions.bun || process.versions.deno
		? (process.env.npm_node_execpath ?? 'node')
		: process.execPath);
const DEADLINE_MS = 20_000;

async function runFixture(mode: string, env: NodeJS.ProcessEnv = {}): Promise<void> {
	const dbPath = generateDBPath();
	try {
		const result: ChildResult = await runChild(nodeExecutable, [fixture, dbPath, mode], {
			deadlineMs: DEADLINE_MS,
			env: { ...process.env, ...env },
			closeTimeoutMs: 5_000,
		});
		const { code, signal, stdout, stderr, timedOut } = result;
		if (timedOut) {
			throw new Error(`${mode} fixture did not finish in ${DEADLINE_MS}ms\n${stdout}${stderr}`);
		}
		if (code !== 0 || signal !== null) {
			throw new Error(
				`${mode} fixture failed (code=${code}, signal=${signal})\n${stdout}${stderr}`
			);
		}
	} finally {
		rmSync(dbPath, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
	}
}

describe('Backup replay position', () => {
	it(
		'a directory backup waits for its flush to record the replayed position',
		() => runFixture('directoryBehind', { ROCKSDB_JS_FLUSH_COMPLETED_DELAY_MS: '1000' }),
		30_000
	);

	it(
		'a stream backup waits for its flush to record the replayed position',
		() => runFixture('streamBehind', { ROCKSDB_JS_FLUSH_COMPLETED_DELAY_MS: '1000' }),
		30_000
	);

	it(
		'a stream backup keeps the position of its table files when a later flush advances it',
		() => runFixture('streamAhead'),
		30_000
	);

	it(
		'concurrent backups under continuous flushes converge without skipping writes',
		() => runFixture('underLoad'),
		30_000
	);

	it(
		'concurrent backups of a pessimistic database under continuous flushes converge',
		() => runFixture('underLoadPessimistic'),
		30_000
	);

	it(
		'a pessimistic database, whose listings always flush, captures without waiting on itself',
		() => runFixture('pessimistic'),
		30_000
	);

	it(
		'a capture whose held listing a flush escapes lists with flushes running instead',
		() => runFixture('escapeFallback', { ROCKSDB_JS_BACKUP_CAPTURE_LIST_DELAY_MS: '1500' }),
		30_000
	);

	it(
		'closing a handle aborts a backup waiting for a running flush',
		() => runFixture('closeDuringCapture', { ROCKSDB_JS_FLUSH_COMPLETED_DELAY_MS: '3000' }),
		30_000
	);
});
