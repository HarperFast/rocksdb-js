// Child-process coverage for the conflict-before-log protocol: the commit execution
// mode and the crash point are read once per process, so neither can be varied
// inside a Vitest worker.

import { RocksDatabase } from '../src/index.ts';
import { generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const isNode = !process.versions.bun && !process.versions.deno;

function removeDBPath(dbPath: string): void {
	if (!process.env.KEEP_FILES) {
		rmSync(dbPath, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
	}
}

function run(
	fixture: string,
	args: string[],
	env: Record<string, string | undefined>
): Promise<{ code: number | null; signal: NodeJS.Signals | null; output: string }> {
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, [join(__dirname, 'fixtures', fixture), ...args], {
			env: { ...process.env, ...env },
		});
		let output = '';
		child.stdout.setEncoding('utf8');
		child.stderr.setEncoding('utf8');
		child.stdout.on('data', (chunk) => (output += chunk));
		child.stderr.on('data', (chunk) => (output += chunk));
		child.on('error', reject);
		child.on('close', (code, signal) => resolve({ code, signal, output }));
	});
}

describe.skipIf(!isNode)('conflict before the log write across commit modes', () => {
	for (const commitThread of ['0', '1', '2']) {
		for (const policy of ['parallel', 'serial']) {
			for (const mode of ['async', 'sync']) {
				it(`refuses before the log write and keeps a durable handle committable (${mode}, ${policy}, commit thread ${commitThread})`, async () => {
					const dbPath = generateDBPath();
					try {
						const result = await run('fork-conflict-before-log.mts', [dbPath, mode, policy], {
							ROCKSDB_JS_COMMIT_THREAD: commitThread,
						});
						expect(result.code, result.output).toBe(0);
					} finally {
						removeDBPath(dbPath);
					}
				}, 60_000);
			}
		}
	}
});

describe.skipIf(!isNode)('a process that dies inside a logged commit', () => {
	type Position = { sequence: number; offset: number };
	const positionAtMost = (a: Position, b: Position) =>
		a.sequence < b.sequence || (a.sequence === b.sequence && a.offset <= b.offset);

	async function crash(point: '1' | '2' | '3') {
		const dbPath = generateDBPath();
		const result = await run('fork-commit-crash.mts', [dbPath, point], {});
		expect(result.output, result.output).toContain('ready');
		expect(result.code, result.output).toBe(137);
		const expectedPosition: Position = JSON.parse(result.output.split('\n')[0]).expectedPosition;
		return { dbPath, expectedPosition };
	}

	it('after the log write and before the RocksDB commit: the entry is on disk, the data is not, and txn.state stays before the entry', async () => {
		const { dbPath, expectedPosition } = await crash('1');
		const db = RocksDatabase.open(dbPath);
		try {
			const log = db.useLog('audit');
			const entries = [...log.query({ start: 0, readUncommitted: true })].map((entry) =>
				entry.data.toString()
			);
			expect(entries).toEqual(['seed', 'crash-entry']);
			expect(await db.get('k')).toBeUndefined();
			expect(await db.get('seed')).toBe('seed');
			const stats = log.getStats();
			expect(stats.lastFlushedPosition).toEqual(expectedPosition);
			expect(positionAtMost(stats.lastFlushedPosition, expectedPosition)).toBe(true);
		} finally {
			db.close();
			removeDBPath(dbPath);
		}
	}, 60_000);

	it('after the RocksDB commit: the entry and its data are both on disk', async () => {
		const { dbPath } = await crash('2');
		const db = RocksDatabase.open(dbPath);
		try {
			const log = db.useLog('audit');
			expect([...log.query({ start: 0 })].map((entry) => entry.data.toString())).toEqual([
				'seed',
				'crash-entry',
			]);
			expect(await db.get('k')).toBe('crashed');
		} finally {
			db.close();
			removeDBPath(dbPath);
		}
	}, 60_000);

	it('after a pre-log rejection: neither the entry nor the write exists', async () => {
		const { dbPath, expectedPosition } = await crash('3');
		const db = RocksDatabase.open(dbPath);
		try {
			const log = db.useLog('audit');
			expect(
				[...log.query({ start: 0, readUncommitted: true })].map((entry) => entry.data.toString())
			).toEqual(['seed']);
			expect(await db.get('k')).toBe('conflict');
			expect(log.getStats().nextLogPosition).toEqual(expectedPosition);
		} finally {
			db.close();
			removeDBPath(dbPath);
		}
	}, 60_000);
});
