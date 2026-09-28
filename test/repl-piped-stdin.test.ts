import { RocksDatabase } from '../src/index.ts';
import { generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

// HarperFast/rocksdb-js#886: ask() rebuilt a one-shot rl.question() listener around every
// prompt. Piped stdin routinely delivers several commands in one chunk; readline parses
// every line in that chunk synchronously, so only the first had a listener and the rest
// were dropped. These tests spawn the real CLI and write the whole script in ONE
// stdin.write() call (matching `printf '...' | node bin/rocksdb-js.mjs`) — the bug does
// not reproduce if the lines are written separately, since each would arrive as its own
// chunk with ask() ready to receive it.
const cliPath = join(import.meta.dirname, '..', 'bin', 'rocksdb-js.mjs');

function runCLI(dbPath: string, input: string): Promise<{ code: number | null; stdout: string }> {
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, [cliPath, dbPath]);
		let stdout = '';
		child.stdout.on('data', (chunk) => {
			stdout += chunk.toString();
		});
		child.on('error', reject);
		child.on('close', (code) => resolve({ code, stdout }));
		child.stdin.end(input);
	});
}

describe('REPL piped stdin', () => {
	it('runs every piped command in order from a single stdin write', async () => {
		const dbPath = generateDBPath();
		const { code, stdout } = await runCLI(
			dbPath,
			'y\nput a 1\nput b 2\nput c 3\nget a\nget b\nget c\nexit\n'
		);
		expect(code).toBe(0);

		// Before the fix, only the first buffered command ("y", answering the "create it?"
		// prompt) ran — every put/get after it was silently dropped.
		const getA = stdout.indexOf('\n1\n');
		const getB = stdout.indexOf('\n2\n', getA + 1);
		const getC = stdout.indexOf('\n3\n', getB + 1);
		expect(getA).toBeGreaterThan(-1);
		expect(getB).toBeGreaterThan(getA);
		expect(getC).toBeGreaterThan(getB);

		const db = RocksDatabase.open(dbPath, { readOnly: true });
		try {
			expect(db.getSync('a')).toBe('1');
			expect(db.getSync('b')).toBe('2');
			expect(db.getSync('c')).toBe('3');
		} finally {
			db.close();
		}
	});

	it('resolves a nested confirmation prompt whose answer is buffered in the same write', async () => {
		const dbPath = generateDBPath();
		const { code, stdout } = await runCLI(dbPath, 'y\nput a 1\nclear\ny\nget a\nexit\n');
		expect(code).toBe(0);
		expect(stdout).toContain('Are you sure you want to clear all data?');
		expect(stdout).toContain('Cleared');
		// If the nested prompt's buffered "y" had been dropped (the pre-fix bug) or
		// misrouted, either clear would never run or "get a" would run before it.
		expect(stdout).toContain('Key not found');

		const db = RocksDatabase.open(dbPath, { readOnly: true });
		try {
			expect(db.getSync('a')).toBeUndefined();
		} finally {
			db.close();
		}
	});

	it('exits cleanly on EOF after the piped commands are drained', async () => {
		const dbPath = generateDBPath();
		const { code, stdout } = await runCLI(dbPath, 'y\nput a 1\n');
		expect(code).toBe(0);
		expect(stdout).not.toContain('Unhandled');

		const db = RocksDatabase.open(dbPath, { readOnly: true });
		try {
			expect(db.getSync('a')).toBe('1');
		} finally {
			db.close();
		}
	});
});
