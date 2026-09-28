import { RocksDatabase } from '../src/index.ts';
import { generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { closeSync, existsSync, openSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

// Piped stdin routinely delivers several commands in one chunk; readline parses every
// line in that chunk synchronously into 'line' events. These tests spawn the real CLI
// and write the whole script in ONE stdin.write()/stdin.end() call (matching
// `printf '...' | node bin/rocksdb-js.mjs`) — a per-line write would not exercise the
// same code path, since each line would arrive as its own chunk.
const cliPath = join(import.meta.dirname, '..', 'bin', 'rocksdb-js.mjs');
// bin/rocksdb-js.mjs imports from ../dist; vitest itself imports src directly (see
// AGENTS.md), so these child-process tests additionally need a build (CI builds before
// testing — see test/steady-clock.test.ts for the same guard).
const distBuilt = existsSync(join(import.meta.dirname, '..', 'dist', 'index.mjs'));

type CLIResult = { code: number | null; stdout: string; stderr: string };

function runCLI(dbPath: string, input: string): Promise<CLIResult> {
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, [cliPath, dbPath]);
		let stdout = '';
		let stderr = '';
		child.stdout.on('data', (chunk) => {
			stdout += chunk.toString();
		});
		child.stderr.on('data', (chunk) => {
			stderr += chunk.toString();
		});
		child.on('error', reject);
		child.on('close', (code) => resolve({ code, stdout, stderr }));
		child.stdin.end(input);
	});
}

// Exercises stdin as a real file (not a pipe/socket), where readline's `close` can race
// ahead of an already-buffered line's consumer — a pipe-backed stdin doesn't hit this.
function runCLIFromFile(dbPath: string, scriptPath: string): Promise<CLIResult> {
	return new Promise((resolve, reject) => {
		const fd = openSync(scriptPath, 'r');
		const child = spawn(process.execPath, [cliPath, dbPath], { stdio: [fd, 'pipe', 'pipe'] });
		let stdout = '';
		let stderr = '';
		child.stdout?.on('data', (chunk) => {
			stdout += chunk.toString();
		});
		child.stderr?.on('data', (chunk) => {
			stderr += chunk.toString();
		});
		child.on('error', (err) => {
			closeSync(fd);
			reject(err);
		});
		child.on('close', (code) => {
			closeSync(fd);
			resolve({ code, stdout, stderr });
		});
	});
}

describe.skipIf(!distBuilt)('REPL piped stdin', () => {
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

	it('drains every buffered line from file-redirected stdin with no trailing newline', async () => {
		const dbPath = generateDBPath();
		const scriptPath = join(dbPath + '-script.txt');
		// No trailing "\n" after the last command: readline emits the final partial line
		// and closes in the same pass, which previously raced ask()'s rl.pause() call.
		writeFileSync(scriptPath, 'y\nput a 1\nput b 2\nput c 3\nget a\nget b\nget c');

		const { code, stdout, stderr } = await runCLIFromFile(dbPath, scriptPath);
		expect(stderr).toBe('');
		expect(code).toBe(0);

		const getA = stdout.indexOf('\n1\n');
		const getB = stdout.indexOf('\n2\n', getA + 1);
		const getC = stdout.indexOf('\n3\n', getB + 1);
		expect(getA).toBeGreaterThan(-1);
		expect(getB).toBeGreaterThan(getA);
		expect(getC).toBeGreaterThan(getB);
	});

	it('exits rather than reinterpret queued input as CLI commands when "repl" is refused', async () => {
		const dbPath = generateDBPath();
		const { code, stdout } = await runCLI(dbPath, 'y\nput a 1\nrepl\nclear\ny\n');
		expect(code).not.toBe(0);
		expect(stdout).toContain('Cannot open the JS sub-REPL with piped input still queued.');
		// The queued "clear"/"y" must never run as CLI commands after the refusal.
		expect(stdout).not.toContain('Cleared');

		const db = RocksDatabase.open(dbPath, { readOnly: true });
		try {
			expect(db.getSync('a')).toBe('1');
		} finally {
			db.close();
		}
	});
});
