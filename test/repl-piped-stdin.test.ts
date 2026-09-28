import { RocksDatabase } from '../src/index.ts';
import { generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { closeSync, existsSync, openSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

// Each script below is sent in ONE stdin.write()/stdin.end() call (matching
// `printf '...' | node bin/rocksdb-js.mjs`) — a per-line write would not exercise the
// same code path, since each line would then arrive as its own chunk.
const cliPath = join(import.meta.dirname, '..', 'bin', 'rocksdb-js.mjs');
// bin/rocksdb-js.mjs imports from ../dist; see test/steady-clock.test.ts for the same
// build-required guard.
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

// A real file, not a pipe/socket: readline's `close` behaves differently here.
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

function cleanup(...paths: string[]) {
	for (const path of paths) rmSync(path, { force: true, recursive: true, maxRetries: 3 });
}

describe.skipIf(!distBuilt)('REPL piped stdin', () => {
	it('runs every piped command in order from a single stdin write', async () => {
		const dbPath = generateDBPath();
		try {
			const { code, stdout } = await runCLI(
				dbPath,
				'y\nput a 1\nput b 2\nput c 3\nget a\nget b\nget c\nexit\n'
			);
			expect(code).toBe(0);

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
		} finally {
			cleanup(dbPath);
		}
	});

	it('resolves a nested confirmation prompt whose answer is buffered in the same write', async () => {
		const dbPath = generateDBPath();
		try {
			const { code, stdout } = await runCLI(dbPath, 'y\nput a 1\nclear\ny\nget a\nexit\n');
			expect(code).toBe(0);
			expect(stdout).toContain('Are you sure you want to clear all data?');
			expect(stdout).toContain('Cleared');
			expect(stdout).toContain('Key not found');

			const db = RocksDatabase.open(dbPath, { readOnly: true });
			try {
				expect(db.getSync('a')).toBeUndefined();
			} finally {
				db.close();
			}
		} finally {
			cleanup(dbPath);
		}
	});

	it('exits cleanly on EOF after the piped commands are drained', async () => {
		const dbPath = generateDBPath();
		try {
			const { code, stdout } = await runCLI(dbPath, 'y\nput a 1\n');
			expect(code).toBe(0);
			expect(stdout).not.toContain('Unhandled');

			const db = RocksDatabase.open(dbPath, { readOnly: true });
			try {
				expect(db.getSync('a')).toBe('1');
			} finally {
				db.close();
			}
		} finally {
			cleanup(dbPath);
		}
	});

	it('drains every buffered line from file-redirected stdin with no trailing newline', async () => {
		const dbPath = generateDBPath();
		const scriptPath = `${dbPath}-script.txt`;
		try {
			// No trailing "\n": readline emits the final partial line and closes in the
			// same pass.
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
		} finally {
			cleanup(dbPath, scriptPath);
		}
	});

	it('exits non-zero rather than reinterpret queued input as CLI commands when "repl" is refused', async () => {
		const dbPath = generateDBPath();
		try {
			const { code, stdout } = await runCLI(dbPath, 'y\nput a 1\nrepl\nclear\ny\n');
			expect(code).toBe(1);
			expect(stdout).toContain('needs an interactive terminal');
			expect(stdout).not.toContain('Cleared');

			const db = RocksDatabase.open(dbPath, { readOnly: true });
			try {
				expect(db.getSync('a')).toBe('1');
			} finally {
				db.close();
			}
		} finally {
			cleanup(dbPath);
		}
	});

	it('refuses "repl" even when the trailing line after it has no newline yet', async () => {
		const dbPath = generateDBPath();
		const scriptPath = `${dbPath}-script.txt`;
		try {
			// No trailing "\n": "put b 2" sits in readline's internal buffer, unemitted,
			// when replCommand runs — a count of emitted lines alone would miss it.
			writeFileSync(scriptPath, 'y\nput a 1\nrepl\nput b 2');

			const { code, stdout } = await runCLIFromFile(dbPath, scriptPath);
			expect(code).toBe(1);
			expect(stdout).toContain('needs an interactive terminal');

			const db = RocksDatabase.open(dbPath, { readOnly: true });
			try {
				expect(db.getSync('b')).toBeUndefined();
			} finally {
				db.close();
			}
		} finally {
			cleanup(dbPath, scriptPath);
		}
	});
});
