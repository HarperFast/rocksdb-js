import { RocksDatabase } from '../src/index.ts';
import { generateDBPath } from './lib/util.ts';
import { type ChildProcess, spawn } from 'node:child_process';
import { once } from 'node:events';
import { existsSync, readFileSync, rmSync, utimesSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

const cliPath = join(import.meta.dirname, '..', 'bin', 'rocksdb-js.mjs');
const distBuilt = existsSync(join(import.meta.dirname, '..', 'dist', 'index.mjs'));
const CLI_TIMEOUT_MS = 20_000;

type CLIResult = { code: number | null; stdout: string; stderr: string };

function cli(args: string[], options: { cwd?: string; input?: string } = {}): Promise<CLIResult> {
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, [cliPath, ...args], {
			cwd: options.cwd,
			env: { ...process.env, NO_COLOR: '1' },
			stdio: [options.input === undefined ? 'ignore' : 'pipe', 'pipe', 'pipe'],
		});
		let stdout = '';
		let stderr = '';
		const timer = setTimeout(() => {
			child.kill('SIGKILL');
			reject(new Error(`CLI timed out: ${args.join(' ')}\n${stdout}\n${stderr}`));
		}, CLI_TIMEOUT_MS);
		child.stdout?.on('data', (chunk) => {
			stdout += chunk.toString();
		});
		child.stderr?.on('data', (chunk) => {
			stderr += chunk.toString();
		});
		if (options.input !== undefined) child.stdin?.end(options.input);
		child.on('error', (err) => {
			clearTimeout(timer);
			reject(err);
		});
		child.on('close', (code) => {
			clearTimeout(timer);
			resolve({ code, stdout, stderr });
		});
	});
}

const tempPaths: string[] = [];
// Children a test spawns itself; reaped before their databases are removed.
const liveChildren = new Set<ChildProcess>();

function tempDBPath(): string {
	const path = generateDBPath();
	tempPaths.push(path);
	return path;
}

function seed(dbPath: string, entries: Record<string, unknown>, name?: string) {
	const db = RocksDatabase.open(dbPath, name ? { name } : {});
	try {
		for (const [key, value] of Object.entries(entries)) db.putSync(key, value);
	} finally {
		db.close();
	}
}

describe.skipIf(!distBuilt || Boolean(process.versions.deno || process.versions.bun))(
	'CLI one-shot commands',
	() => {
		afterEach(async () => {
			for (const child of liveChildren) {
				if (child.exitCode === null && child.signalCode === null) {
					const closed = once(child, 'close');
					child.kill('SIGKILL');
					await closed;
				}
			}
			liveChildren.clear();
			for (const path of tempPaths) rmSync(path, { force: true, recursive: true, maxRetries: 3 });
			tempPaths.length = 0;
		});

		it('requires a command for one-shot-only options, but starts the REPL without them', async () => {
			const dbPath = tempDBPath();
			seed(dbPath, { a: 'one' });

			for (const options of [['-c', 'foo'], ['--json'], ['--yes']]) {
				const result = await cli([dbPath, ...options]);
				expect(result.code).toBe(2);
				expect(result.stdout).toBe('');
				expect(result.stderr).toContain('Error: specify a command');
				expect(result.stderr).toContain('rocksdb-js [dbpath] <command>');
			}

			const repl = await cli([dbPath], { input: 'get a\nexit\n' });
			expect(repl.code).toBe(0);
			expect(repl.stdout).toContain('Type "help" for more information.');
			expect(repl.stdout).toContain('\none\n');
		});

		it('documents every command and option in --help', async () => {
			const { code, stdout } = await cli(['--help']);
			expect(code).toBe(0);
			for (const text of [
				'rocksdb-js [dbpath] <command>',
				'--json',
				'--yes',
				'--column',
				'--readonly',
				'verify-logs [name]',
				'query [start] [end]',
				'backups <dir> [subcommand] [arg]',
				'repl',
				'(REPL only)',
			]) {
				expect(stdout).toContain(text);
			}
		});

		it('prints results to stdout and runs read commands read-only alongside a writer', async () => {
			const dbPath = tempDBPath();
			seed(dbPath, { a: 'one', b: { x: 1 }, c: 3, d: { type: 'Buffer', data: [65, 66] } });
			// Holding the database open read-write in this process takes RocksDB's LOCK, so
			// only a read-only open can succeed in the child.
			const writer = RocksDatabase.open(dbPath);
			try {
				let result = await cli([dbPath, 'get', 'a']);
				expect(result).toMatchObject({ code: 0, stdout: 'one\n', stderr: '' });

				result = await cli([dbPath, 'count', '--json']);
				expect(result.code).toBe(0);
				expect(JSON.parse(result.stdout).count).toBe(4);

				result = await cli([dbPath, 'get', 'd', '--json']);
				expect(JSON.parse(result.stdout)).toEqual({ type: 'Buffer', data: [65, 66] });

				result = await cli([dbPath, 'query', 'a', 'c', '--json']);
				expect(result.code).toBe(0);
				expect(JSON.parse(result.stdout)).toEqual([
					{ key: 'a', value: 'one' },
					{ key: 'b', value: { x: 1 } },
				]);

				result = await cli([dbPath, 'put', 'd', '4', '--yes']);
				expect(result.code).toBe(1);
				expect(result.stderr).toContain('RocksDB is already open by another process');

				result = await cli([dbPath, 'put', 'd', '4', '--yes', '--json']);
				expect(JSON.parse(result.stderr)).toEqual({
					error: 'RocksDB is already open by another process',
					code: 'ERR_DATABASE_LOCKED',
				});

				const backupDir = tempDBPath();
				result = await cli([dbPath, 'backups', backupDir, 'backup']);
				expect(result.code).toBe(1);
				expect(result.stderr).toContain('RocksDB is already open by another process');
				result = await cli([dbPath, 'backups', backupDir, 'backup', '--readonly']);
				expect(result.code).toBe(0);

				result = await cli([dbPath, 'backups', backupDir, 'restore', '--yes']);
				expect(result.code).toBe(2);
				expect(result.stderr).toContain('<backup-id>');
			} finally {
				writer.close();
			}
		});

		it('writes with put and remove, and confirms destructive commands with --yes', async () => {
			const dbPath = tempDBPath();
			seed(dbPath, { a: 'one', b: 'two' });

			expect((await cli([dbPath, 'put', 'c', 'three'])).code).toBe(0);
			expect((await cli([dbPath, 'put', 'e', ''])).code).toBe(0);
			expect((await cli([dbPath, 'put', 'q', '"quoted"'])).code).toBe(0);
			expect((await cli([dbPath, 'remove', 'a'])).code).toBe(0);

			let result = await cli([dbPath, 'clear']);
			expect(result.code).toBe(1);
			expect(result.stderr).toContain('Pass --yes to confirm');

			let db = RocksDatabase.open(dbPath, { readOnly: true });
			try {
				expect(db.getSync('a')).toBeUndefined();
				expect(db.getSync('b')).toBe('two');
				expect(db.getSync('c')).toBe('three');
				expect(db.getSync('e')).toBe('');
				expect(db.getSync('q')).toBe('"quoted"');
			} finally {
				db.close();
			}

			result = await cli([dbPath, 'clear', '--yes']);
			expect(result.code).toBe(0);
			db = RocksDatabase.open(dbPath, { readOnly: true });
			try {
				expect(db.getKeysCount()).toBe(0);
			} finally {
				db.close();
			}
		});

		it('reports failures on stderr with distinct exit codes', async () => {
			const dbPath = tempDBPath();
			seed(dbPath, { a: 'one' });

			let result = await cli([dbPath, 'get', 'missing']);
			expect(result).toMatchObject({ code: 1, stdout: '' });
			expect(result.stderr).toContain('Key not found');

			result = await cli([dbPath, 'get', 'missing', '--json']);
			expect(result.code).toBe(1);
			expect(JSON.parse(result.stderr)).toEqual({ error: 'Key not found' });

			result = await cli([dbPath, 'get']);
			expect(result.code).toBe(2);
			expect(result.stderr).toContain('Usage: get <key>');

			result = await cli([dbPath, 'frobnicate']);
			expect(result.code).toBe(2);
			expect(result.stderr).toContain('Unknown command: frobnicate');

			result = await cli([dbPath, 'repl']);
			expect(result.code).toBe(2);
			expect(result.stderr).toContain('only available in the REPL');

			result = await cli([dbPath, 'put', 'a', 'two', '--readonly']);
			expect(result.code).toBe(2);
			expect(result.stderr).toContain('not available in read-only mode');

			result = await cli(['--no-such-option', '--json']);
			expect(result.code).toBe(2);
			expect(JSON.parse(result.stderr).error).toContain('--no-such-option');
		});

		it('does not create a missing database for a read command, nor without --yes', async () => {
			const dbPath = tempDBPath();

			let result = await cli([dbPath, 'count']);
			expect(result.code).toBe(1);
			expect(result.stderr).toContain('does not exist');
			expect(existsSync(dbPath)).toBe(false);

			result = await cli([dbPath, 'put', 'a', '1']);
			expect(result.code).toBe(1);
			expect(existsSync(dbPath)).toBe(false);

			result = await cli([dbPath, 'clear', '--yes']);
			expect(result.code).toBe(1);
			expect(existsSync(dbPath)).toBe(false);

			result = await cli([dbPath, 'put', 'a', '1', '--yes']);
			expect(result.code).toBe(0);
			expect((await cli([dbPath, 'get', 'a'])).stdout).toBe('1\n');

			const emptyPath = tempDBPath();
			result = await cli([emptyPath, 'put', '--yes']);
			expect(result.code).toBe(2);
			expect(result.stderr).toContain('Usage: put <key> <value>');
			expect(existsSync(emptyPath)).toBe(false);

			result = await cli([dbPath, '-c', 'typo', 'clear', '--yes']);
			expect(result.code).toBe(1);
			expect(result.stderr).toContain('Column family "typo" does not exist');
			expect(JSON.parse((await cli([dbPath, 'columns', '--json'])).stdout)).toEqual(['default']);
		});

		it('fails a query whose rows cannot all be read, but still prints the readable ones', async () => {
			const dbPath = tempDBPath();
			seed(dbPath, { a: 'one', c: 'three' });
			const raw = RocksDatabase.open(dbPath, { encoding: 'binary' });
			try {
				// A str8 header promising 5 bytes that are not there.
				raw.putSync('b', Buffer.from([0xd9, 0x05]));
			} finally {
				raw.close();
			}

			let result = await cli([dbPath, 'query']);
			expect(result.code).toBe(1);
			expect(result.stdout).toContain("a = 'one'");
			expect(result.stdout).toContain("c = 'three'");
			expect(result.stderr).toContain('b = ');

			result = await cli([dbPath, 'query', '--json']);
			expect(result.code).toBe(1);
			const rows = JSON.parse(result.stdout);
			expect(rows.map((row: { key: string }) => row.key)).toEqual(['a', 'b', 'c']);
			expect(rows[1].error).toBeTypeOf('string');
			expect(JSON.parse(result.stderr).error).toBe('1 row(s) could not be read');
		});

		it('reports unreadable first, adjacent and last rows of a query in place', async () => {
			const dbPath = tempDBPath();
			seed(dbPath, { b: 'two', e: 'five' });
			const raw = RocksDatabase.open(dbPath, { encoding: 'binary' });
			try {
				for (const key of ['a', 'c', 'd', 'f']) raw.putSync(key, Buffer.from([0xd9, 0x05]));
			} finally {
				raw.close();
			}

			let result = await cli([dbPath, 'query', '--json']);
			expect(result.code).toBe(1);
			const rows = JSON.parse(result.stdout);
			expect(rows.map((row: { key: string }) => row.key)).toEqual(['a', 'b', 'c', 'd', 'e', 'f']);
			expect(rows.map((row: { error?: string }) => row.error === undefined)).toEqual([
				false,
				true,
				false,
				false,
				true,
				false,
			]);
			expect(rows[1].value).toBe('two');
			expect(rows[4].value).toBe('five');
			expect(JSON.parse(result.stderr).error).toBe('4 row(s) could not be read');

			result = await cli([dbPath, 'query', 'b', 'e', '--json']);
			expect(result.code).toBe(1);
			expect(JSON.parse(result.stdout).map((row: { key: string }) => row.key)).toEqual([
				'b',
				'c',
				'd',
			]);
		});

		it('exits cleanly when stdout closes before a query finishes', async () => {
			const dbPath = tempDBPath();
			const db = RocksDatabase.open(dbPath);
			try {
				for (let i = 0; i < 20_000; i++) db.putSync(`key${i}`, 'x'.repeat(100));
			} finally {
				db.close();
			}

			const child = spawn(process.execPath, [cliPath, dbPath, 'query'], {
				env: { ...process.env, NO_COLOR: '1' },
				stdio: ['ignore', 'pipe', 'pipe'],
			});
			liveChildren.add(child);
			let stderr = '';
			child.stderr.on('data', (chunk) => {
				stderr += chunk.toString();
			});
			child.stdout.once('data', () => child.stdout.destroy());
			let timedOut = false;
			const timer = setTimeout(() => {
				timedOut = true;
				child.kill('SIGKILL');
			}, CLI_TIMEOUT_MS);
			const code = await new Promise((resolve) => child.on('close', resolve));
			clearTimeout(timer);
			expect(timedOut, `CLI timed out after stdout closed\n${stderr}`).toBe(false);
			expect(stderr).toBe('');
			expect(code).toBe(0);
		});

		it('uses the current directory when the first argument is a command', async () => {
			const dbPath = tempDBPath();
			seed(dbPath, { a: 'one' });
			const result = await cli(['get', 'a'], { cwd: dbPath });
			expect(result).toMatchObject({ code: 0, stdout: 'one\n' });
		});

		it('rejects blank paths and required arguments instead of falling back to a default', async () => {
			const dbPath = tempDBPath();
			seed(dbPath, { a: 'one' });

			for (const args of [['', 'clear', '--yes'], [' ', 'clear', '--yes'], ['']]) {
				const result = await cli(args, { cwd: dbPath });
				expect(result.code).toBe(2);
				expect(result.stderr).toContain('database path must not be empty');
			}
			for (const args of [
				['purge-logs', '', '--yes'],
				['drop', ' '],
				['remove', ''],
				['get', ''],
			]) {
				const result = await cli([dbPath, ...args]);
				expect(result.code).toBe(2);
				expect(result.stderr).toContain('Usage:');
			}
			expect((await cli(['get', 'a'], { cwd: dbPath })).stdout).toBe('one\n');
		});

		it('selects a column family with --column', async () => {
			const dbPath = tempDBPath();
			seed(dbPath, { a: 'default-a' });
			seed(dbPath, { a: 'users-a' }, 'users');

			expect((await cli([dbPath, '-c', 'users', 'get', 'a'])).stdout).toBe('users-a\n');
			expect((await cli([dbPath, 'get', 'a'])).stdout).toBe('default-a\n');

			for (const args of [
				['-c', '', 'clear', '--yes'],
				['--column=', 'clear', '--yes'],
				['--column', ' ', 'clear', '--yes'],
				['-c', ''],
			]) {
				const empty = await cli([dbPath, ...args]);
				expect(empty.code).toBe(2);
				expect(empty.stderr).toContain('--column needs a column family name');
			}
			expect((await cli([dbPath, 'get', 'a'])).stdout).toBe('default-a\n');

			const result = await cli([dbPath, '--column', 'nope', 'count']);
			expect(result.code).toBe(1);
			expect(result.stderr).toContain('Column family "nope" does not exist');

			const columns = await cli([dbPath, 'columns', '--json']);
			expect(JSON.parse(columns.stdout)).toEqual(['default', 'users']);
		});

		it('exits non-zero from verify-logs when a log store is invalid', async () => {
			const dbPath = tempDBPath();
			const db = RocksDatabase.open(dbPath);
			try {
				const log = db.useLog('events');
				await db.transaction(async (txn) => {
					log.addEntry(Buffer.from('entry'), txn.id);
				});
			} finally {
				db.close();
			}

			let result = await cli([dbPath, 'verify-logs', '--json']);
			expect(result.code).toBe(0);
			expect(JSON.parse(result.stdout)).toMatchObject({ valid: true, invalidStores: 0 });

			const logFile = join(dbPath, 'transaction_logs', 'events', '1.txnlog');
			const bytes = readFileSync(logFile);
			bytes.writeUInt32BE(0xdeadbeef, 0);
			writeFileSync(logFile, bytes);

			result = await cli([dbPath, 'verify-logs', 'events']);
			expect(result.code).toBe(1);
			expect(result.stdout).toContain('INVALID');
		});

		it('creates, lists and verifies backups without opening the database for list/verify', async () => {
			const dbPath = tempDBPath();
			const backupDir = tempDBPath();
			seed(dbPath, { a: 'one' });

			let result = await cli([dbPath, 'backups', backupDir, 'backup', '--json']);
			expect(result.code).toBe(0);
			expect(JSON.parse(result.stdout)).toMatchObject({ subcommand: 'backup', backupId: 1 });

			result = await cli(['backups', backupDir, '--json'], { cwd: backupDir });
			expect(result.code).toBe(0);
			expect(JSON.parse(result.stdout).backups).toHaveLength(1);

			result = await cli(['backups', backupDir, 'verify', '1'], { cwd: backupDir });
			expect(result.code).toBe(0);
			expect(result.stdout).toContain('verified OK');

			for (const args of [
				...['', ' ', '-1', '1.5', '4294967296'].map((n) => [backupDir, 'purge', n]),
				[backupDir, 'verify', '4294967296'],
				['', 'purge', '0'],
				[' ', 'list'],
			]) {
				result = await cli(['backups', ...args], { cwd: backupDir });
				expect(result.code).toBe(2);
			}
			result = await cli(['backups', backupDir, '--json'], { cwd: backupDir });
			expect(JSON.parse(result.stdout).backups).toHaveLength(1);

			result = await cli(['backups', backupDir, 'verify', '9'], { cwd: backupDir });
			expect(result.code).toBe(1);
			expect(result.stderr).toContain('Backup 9 does not exist');
		});

		it('restores a backup, but never under a live writer', async () => {
			const dbPath = tempDBPath();
			const backupDir = tempDBPath();
			const freshPath = tempDBPath();
			seed(dbPath, { a: 'one' });
			expect((await cli([dbPath, 'backups', backupDir, 'backup'])).code).toBe(0);
			seed(dbPath, { a: 'changed' });

			let result = await cli([dbPath, 'backups', backupDir, 'restore', '1']);
			expect(result.code).toBe(1);
			expect(result.stderr).toContain('Pass --yes to confirm');

			result = await cli([dbPath, 'backups', backupDir, 'restore', '1', '--yes']);
			expect(result.code).toBe(0);
			expect((await cli([dbPath, 'get', 'a'])).stdout).toBe('one\n');

			result = await cli([freshPath, 'backups', backupDir, 'restore', '1', '--yes']);
			expect(result.code).toBe(0);
			expect((await cli([freshPath, 'get', 'a'])).stdout).toBe('one\n');

			const writer = RocksDatabase.open(dbPath);
			try {
				writer.putSync('a', 'live');
				result = await cli([dbPath, 'backups', backupDir, 'restore', '1', '--yes']);
				expect(result.code).toBe(1);
				expect(result.stderr).toContain('RocksDB is already open by another process');
				expect(writer.getSync('a')).toBe('live');
			} finally {
				writer.close();
			}

			result = await cli([dbPath, 'backups', backupDir, 'restore', '1', '--readonly', '--yes']);
			expect(result.code).toBe(2);
		});

		it('does not purge aged transaction logs when opening for a backup or restore', async () => {
			const dbPath = tempDBPath();
			const backupDir = tempDBPath();
			const db = RocksDatabase.open(dbPath, { transactionLogMaxSize: 400 });
			try {
				const log = db.useLog('events');
				for (let i = 0; i < 4; i++) {
					await db.transaction(async (txn) => {
						log.addEntry(Buffer.alloc(200, i), txn.id);
						db.putSync(`k${i}`, i, { transaction: txn });
					});
				}
			} finally {
				db.close();
			}
			const segment = join(dbPath, 'transaction_logs', 'events', '1.txnlog');
			expect(existsSync(join(dbPath, 'transaction_logs', 'events', '2.txnlog'))).toBe(true);
			const aged = new Date(Date.now() - 10 * 24 * 60 * 60 * 1000);
			utimesSync(segment, aged, aged);

			expect((await cli([dbPath, 'backups', backupDir, 'backup'])).code).toBe(0);
			expect(existsSync(segment)).toBe(true);

			const result = await cli([dbPath, 'backups', backupDir, 'restore', '1']);
			expect(result.code).toBe(1);
			expect(result.stderr).toContain('Pass --yes to confirm');
			expect(existsSync(segment)).toBe(true);

			expect((await cli([dbPath, 'put', 'x', 'y'])).code).toBe(0);
			expect(existsSync(segment)).toBe(false);
		});
	}
);
