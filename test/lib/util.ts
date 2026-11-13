import { getRegistryStatus, RocksDatabase, type RocksDatabaseOptions } from '../../src/index.ts';
import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { mkdirSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import type { Worker } from 'node:worker_threads';

function resolveIdentity(path: string): string {
	try {
		return realpathSync(path);
	} catch {
		return resolve(path);
	}
}

export function generateDBPath(): string {
	const testDir = join(tmpdir(), 'rocksdb-js-tests');
	mkdirSync(testDir, { recursive: true });
	return join(testDir, `testdb-${randomBytes(8).toString('hex')}`);
}

type TestDB = { db: RocksDatabase; dbPath: string };

type TestOptions = {
	dbOptions?: ((RocksDatabaseOptions & { path?: string }) | undefined)[];
	skipOpen?: boolean;
};

type TestFn = (...databases: TestDB[]) => void | Promise<void>;

/**
 * Creates one or more databases instances and runs a test function with them.
 *
 * If the test function has 3 arguments, it will create 3 database instances
 * with the SAME path, and (by default) open them. Use the `dbOptions` option
 * to specify different options for each database.
 *
 * @example
 * ```ts
 * it('should test database', () => dbRunner(async ({ db }) => {
 * 	 await db.put('foo', 'bar');
 * }));
 * ```
 *
 * @example
 * ```ts
 * it('should test multiple databases', () => dbRunner({
 * 	 dbOptions: [
 *     {}, // use defaults
 *     { path: generateDBPath(), name: 'db2' },
 *   ],
 *   skipOpen: true,
 * }, async ({ db }, { db: db2 }) => {
 *   db.open();
 *   db2.open();
 * 	 await db.put('foo', 'bar');
 * 	 await db2.put('foo', 'bar');
 * }));
 * ```
 */
export async function dbRunner(options: TestOptions | TestFn, test?: TestFn): Promise<void> {
	let testFn: TestFn;
	if (typeof options === 'function') {
		testFn = options;
		options = {};
	} else if (test) {
		testFn = test;
	} else {
		throw new Error('No test function provided');
	}

	const dbPath = generateDBPath();
	const dbPaths = new Set<string>([dbPath]);
	const databases: TestDB[] = [];
	let testError: unknown;
	let closeError: unknown;
	let testFailed = false;
	let closeFailed = false;

	try {
		for (let i = 0; i < testFn.length; i++) {
			const path = options.dbOptions?.[i]?.path ?? dbPath;
			dbPaths.add(path);
			const db = new RocksDatabase(path, options.dbOptions?.[i]);
			if (options.skipOpen !== true) {
				db.open();
			}
			databases.push({ db, dbPath: path });
		}

		await testFn(...databases);
	} catch (error) {
		testFailed = true;
		testError = error;
	} finally {
		for (const { db } of databases.reverse()) {
			try {
				db?.close();
			} catch (error) {
				if (!closeFailed) closeError = error;
				closeFailed = true;
			}
		}

		if (globalThis.gc) {
			globalThis.gc();
			await delay(100);
			// Second gc() to finalize external buffers (napi_create_external_buffer
			// C++ destructors may be deferred past the first gc cycle on some Node versions)
			globalThis.gc();
			await delay(50);
		}

		// A handle the test left open (a `use()` view, say) keeps its database registered; removing
		// the directory would fail that database's eventual flush, so keep it and fail the test.
		const registered = new Set(getRegistryStatus().map((entry) => resolveIdentity(entry.path)));
		for (const dbPath of dbPaths) {
			if (registered.has(resolveIdentity(dbPath))) {
				dbPaths.delete(dbPath);
				if (!closeFailed) {
					closeFailed = true;
					closeError = new Error(
						`Database still open after the test closed its databases: ${dbPath}`
					);
				}
			}
		}

		if (!process.env.KEEP_FILES) {
			for (const dbPath of dbPaths) {
				try {
					rmSync(dbPath, { force: true, recursive: true, maxRetries: 3, retryDelay: 500 });
				} catch (err) {
					console.error(`Error removing database: ${dbPath}: ${err}`);
				}
				dbPaths.delete(dbPath);
			}
		}
	}
	if (testFailed) throw testError;
	if (closeFailed) throw closeError;
}

/**
 * Waits for a worker thread to exit. On Deno and Bun, worker threads often fail
 * to exit after `close` messages, so we force-terminate after a short timeout.
 */
export async function terminateWorker(worker: Worker): Promise<void> {
	// check if the worker is already terminated
	if (worker.threadId === -1) {
		return;
	}

	if (process.versions.deno || process.versions.bun) {
		await new Promise<void>((resolve) => {
			const timer = setTimeout(() => {
				worker.terminate();
				resolve();
			}, 100);

			worker.on('exit', () => {
				clearTimeout(timer);
				resolve();
			});
		});
		return;
	}

	await new Promise<void>((resolve) => {
		worker.on('exit', () => resolve());
	});
}

const nodeExecutable =
	process.env.NODE_BINARY ??
	(process.versions.bun || process.versions.deno
		? (process.env.npm_node_execpath ?? 'node')
		: process.execPath);

/**
 * Runs a forked fixture, resolving on a clean exit or, with `killsItself`, only when it SIGKILLed
 * itself after printing `ready`.
 */
export function runFixture(
	fixture: string,
	dbPath: string,
	{ env, killsItself = false }: { env?: NodeJS.ProcessEnv; killsItself?: boolean } = {}
): Promise<void> {
	return new Promise((resolve, reject) => {
		const child = spawn(nodeExecutable, [fixture, dbPath], { env: { ...process.env, ...env } });
		let output = '';
		child.stdout.on('data', (chunk) => (output += chunk));
		child.stderr.on('data', (chunk) => (output += chunk));
		// Under the callers' 20 s test timeout; the rejection waits for the child to exit, so a caller's
		// cleanup does not remove the database under it.
		let timedOut = false;
		const timeout = setTimeout(() => {
			timedOut = true;
			child.kill('SIGKILL');
		}, 15_000);
		child.on('error', reject);
		child.on('close', (code, signal) => {
			clearTimeout(timeout);
			if (timedOut) {
				reject(new Error(`fixture timed out\n${output}`));
				return;
			}
			// Windows maps a self-kill to TerminateProcess(h, 1).
			const killed =
				signal === 'SIGKILL' || code === 137 || (process.platform === 'win32' && code === 1);
			if (killsItself ? killed && output.includes('ready') : code === 0 && signal === null) {
				resolve();
			} else {
				reject(new Error(`fixture exited (code=${code} signal=${signal}): ${output}`));
			}
		});
	});
}
