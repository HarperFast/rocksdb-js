import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { randomBytes } from 'node:crypto';
import { RocksDatabase, type RocksDatabaseOptions } from '../../src/index.js';
import { rimraf } from 'rimraf';
import { setTimeout as delay } from 'node:timers/promises';
import { mkdirSync } from 'node:fs';

export function generateDBPath(): string {
	const testDir = join(tmpdir(), 'rocksdb-js-tests');
	mkdirSync(testDir, { recursive: true });
	return join(testDir, `testdb-${randomBytes(8).toString('hex')}`);
}

type TestDB = {
	db: RocksDatabase;
	dbPath: string;
};

type TestOptions = {
	dbOptions?: (RocksDatabaseOptions & { path?: string } | undefined)[];
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
export async function dbRunner(
	options: TestOptions | TestFn,
	test?: TestFn
): Promise<void> {
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
	} finally {
		for (const { db } of databases.reverse()) {
			db?.close();
		}

		if (!process.env.KEEP_FILES) {
			const retries = 3;
			for (let i = 0; i < retries && dbPaths.size > 0; i++) {
				for (const dbPath of dbPaths) {
					try {
						await rimraf(dbPath);
						dbPaths.delete(dbPath);
						break;
					} catch (e) {
						if (e instanceof Error && 'code' in e && e.code === 'EPERM') {
							await delay(150);
							// try again, but skip after 3 attempts
						} else {
							// eslint-disable-next-line no-unsafe-finally
							throw e;
						}
					}
				}
			}
		}
	}
}

/**
 * Creates a bootstrap script to run in a worker thread.
 *
 * @returns The script to run in a worker thread.
 */
export function createWorkerBootstrapScript(path: string): string {
	if (process.versions.deno || process.versions.bun) {
		return `
			import { pathToFileURL } from 'node:url';
			import(pathToFileURL('${path}'));
			`;
	}

	const majorVersion = parseInt(process.versions.node.split('.')[0]);
	if (majorVersion < 20) {
		// Node.js 18 and older doesn't properly eval ESM code
		return `
			const tsx = require('tsx/cjs/api');
			tsx.require('${path}', __dirname);
			`;
	}

	return `
		import { register } from 'tsx/esm/api';
		register();
		import('${path}');
		`;
}
