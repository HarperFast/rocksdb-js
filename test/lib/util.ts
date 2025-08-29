import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { randomBytes } from 'node:crypto';
import { RocksDatabase, type RocksDatabaseOptions } from '../../src/index.js';
import { rimraf } from 'rimraf';
import { setTimeout as delay } from 'node:timers/promises';

export function generateDBPath() {
	return join(
		tmpdir(),
		`testdb-${randomBytes(8).toString('hex')}`
	);
}

type TestDB = {
	db: RocksDatabase;
	dbPath: string;
};

type TestOptions = {
	dbOptions?: (RocksDatabaseOptions & { path?: string })[];
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
) {
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
	const databases: TestDB[] = [];

	try {
		for (let i = 0; i < testFn.length; i++) {
			const path = options.dbOptions?.[i]?.path ?? dbPath;
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

		for (let i = 0; i < 3; i++) {
			try {
				await rimraf(dbPath);
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
