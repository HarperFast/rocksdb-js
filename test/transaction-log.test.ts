import { assert, describe, expect, it } from 'vitest';
import { dbRunner } from './lib/util.js';
import { mkdir, writeFile } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import type { NativeTransactionLog } from '../src/load-binding.js';
import { existsSync } from 'node:fs';

describe('Transaction Log', () => {
	it('should detect existing transaction logs', () => dbRunner({
		skipOpen: true
	}, async ({ db, dbPath }) => {
		await mkdir(`${dbPath}/transaction_logs/foo`, { recursive: true });
		await writeFile(`${dbPath}/transaction_logs/foo/foo.1.txnlog`, '');

		db.open();

		expect(db.listLogs()).toEqual(['foo']);

		const fooLog = db.useLog('foo');
		expect(fooLog).toBeDefined();

		const barLog = db.useLog('bar');
		expect(barLog).toBeDefined();
		expect(barLog).not.toBe(fooLog);

		const fooLog2 = db.useLog('foo');
		expect(fooLog2).toBe(fooLog);

		expect(db.listLogs()).toEqual(['bar', 'foo']);
	}));

	(globalThis.gc ? it : it.skip)('should cleanup transaction log instance on GC', () => dbRunner(async ({ db }) => {
		let weakRef: WeakRef<NativeTransactionLog> | undefined;

		await new Promise<void>((resolve) => {
			const log = db.useLog('foo');
			weakRef = new WeakRef(log);
			resolve();
		});

		assert(weakRef);
		assert(globalThis.gc);

		// this is flaky
		const until = Date.now() + 3000;
		while (Date.now() < until) {
			globalThis.gc();
			await delay(250);
			globalThis.gc();
			await delay(250);
			if (!weakRef.deref()) {
				break;
			}
		}

		expect(weakRef.deref()).toBeUndefined();
	}), 10000);

	it('should rotate a transaction log', () => dbRunner(async ({ db, dbPath }) => {
		const log = db.useLog('foo');

		const entries = (16 * 1024 * 1024) / 4096;
		const value = Buffer.alloc(4096);
		for (let i = 0; i < entries; i++) {
			log.addEntry(Date.now(), value);
		}

		expect(existsSync(`${dbPath}/transaction_logs/foo/foo.1.txnlog`)).toBe(true);
		expect(existsSync(`${dbPath}/transaction_logs/foo/foo.2.txnlog`)).toBe(false);
		log.addEntry(Date.now(), value);
		expect(existsSync(`${dbPath}/transaction_logs/foo/foo.2.txnlog`)).toBe(true);
	}));

	it('should queue all entries for a transaction', () => dbRunner(async ({ db, dbPath }) => {
		// TODO
	}));

	it('should purge all transaction log files', () => dbRunner({
		skipOpen: true
	}, async ({ db, dbPath }) => {
		const logDirectory = `${dbPath}/transaction_logs/foo`;
		const logFile = `${logDirectory}/foo.1.txnlog`;
		await mkdir(`${dbPath}/transaction_logs/foo`, { recursive: true });
		await writeFile(logFile, '');

		db.open();
		expect(db.listLogs()).toEqual(['foo']);
		expect(existsSync(logFile)).toBe(true);
		expect(db.purgeLogs({ all: true })).toEqual([logFile]);
		expect(existsSync(logDirectory)).toBe(false);
	}));

	it('should purge a specific transaction log file', () => dbRunner({
		skipOpen: true
	}, async ({ db, dbPath }) => {
		const fooLogDirectory = `${dbPath}/transaction_logs/foo`;
		const fooLogFile = `${fooLogDirectory}/foo.1.txnlog`;
		await mkdir(`${dbPath}/transaction_logs/foo`, { recursive: true });
		await writeFile(fooLogFile, '');

		const barLogDirectory = `${dbPath}/transaction_logs/bar`;
		const barLogFile = `${barLogDirectory}/bar.1.txnlog`;
		await mkdir(`${dbPath}/transaction_logs/bar`, { recursive: true });
		await writeFile(barLogFile, '');

		db.open();
		expect(db.listLogs().sort()).toEqual(['bar', 'foo']);
		expect(existsSync(fooLogFile)).toBe(true);
		expect(existsSync(barLogFile)).toBe(true);
		expect(db.purgeLogs({ all: true, name: 'foo' })).toEqual([fooLogFile]);
		expect(existsSync(fooLogFile)).toBe(false);
		expect(existsSync(fooLogDirectory)).toBe(false);
		expect(existsSync(barLogFile)).toBe(true);
		expect(existsSync(barLogDirectory)).toBe(true);
	}));
});
