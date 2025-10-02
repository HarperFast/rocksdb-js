import { assert, describe, expect, it } from 'vitest';
import { dbRunner } from './lib/util.js';
import { mkdir, writeFile } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import type { NativeTransactionLog } from '../src/load-binding.js';

describe('Transaction Log', () => {
	it.skip('should detect existing transaction logs', () => dbRunner({
		skipOpen: true
	}, async ({ db, dbPath }) => {
		console.log(dbPath);
		await mkdir(`${dbPath}/transaction_logs`, { recursive: true });
		await writeFile(`${dbPath}/transaction_logs/foo.1.txnlog`, '');

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

		const until = Date.now() + 3000;
		while (weakRef.deref() && Date.now() < until) {
			console.log('GCing');
			globalThis.gc();
			await delay(250);
		}

		expect(weakRef.deref()).toBeUndefined();
	}), 10000);
});
