import { describe, expect, it } from 'vitest';
import { dbRunner } from './lib/util.js';
import { mkdir, writeFile } from 'node:fs/promises';

describe('Transaction Log', () => {
	it('should detect existing transaction logs', () => dbRunner({
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

		// const fooLog2 = db.useLog('foo');
		// expect(fooLog2).toBe(fooLog);

		expect(db.listLogs()).toEqual(['bar', 'foo']);
	}));
});