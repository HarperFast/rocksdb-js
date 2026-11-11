import { forceTryAgainForTesting } from '../src/load-binding.ts';
import { dbRunner, generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

describe('commitSync() shares the async commit sequence', () => {
	it('takes the TryAgain reset path, writing its log batch once', () =>
		dbRunner(async ({ db }) => {
			const log = db.useLog('repl');
			const payload = Buffer.alloc(32, 'z');
			const txnIds = new Set<number>();
			const attempts: number[] = [];
			forceTryAgainForTesting(1);
			try {
				db.transactionSync(
					(txn, attempt) => {
						attempts.push(attempt as number);
						txnIds.add(txn.id);
						if (attempt === 2) {
							expect(db.getSync('k')).toBeUndefined();
							expect([...log.query({ start: 0 })]).toEqual([]);
							expect(txn.logWritten()).toBe(true);
						}
						txn.putSync('k', 'committed');
						log.addEntry(payload, txn.id);
					},
					{ retryOnBusy: true, maxRetries: 5 }
				);
			} finally {
				forceTryAgainForTesting(0);
			}
			expect(attempts).toEqual([1, 2]);
			expect(txnIds.size).toBe(1);
			expect(db.getSync('k')).toBe('committed');
			expect(log.getStats().totals.entriesWritten).toBe(1);
			expect([...log.query({ start: 0 })].map((entry) => Buffer.from(entry.data))).toEqual([
				payload,
			]);
		}));

	it.skipIf(!!process.versions.deno)(
		'holds a foreign shutdown() until the commit finishes',
		{ timeout: 30_000 },
		async () => {
			const result = await new Promise<{
				code: number | null;
				signal: NodeJS.Signals | null;
				stderr: string;
			}>((resolve, reject) => {
				const child = spawn(
					process.execPath,
					[join(__dirname, 'fixtures', 'fork-commit-sync-shutdown.mts'), generateDBPath()],
					{
						env: {
							...process.env,
							ROCKSDB_JS_COMMIT_EXECUTE_DELAY_MS: '1000',
							// Holds the commit's own close() open after it leaves the closables, which
							// shutdown() must also wait out.
							ROCKSDB_JS_TXN_CLOSE_DELAY_MS: '500',
						},
					}
				);
				const timer = setTimeout(() => child.kill(), 20_000);
				let stderr = '';
				child.stderr?.on('data', (chunk) => (stderr += chunk.toString()));
				child.on('close', (code, signal) => {
					clearTimeout(timer);
					resolve({ code, signal, stderr });
				});
				child.on('error', reject);
			});
			expect(result).toEqual({ code: 0, signal: null, stderr: '' });
		}
	);
});
