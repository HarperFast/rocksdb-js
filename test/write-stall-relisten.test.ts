import { generateDBPath } from './lib/util.ts';
import { spawn } from 'node:child_process';
import { rmSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const __dirname = dirname(fileURLToPath(import.meta.url));

/**
 * Regression for the writeStall debounce blocker (both cross-model reviewers): the
 * per-CF FSM must advance while unobserved, so a listener attached after a stall
 * episode still gets the rising edge on the next stall. Runs in a child process so
 * ROCKSDB_JS_WRITE_STALL_DEBOUNCE_MS=0 reaches native `::getenv` (a worker_threads
 * `process.env` write would not), making recovery clear the FSM deterministically.
 */
describe('writeStall listener re-registration', () => {
	it('re-emits after a listener detaches, the CF recovers unobserved, and one re-attaches', async () => {
		const dbPath = generateDBPath();
		const fixture = join(__dirname, 'fixtures', 'fork-write-stall-relisten.mts');
		try {
			const output = await new Promise<string>((resolve, reject) => {
				let out = '';
				const child = spawn(process.execPath, [fixture, dbPath], {
					env: { ...process.env, ROCKSDB_JS_WRITE_STALL_DEBOUNCE_MS: '0' },
				});
				child.stdout.setEncoding('utf8');
				child.stderr.setEncoding('utf8');
				child.stdout.on('data', (c) => (out += c));
				child.stderr.on('data', (c) => (out += c));
				child.on('error', reject);
				child.on('close', (code) =>
					code === 0 ? resolve(out) : reject(new Error(`fixture exited ${code}: ${out}`))
				);
			});

			const line = output.split('\n').find((l) => l.startsWith('RESULT '));
			expect(line, `no RESULT line in fixture output: ${output}`).toBeTruthy();
			const result = JSON.parse(line!.slice('RESULT '.length));

			expect(result.firstStall).toBe(true);
			// The blocker: without the fix, the FSM never advances while unobserved,
			// so the re-attached listener never sees the next stall's rising edge.
			expect(result.secondStall).toBe(true);
		} finally {
			rmSync(dbPath, { force: true, recursive: true, maxRetries: 3 });
		}
	}, 30_000);
});
