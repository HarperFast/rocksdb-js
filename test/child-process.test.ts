import { runChild } from './lib/child-process.ts';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

const probePath = join(__dirname, 'fixtures', 'run-child-probe.mts');

function isAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch {
		return false;
	}
}

describe('runChild()', () => {
	it('resolves with the exit code and both streams of a child that exits', async () => {
		const result = await runChild(process.execPath, [probePath, 'exit'], { deadlineMs: 20_000 });
		expect(result.timedOut, result.stderr).toBe(false);
		expect(result.code, result.stderr).toBe(3);
		expect(result.signal).toBeNull();
		expect(result.stdout).toContain('probe stdout');
		expect(result.stderr).toContain('probe stderr');
	});

	it('kills a child past its deadline and settles only once it is gone', async () => {
		const result = await runChild(process.execPath, [probePath, 'hang'], { deadlineMs: 2_000 });
		expect(result.timedOut).toBe(true);
		expect(result.code).not.toBe(0);
		expect(result.stderr).toContain('probe stderr');
		expect(isAlive(result.pid)).toBe(false);
	});

	it('kills a child once stopWhen matches and settles only once it is gone', async () => {
		const result = await runChild(process.execPath, [probePath, 'hang'], {
			deadlineMs: 20_000,
			stopWhen: ({ stdout }) => stdout.includes('READY'),
		});
		expect(result.timedOut, result.stderr).toBe(false);
		expect(result.stdout).toContain('READY');
		expect(isAlive(result.pid)).toBe(false);
	});

	it('rejects when the command cannot be spawned', async () => {
		await expect(
			runChild(join(__dirname, 'fixtures', 'does-not-exist'), [], { deadlineMs: 20_000 })
		).rejects.toThrow(/ENOENT/);
	});
});
