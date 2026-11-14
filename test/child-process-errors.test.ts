import { ChildCloseTimeoutError, runChild } from './lib/child-process.ts';
import { spawn } from 'node:child_process';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('node:child_process', { spy: true });

describe('runChild() errors', () => {
	beforeEach(() => vi.useFakeTimers());
	afterEach(() => {
		vi.useRealTimers();
		vi.restoreAllMocks();
	});

	function mockChild() {
		const child = Object.assign(new EventEmitter(), {
			pid: 123,
			stdout: new PassThrough(),
			stderr: new PassThrough(),
			kill: vi.fn(() => true),
		});
		vi.mocked(spawn).mockReturnValueOnce(child as unknown as ReturnType<typeof spawn>);
		return child;
	}

	it('waits for close before rejecting a post-spawn error', async () => {
		const child = mockChild();
		const error = new Error('kill failed');
		const result = runChild('probe', [], { deadlineMs: 100 });
		const rejected = expect(result).rejects.toBe(error);
		let settled = false;
		void result.then(
			() => {
				settled = true;
			},
			() => {
				settled = true;
			}
		);
		child.emit('error', error);
		await Promise.resolve();
		expect(settled).toBe(false);
		child.emit('close', 0, null);
		await rejected;
		expect(vi.getTimerCount()).toBe(0);
	});

	it.each([false, true])(
		'preserves close-timeout diagnostics when timedOut=%s',
		async (timedOut) => {
			const child = mockChild();
			const error = new Error('kill failed');
			const result = runChild('probe', [], {
				deadlineMs: 100,
				closeTimeoutMs: 200,
				stopWhen: timedOut ? undefined : ({ stdout }) => stdout.includes('READY'),
			}).catch((error: unknown) => error);
			child.stderr.emit('data', Buffer.from('probe stderr'));
			child.stdout.emit('data', Buffer.from('READY'));
			child.emit('error', error);
			await vi.advanceTimersByTimeAsync(300);
			expect(await result).toBeInstanceOf(ChildCloseTimeoutError);
			expect(await result).toMatchObject({
				message: expect.stringContaining(
					`timedOut=${timedOut}\nstdout:\nREADY\nstderr:\nprobe stderr`
				),
				cause: error,
			});
			expect(child.kill).toHaveBeenCalledExactlyOnceWith('SIGKILL');
			expect(vi.getTimerCount()).toBe(0);
		}
	);
});
