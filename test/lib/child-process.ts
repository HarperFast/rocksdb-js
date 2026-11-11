import { spawn } from 'node:child_process';

type ChildOutput = { stdout: string; stderr: string };

export type ChildResult = ChildOutput & {
	code: number | null;
	pid: number;
	signal: NodeJS.Signals | null;
	timedOut: boolean;
};

type RunChildOptions = {
	deadlineMs: number;
	env?: NodeJS.ProcessEnv;
	stopWhen?: (output: ChildOutput) => boolean;
	closeTimeoutMs?: number;
};

/**
 * Runs a child process and settles only after its `close` event, so the
 * process is gone and its stdio drained before the caller deletes anything it
 * held open (Windows refuses to remove a directory a live process has open).
 * A child that does not close within `closeTimeoutMs` of being killed rejects
 * rather than outliving the test unobserved.
 */
export function runChild(
	command: string,
	args: string[],
	{ deadlineMs, env, stopWhen, closeTimeoutMs = 10_000 }: RunChildOptions
): Promise<ChildResult> {
	return new Promise((resolve, reject) => {
		const child = spawn(command, args, { env });
		const output: ChildOutput = { stdout: '', stderr: '' };
		let timedOut = false;
		let settled = false;
		let closeTimer: NodeJS.Timeout | undefined;
		let childError: Error | undefined;

		const settle = (error: Error | undefined, result?: ChildResult) => {
			if (settled) {
				return;
			}
			settled = true;
			clearTimeout(deadline);
			clearTimeout(closeTimer);
			if (error) {
				reject(error);
			} else {
				resolve(result!);
			}
		};

		const kill = () => {
			if (closeTimer) {
				return;
			}
			// A stopWhen kill must not be reported as a timeout if close lands after the deadline.
			clearTimeout(deadline);
			child.kill('SIGKILL');
			closeTimer = setTimeout(() => {
				settle(
					new Error(
						`Child ${child.pid} did not close within ${closeTimeoutMs}ms of SIGKILL\nstderr:\n${output.stderr}`,
						{ cause: childError }
					)
				);
			}, closeTimeoutMs);
		};

		const deadline = setTimeout(() => {
			timedOut = true;
			kill();
		}, deadlineMs);

		const onData = (stream: keyof ChildOutput) => (chunk: Buffer) => {
			output[stream] += chunk.toString();
			if (stopWhen?.(output)) {
				kill();
			}
		};
		child.stdout?.on('data', onData('stdout'));
		child.stderr?.on('data', onData('stderr'));

		child.on('error', (error) => {
			// Without a pid nothing was started, so there is no process to wait for.
			if (child.pid === undefined) {
				settle(error);
			} else {
				childError = error;
			}
		});
		child.on('close', (code, signal) => {
			settle(undefined, { ...output, code, pid: child.pid!, signal, timedOut });
		});
	});
}
