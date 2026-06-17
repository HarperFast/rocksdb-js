import { it } from 'vitest';

/**
 * Options for a stress test.
 *
 * Mirrors the benchmark `mode` mechanism (see `benchmark/setup.ts`): when
 * `STRESS_MODE=essential` is set (e.g. in CI), only tests marked
 * `mode: 'essential'` run and all others are skipped. With no `STRESS_MODE`
 * (or `STRESS_MODE=full`), every stress test runs.
 */
export type StressTestOptions = {
	mode?: 'essential' | 'full';
	/** Skip this test when the condition is true (e.g. `!globalThis.gc`). */
	skipIf?: boolean;
};

/**
 * Defines a stress test, honoring the `STRESS_MODE` env var to filter
 * non-essential tests.
 */
export function stressTest(
	name: string,
	options: StressTestOptions,
	fn: () => void | Promise<void>
): void {
	const skip =
		options.skipIf === true ||
		(process.env.STRESS_MODE === 'essential' && options.mode !== 'essential');

	(skip ? it.skip : it)(name, fn);
}
