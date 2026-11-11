import * as binding from '../src/load-binding.ts';
import { afterEach, describe, expect, it } from 'vitest';

// A non-default value for every switch `resetTestSwitchesForTesting()` restores. A new
// `*ForTesting` setter fails the first test until it is added here, and the second until the
// native `testSwitches` table restores it.
const nonDefaultArgs: Record<string, unknown[]> = {
	forceTryAgainForTesting: [3],
	disablePrelogConflictCheckForTesting: [true],
	setPrelogConflictCheckRetriesOnlyForTesting: [false],
	forceDropFailureForTesting: [1],
	setTransactionStagingDelayForTesting: [1, 1000],
	setTransactionCommitAdmissionDelayForTesting: [1000],
	setWriteBufferManagerJoinDelayForTesting: [1, 1000],
};

const exportedSetters = Object.keys(binding).filter(
	(name) =>
		name.endsWith('ForTesting') && !name.startsWith('is') && name !== 'resetTestSwitchesForTesting'
);

describe('Test switches', () => {
	afterEach(() => {
		binding.resetTestSwitchesForTesting();
	});

	it('should cover every exported setter', () => {
		expect(exportedSetters.sort()).toEqual(Object.keys(nonDefaultArgs).sort());
	});

	it('should start each test file with every switch at its default', () => {
		expect(binding.resetTestSwitchesForTesting()).toEqual([]);
	});

	it('should restore every switch', () => {
		for (const [name, args] of Object.entries(nonDefaultArgs)) {
			(binding as unknown as Record<string, (...args: unknown[]) => void>)[name](...args);
		}
		expect(binding.resetTestSwitchesForTesting().sort()).toEqual(
			Object.keys(nonDefaultArgs).sort()
		);
		expect(binding.resetTestSwitchesForTesting()).toEqual([]);
	});
});
