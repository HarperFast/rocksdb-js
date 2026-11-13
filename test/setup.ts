import { getRegistryStatus, resetTestSwitchesForTesting } from '../src/load-binding.ts';
import { afterAll, beforeAll } from 'vitest';

// Native test switches are process-global, and Vitest runs many test files in one process.
resetTestSwitchesForTesting();

// The registry is process-global too, so a descriptor a file leaves registered stays open until
// the process exits, whose final flush then fails the run if the file removed the directory.
// Failing the file that leaked it names the culprit. Entries already present when the file started
// belong to an earlier file, which has already failed.
let registeredBefore = new Set<string>();
beforeAll(() => {
	registeredBefore = new Set(getRegistryStatus().map((entry) => entry.path));
});
afterAll(() => {
	const leaked = getRegistryStatus().filter((entry) => !registeredBefore.has(entry.path));
	if (leaked.length > 0) {
		throw new Error(
			`Test file left ${leaked.length} database(s) registered after its tests finished: ${JSON.stringify(leaked, null, 2)}`
		);
	}
});
