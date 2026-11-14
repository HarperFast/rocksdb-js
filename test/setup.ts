import { getRegistryStatus, resetTestSwitchesForTesting } from '../src/load-binding.ts';
import { afterAll, beforeAll } from 'vitest';

// Native test switches are process-global, and Vitest runs many test files in one process.
resetTestSwitchesForTesting();

// The registry is process-global too: a database a file leaves registered stays open until the
// exit-time flush, which fails once its directory is gone. Fail the file that leaked it instead;
// entries present at its start belong to an earlier, already-failed file.
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
