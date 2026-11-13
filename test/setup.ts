import { resetTestSwitchesForTesting } from '../src/load-binding.ts';

// Native test switches are process-global, and Vitest runs many test files in one process.
resetTestSwitchesForTesting();
