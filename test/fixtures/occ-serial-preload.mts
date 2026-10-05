// Loaded through NODE_OPTIONS=--import so the vitest process and every Node child it spawns open
// optimistic databases with serial validation. The setting is process-wide native state, so
// vitest's worker threads share it. Only the binding is loaded: importing src/index.ts here would
// install TransactionLog.prototype.query a second time when vitest loads its own copy.
import { config } from '../../src/load-binding.ts';

config({ occValidation: 'serial' });
