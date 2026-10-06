import { spawnSync } from 'node:child_process';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { describe, expect, it } from 'vitest';

const root = join(__dirname, '..');
const preload = pathToFileURL(join(__dirname, 'fixtures', 'occ-serial-preload.mts')).href;
const suites = [
	'test/drop-deferred-reclamation.test.ts',
	'test/drop.test.ts',
	'test/txn-close-commit-uaf.test.ts',
];

// The column-family drop protocol (invariant 24) must hold under both validation policies, and its
// fork fixtures spawn their own processes, so the preload rides NODE_OPTIONS into all of them.
describe.skipIf(!!process.versions.bun || !!process.versions.deno)(
	'column-family drop suites under occValidation serial',
	() => {
		it('pass', () => {
			const env = Object.fromEntries(
				Object.entries(process.env).filter(
					([name]) => !name.startsWith('VITEST') && name !== 'FORCE_COLOR'
				)
			);
			env.NO_COLOR = '1';
			env.NODE_OPTIONS = [process.env.NODE_OPTIONS, `--import=${preload}`]
				.filter(Boolean)
				.join(' ');
			const child = spawnSync(
				process.execPath,
				['--expose-gc', join(root, 'node_modules/vitest/vitest.mjs'), 'run', ...suites],
				{ cwd: root, encoding: 'utf8', env, timeout: 280000 }
			);
			const output = `${child.stdout}\n${child.stderr}`;
			expect(child.error, output).toBeUndefined();
			expect(child.status, output).toBe(0);
			expect(output).toMatch(/Test Files\s+3 passed/);
		}, 300000);
	}
);
