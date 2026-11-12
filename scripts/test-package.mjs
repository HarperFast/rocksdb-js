// Run after `pnpm build`. The native binding ships as a separate per-platform package, so the
// locally built one is copied into the installed package's `build/Release` (where the loader looks).
import { execFileSync } from 'node:child_process';
import { cpSync, mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const isWindows = process.platform === 'win32';
const tmp = mkdtempSync(join(tmpdir(), 'rocksdb-js-package-'));

// pnpm/npm are .cmd shims on Windows and need a shell, which splits unquoted paths with spaces
function run(cmd, args, cwd) {
	console.log(`$ ${cmd} ${args.join(' ')}`);
	const needsShell = isWindows && cmd !== 'node';
	return execFileSync(cmd, needsShell ? args.map((a) => `"${a}"`) : args, {
		cwd,
		encoding: 'utf8',
		shell: needsShell,
		stdio: ['ignore', 'pipe', 'inherit'],
		timeout: 300_000,
	});
}

try {
	const packDir = join(tmp, 'pack');
	mkdirSync(packDir);
	run('pnpm', ['pack', '--pack-destination', packDir], root);
	const tarballName = readdirSync(packDir).find((f) => f.endsWith('.tgz'));
	if (!tarballName) throw new Error('pnpm pack produced no tarball');
	const tarball = join(packDir, tarballName);

	const project = join(tmp, 'project');
	mkdirSync(project);
	writeFileSync(
		join(project, 'package.json'),
		JSON.stringify({ name: 'smoke', private: true, type: 'module' })
	);
	// optional deps are the published per-platform bindings, which may not match this build
	run(
		'npm',
		[
			'install',
			'--omit=optional',
			'--no-audit',
			'--no-fund',
			tarball,
			'typescript@5',
			'@types/node',
		],
		project
	);

	const installed = join(project, 'node_modules', '@harperfast', 'rocksdb-js');
	mkdirSync(join(installed, 'build', 'Release'), { recursive: true });
	cpSync(
		join(root, 'build', 'Release', 'rocksdb-js.node'),
		join(installed, 'build', 'Release', 'rocksdb-js.node')
	);

	const body = `
	const dir = ${'`${process.cwd()}`'} + '/db-' + Math.random().toString(36).slice(2);
	const db = RocksDatabase.open(dir);
	await db.put('key', { hello: 'world' });
	const value = await db.get('key');
	if (value?.hello !== 'world') throw new Error('unexpected value: ' + JSON.stringify(value));
	db.close();
	console.log(label + ' ok');
	`;
	writeFileSync(
		join(project, 'smoke.mjs'),
		`import { RocksDatabase } from '@harperfast/rocksdb-js';\nconst label = 'esm';${body}`
	);
	writeFileSync(
		join(project, 'smoke.cjs'),
		`const { RocksDatabase } = require('@harperfast/rocksdb-js');\nconst label = 'cjs';(async () => {${body}})().catch((e) => { console.error(e); process.exit(1); });`
	);
	writeFileSync(
		join(project, 'types.mts'),
		`import { RocksDatabase } from '@harperfast/rocksdb-js';\nconst db: RocksDatabase = RocksDatabase.open('x');\nexport const n: Promise<unknown> = db.get('k');\n`
	);
	writeFileSync(
		join(project, 'types.cts'),
		`import { RocksDatabase } from '@harperfast/rocksdb-js';\nconst db: RocksDatabase = RocksDatabase.open('x');\nexport const n: Promise<unknown> = db.get('k');\n`
	);
	writeFileSync(
		join(project, 'tsconfig.json'),
		JSON.stringify({
			compilerOptions: {
				module: 'nodenext',
				moduleResolution: 'nodenext',
				noEmit: true,
				strict: true,
				target: 'es2022',
			},
			files: ['types.mts', 'types.cts'],
		})
	);

	console.log(run('node', ['smoke.mjs'], project));
	console.log(run('node', ['smoke.cjs'], project));
	run('node', [join('node_modules', 'typescript', 'bin', 'tsc'), '-p', 'tsconfig.json'], project);
	run('npm', ['exec', '--no', '--', 'rocksdb-js', '--help'], project);
	console.log('Package smoke test passed');
} finally {
	rmSync(tmp, { force: true, recursive: true, maxRetries: 5 });
}
