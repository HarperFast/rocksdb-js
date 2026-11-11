// Verifies the package as published: packs the tarball, installs it into a scratch project,
// then loads it through ESM, CJS and TypeScript and does a real open/put/get/close.
// Run after `pnpm build`. The native binding ships as a separate per-platform package, so the
// locally built one is copied into the installed package's `build/Release` (where the loader looks).
import { execFileSync } from 'node:child_process';
import { cpSync, mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const shell = process.platform === 'win32';
const tmp = mkdtempSync(join(tmpdir(), 'rocksdb-js-package-'));

function run(cmd, args, cwd) {
	console.log(`$ ${cmd} ${args.join(' ')}`);
	return execFileSync(cmd, args, {
		cwd,
		encoding: 'utf8',
		shell,
		stdio: ['ignore', 'pipe', 'inherit'],
	});
}

try {
	const packDir = join(tmp, 'pack');
	mkdirSync(packDir);
	run('pnpm', ['pack', '--pack-destination', packDir], root);
	const tarball = join(
		packDir,
		readdirSync(packDir).find((f) => f.endsWith('.tgz'))
	);

	const project = join(tmp, 'project');
	mkdirSync(project);
	writeFileSync(
		join(project, 'package.json'),
		JSON.stringify({ name: 'smoke', private: true, type: 'module' })
	);
	// optional deps are the published per-platform bindings, which may not match this build
	run('npm', ['install', '--omit=optional', '--no-audit', '--no-fund', tarball], project);
	run(
		'npm',
		['install', '--no-save', '--no-audit', '--no-fund', 'typescript@5', '@types/node'],
		project
	);

	const installed = join(project, 'node_modules', '@harperfast', 'rocksdb-js');
	mkdirSync(join(installed, 'build'), { recursive: true });
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
	// the `.mts`/`.cts` pair exercises the `import` and `require` type conditions
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
				// dependency @harperfast/extended-iterable ships no CJS typings
				skipLibCheck: true,
				strict: true,
				target: 'es2022',
			},
			files: ['types.mts', 'types.cts'],
		})
	);

	console.log(run('node', ['smoke.mjs'], project));
	console.log(run('node', ['smoke.cjs'], project));
	run('node', [join('node_modules', 'typescript', 'bin', 'tsc'), '-p', 'tsconfig.json'], project);
	run(
		'node',
		[join('node_modules', '@harperfast', 'rocksdb-js', 'bin', 'rocksdb-js.mjs'), '--help'],
		project
	);
	console.log('Package smoke test passed');
} finally {
	rmSync(tmp, { force: true, recursive: true, maxRetries: 5 });
}
