// A pinned WebAssembly clang-format, so the same input formats identically on every platform
// regardless of any system clang-format.

import { format, version } from '@wasm-fmt/clang-format';
import { existsSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { extname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const repoRoot = resolve(fileURLToPath(import.meta.url), '../..');
// First-party code only: deps/ (gtest) and build/ hold vendored and generated sources.
const roots = ['benchmark', 'src/binding', 'test/native'];
const extensions = new Set(['.c', '.cc', '.cpp', '.cxx', '.h', '.hh', '.hpp', '.hxx']);
const check = process.argv.includes('--check');
const style = readFileSync(join(repoRoot, '.clang-format'), 'utf-8');
const label = `clang-format ${/version (\S+)/.exec(version())?.[1] ?? version()}`;

function collect(dir: string, files: string[]): void {
	for (const entry of readdirSync(dir, { withFileTypes: true })) {
		const path = join(dir, entry.name);
		if (entry.isDirectory()) {
			collect(path, files);
		} else if (entry.isFile() && extensions.has(extname(entry.name))) {
			files.push(path);
		}
	}
}

const files: string[] = [];
for (const root of roots) {
	const dir = join(repoRoot, root);
	if (existsSync(dir)) {
		collect(dir, files);
	}
}
files.sort();

// Format everything before writing anything, so a failure leaves the tree untouched.
const changed: { file: string; formatted: string }[] = [];
for (const file of files) {
	const source = readFileSync(file, 'utf-8');
	let formatted: string;
	try {
		formatted = format(source, file, style);
	} catch (error) {
		throw new Error(`${label} failed on ${relative(repoRoot, file)}`, { cause: error });
	}
	if (formatted !== source) {
		changed.push({ file, formatted });
	}
}

if (!check) {
	for (const { file, formatted } of changed) {
		writeFileSync(file, formatted);
	}
	console.log(`${label}: formatted ${changed.length} of ${files.length} C/C++ files`);
} else if (changed.length) {
	console.error(`${label}: ${changed.length} of ${files.length} C/C++ files need \`pnpm fmt\`:`);
	for (const { file } of changed) {
		console.error(`  ${relative(repoRoot, file)}`);
	}
	process.exitCode = 1;
} else {
	console.log(`${label}: all ${files.length} C/C++ files are formatted`);
}
