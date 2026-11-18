// Release builds define no NDEBUG, so a libc assert() aborts the host process; native code uses
// ROCKSDB_JS_ASSERT (src/binding/core/assert.h) instead.

import { readdirSync, readFileSync } from 'node:fs';
import { extname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const repoRoot = resolve(fileURLToPath(import.meta.url), '../..');
const root = join(repoRoot, 'src/binding');
const extensions = new Set(['.c', '.cc', '.cpp', '.cxx', '.h', '.hh', '.hpp', '.hxx']);
const bareAssert = /(?<![\w.>:])assert\s*\(|#\s*include\s*[<"](?:cassert|assert\.h)[>"]/;

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
collect(root, files);

const violations: string[] = [];
for (const file of files) {
	const lines = readFileSync(file, 'utf-8').split(/\r?\n/);
	for (const [index, line] of lines.entries()) {
		const code = line.replace(/\/\/.*$/, '');
		if (/^\s*(?:\/\*|\*(?:\s|\/|$))/.test(code)) {
			continue;
		}
		if (bareAssert.test(code)) {
			violations.push(`${relative(repoRoot, file)}:${index + 1}: ${line.trim()}`);
		}
	}
}

if (violations.length > 0) {
	console.error(
		'Bare assert() in native code aborts release builds; use ROCKSDB_JS_ASSERT or ROCKSDB_JS_CHECK (src/binding/core/assert.h):'
	);
	for (const violation of violations) {
		console.error(`  ${violation}`);
	}
	process.exit(1);
}
