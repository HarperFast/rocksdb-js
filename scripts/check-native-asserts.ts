// Release builds define no NDEBUG, so a libc assert() aborts the host process; native code uses
// ROCKSDB_JS_ASSERT (src/binding/core/assert.h) instead.

import { readdirSync, readFileSync } from 'node:fs';
import { extname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const repoRoot = resolve(fileURLToPath(import.meta.url), '../..');
const root = join(repoRoot, 'src/binding');
const extensions = new Set(['.c', '.cc', '.cpp', '.cxx', '.h', '.hh', '.hpp', '.hxx']);
const assertInclude = /^[ \t]*#[ \t]*include\s*[<"](?:cassert|assert\.h)[>"]/gm;
const assertCall = /(?<![\w.>:])assert\s*\(/g;
// Comments and string/char literals, blanked out so a match inside one is ignored. Newlines are
// kept, so offsets still map to the right line and a call split across lines still matches. A quote
// after a hex digit is a digit separator (1'000), not a character literal.
const commentOrLiteral =
	/\/\*[\s\S]*?\*\/|\/\/[^\n]*|"(?:\\.|[^"\\\n])*"|(?<![\dA-Fa-f])'(?:\\.|[^'\\\n])*'/g;

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

function lineAt(source: string, offset: number): number {
	let line = 1;
	for (let i = 0; i < offset; i++) {
		if (source.charCodeAt(i) === 10) {
			line++;
		}
	}
	return line;
}

const files: string[] = [];
collect(root, files);

const violations: string[] = [];
for (const file of files) {
	const source = readFileSync(file, 'utf-8');
	const lines = source.split(/\r?\n/);
	const report = (offset: number) => {
		const line = lineAt(source, offset);
		violations.push(`${relative(repoRoot, file)}:${line}: ${lines[line - 1].trim()}`);
	};
	const blank = (text: string) => text.replace(/[^\n]/g, ' ');
	const uncommented = source.replace(commentOrLiteral, (text) =>
		text.startsWith('"') || text.startsWith("'") ? text : blank(text)
	);
	for (const match of uncommented.matchAll(assertInclude)) {
		report(match.index);
	}
	const code = source.replace(commentOrLiteral, blank);
	for (const match of code.matchAll(assertCall)) {
		report(match.index);
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
