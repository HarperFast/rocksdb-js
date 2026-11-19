// Nothing defines NDEBUG in release builds, so a libc assert() would abort the host process.

import { readdirSync, readFileSync } from 'node:fs';
import { extname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const repoRoot = resolve(fileURLToPath(import.meta.url), '../..');
const root = join(repoRoot, 'src/binding');
const extensions = new Set(['.c', '.cc', '.cpp', '.cxx', '.h', '.hh', '.hpp', '.hxx']);
const assertInclude = /^[ \t]*#[ \t]*include\s*[<"](?:cassert|assert\.h)[>"]/gm;
const assertCall = /(?<![\w.>:])assert\s*\(/g;

const identifier = /[A-Za-z_]\w*/y;
// A pp-number takes its digit separators (1'000, 0xFF'FF), so their quotes never open a literal.
const ppNumber = /\.?\d(?:[eEpP][+-]|'\w|[\w.])*/y;
const quoted = /(["'])(?:\\.|(?!\1)[^\\\n])*\1?/y;
const rawOpen = /"([^()\\\s]{0,16})\(/y;
const literalPrefix = /^(?:u8|u|U|L)$/;
const rawPrefix = /^(?:u8|u|U|L)?R$/;

const blank = (text: string) => text.replace(/[^\n]/g, ' ');

/**
 * Returns `code` with comments and literals blanked, and `directives` with comments and raw strings
 * blanked, so `#include "assert.h"` stays visible but a multi-line raw string cannot fake a directive.
 * Newlines are kept, so offsets map to the right line.
 */
function mask(source: string): { code: string; directives: string } {
	const code: string[] = [];
	const directives: string[] = [];
	const emit = (text: string, inCode: string, inDirectives: string) => {
		code.push(inCode);
		directives.push(inDirectives);
		return text.length;
	};
	const keep = (text: string) => emit(text, text, text);
	const literal = (text: string) => emit(text, blank(text), text);
	const hidden = (text: string) => emit(text, blank(text), blank(text));
	const match = (pattern: RegExp, at: number) => {
		pattern.lastIndex = at;
		return pattern.exec(source);
	};

	let i = 0;
	while (i < source.length) {
		const char = source[i];
		const name = match(identifier, i)?.[0];
		const number = name ? undefined : match(ppNumber, i)?.[0];
		if (source.startsWith('//', i)) {
			const end = source.indexOf('\n', i);
			i += hidden(source.slice(i, end < 0 ? source.length : end));
		} else if (source.startsWith('/*', i)) {
			const end = source.indexOf('*/', i + 2);
			i += hidden(source.slice(i, end < 0 ? source.length : end + 2));
		} else if (name) {
			const after = i + name.length;
			const raw = rawPrefix.test(name) ? match(rawOpen, after) : null;
			if (raw) {
				const close = `)${raw[1]}"`;
				const end = source.indexOf(close, after + raw[0].length);
				i += hidden(source.slice(i, end < 0 ? source.length : end + close.length));
			} else {
				i += keep(name);
				if (literalPrefix.test(name) && (source[i] === '"' || source[i] === "'")) {
					i += literal(match(quoted, i)![0]);
				}
			}
		} else if (number) {
			i += keep(number);
		} else if (char === '"' || char === "'") {
			i += literal(match(quoted, i)![0]);
		} else {
			i += keep(char);
		}
	}
	return { code: code.join(''), directives: directives.join('') };
}

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

/** Joins backslash-newline splices as the compiler does before tokenizing; `origin` maps each kept character back. */
function splice(source: string): { text: string; origin: number[] } {
	const kept: string[] = [];
	const origin: number[] = [];
	for (let i = 0; i < source.length; i++) {
		const joined =
			source[i] === '\\' && source.startsWith(source[i + 1] === '\r' ? '\r\n' : '\n', i + 1);
		if (joined) {
			i += source[i + 1] === '\r' ? 2 : 1;
		} else {
			kept.push(source[i]);
			origin.push(i);
		}
	}
	return { text: kept.join(''), origin };
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
	const { text, origin } = splice(source);
	const report = (offset: number) => {
		const line = lineAt(source, origin[offset]);
		violations.push(`${relative(repoRoot, file)}:${line}: ${lines[line - 1].trim()}`);
	};
	const { code, directives } = mask(text);
	for (const found of directives.matchAll(assertInclude)) {
		report(found.index);
	}
	for (const found of code.matchAll(assertCall)) {
		report(found.index);
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
