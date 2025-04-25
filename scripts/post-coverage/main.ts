/**
 * This script is used to post-process the HTML coverage report and make it
 * easier on the eyes at night. In other words, dark mode.
 */

import { existsSync, readdirSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';

const css = `<style type="text/css">
	body {
		background-color:#171717;
		color: white;
	}
	.quiet {
		color: #888;
	}
	.coverage-summary td.empty {
		opacity: 1;
	}
	.empty {
		background-color: #fff;
	}
	.low, .medium, .high {
		color: #333;
	}
	span.cline-neutral {
		background-color: #222;
	}
	.cline-yes {
		background-color: #8ac533;
		color: #333;
	}
	.cline-no, .cstat-no, .fstat-no {
		background-color: #902a3b;
		color: #333;
	}
	.cbranch-no {
		background-color: #ffff0033;
	}
	.kwd {
		color: #83d6c5;
	}
	.pln {
		color: #AA9BF5;
	}
	.pun {
		color:rgb(248, 180, 23);
	}
	.typ {
		color:rgb(248, 180, 23);
	}
	.str {
		color: #e394dc;
	}
</style>`;

const dirs: string[] = [
	resolve(import.meta.dirname, '../../coverage')
];

let dir: string | undefined;
while (dir = dirs.shift()) {
	if (existsSync(dir)) {
		for (const name of readdirSync(dir)) {
			try {
				const path = join(dir, name);
				if (statSync(path).isDirectory()) {
					dirs.push(path);
				} else if (name.endsWith('.html')) {
					const content = readFileSync(path, 'utf-8');
					const newContent = content.replace(/<\/head>/, `\n${css}\n</head>`);
					writeFileSync(path, newContent, 'utf-8');
				}
			} catch (error) {
				console.error(error);
			}
		}
	}
}
