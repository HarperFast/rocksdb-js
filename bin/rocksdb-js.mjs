#!/usr/bin/env node

import { RocksDatabase, parseTransactionLog, versions } from '../dist/index.mjs';
import { existsSync, readdirSync } from 'node:fs';
import { mkdir, readdir } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { stdin, stdout } from 'node:process';
import { createInterface } from 'node:readline/promises';
import repl from 'node:repl';
import { inspect, parseArgs, styleText } from 'node:util';

const COMMANDS = {
	clear: clearCommand,
	columns: columnsCommand,
	count: countCommand,
	drop: dropCommand,
	exit: () => process.exit(0),
	get: getCommand,
	help: helpCommand,
	log: logCommand,
	prop: propCommand,
	'purge-logs': purgeLogsCommand,
	put: putCommand,
	query: queryCommand,
	remove: removeCommand,
	repl: replCommand,
	stats: statsCommand,
	use: useCommand,
};

const { values: argv, positionals } = parseArgs({
	allowPositionals: true,
	arguments: process.argv.slice(2),
	options: {
		help: { type: 'boolean', short: 'h' },
		readonly: { type: 'boolean', short: 'r' },
		version: { type: 'boolean', short: 'v' },
	},
});
const dbPath = resolve(positionals[0] || process.cwd());
const dbs = {};
let currentDB = null;
let r = null;
const defaultOpenOptions = {
	disableWAL: false,
	encoding: 'msgpack',
	freezeData: true,
	randomAccessStructure: true,
	readOnly: argv.readonly,
};

function completer(line) {
	const parts = line.trimStart().split(/[ \t]+/);
	const command = parts[0];

	if (parts.length <= 1) {
		const hits = Object.keys(COMMANDS).filter((c) => c.startsWith(command));
		return [hits.length ? hits : [], line];
	}

	if ((command === 'drop' || command === 'use') && parts.length === 2) {
		const arg = parts[1];
		const columns = dbs.default?.columns ?? [];
		const hits = columns.filter((c) => c.startsWith(arg));
		return [hits.length ? hits : columns, arg];
	}

	if (command === 'log' && currentDB) {
		if (parts.length === 2) {
			const arg = parts[1];
			const logs = currentDB.listLogs();
			const hits = logs.filter((l) => l.startsWith(arg));
			return [hits.length ? hits : logs, arg];
		} else if (parts.length === 3) {
			const [name, store] = parts.slice(1);
			const logs = currentDB.listLogs();
			const hit = logs.find((l) => l === name);
			if (hit) {
				const logPath = join(currentDB.path, 'transaction_logs', name);
				const logFiles = readdirSync(logPath).filter(
					(f) => (!store || f.startsWith(store)) && f.endsWith('.txnlog')
				);
				return [logFiles.length ? logFiles : [], store, name];
			}
			return [[], store, name];
		}
	}

	return [[], line];
}

function onSIGINT() {
	if (ctrlC) {
		console.log();
		process.exit(0);
	}
	ctrlC = true;
	console.log('\n(To exit, press Ctrl+C again or Ctrl+D or type "exit")');
	currentAbortController?.abort();
}

function createRL() {
	const iface = createInterface({ input: stdin, output: stdout, completer });
	iface.on('SIGINT', onSIGINT);
	iface.on('SIGBREAK', () => process.exit(0));
	return iface;
}

let rl = createRL();
let ctrlC = false;
let currentAbortController = null;

function str(text) {
	try {
		if (typeof text === 'symbol') {
			return `Symbol(${text.description ?? ''})`;
		}
		return String(text);
	} catch {
		return inspect(text, { colors: true, depth: 10 });
	}
}

const hl = (text) => styleText('magenta', str(text));
const note = (text) => styleText('gray', str(text));
const warn = (text) => styleText('yellow', str(text));
const bad = (text) => styleText('red', str(text));

function wrapColumns(columns) {
	const width = stdout.columns || 80;
	const prefix = 'Columns: ';
	const indent = ' '.repeat(prefix.length);
	const lines = [];
	let line = '';
	let lineLen = 0;
	for (const col of columns) {
		const sepLen = line ? 2 : 0; // ', '
		if (line && prefix.length + lineLen + sepLen + col.length > width) {
			lines.push(line);
			line = hl(col);
			lineLen = col.length;
		} else {
			line += (line ? ', ' : '') + hl(col);
			lineLen += sepLen + col.length;
		}
	}
	if (line) lines.push(line);
	return prefix + lines.join('\n' + indent);
}

async function run(fn) {
	const start = Date.now();
	const result = await fn();
	const end = Date.now();
	return { time: end - start, result };
}

async function ask(prompt) {
	rl.resume();
	while (true) {
		try {
			currentAbortController = new AbortController();
			const answer = await rl.question(prompt, { signal: currentAbortController.signal });
			rl.pause();
			ctrlC = false;
			return answer;
		} catch (err) {
			if (err.code !== 'ABORT_ERR') throw err;
		}
	}
}

async function clearCommand() {
	const answer = await ask(`Are you sure you want to clear all data? (y/N) `);
	if (answer !== 'y' && answer !== 'Y') {
		console.log();
		return;
	}
	const { time } = await run(() => currentDB.clear());
	console.log(`Cleared ${hl(currentDB.name)} ${note(`(${time}ms)`)}\n`);
}

function columnsCommand() {
	for (const column of currentDB.columns) {
		console.log(hl(column));
	}
	console.log();
}

async function countCommand() {
	const { result: count, time } = await run(() => currentDB.getKeysCount());
	console.log(`Count: ${hl(count)} ${note(`(${time}ms)`)}\n`);
}

async function dropCommand(args) {
	const [column] = args;
	if (!column) {
		console.log(`Usage: ${hl('drop <column>')}\n`);
		return;
	}

	const { columns } = dbs.default;
	if (!columns.includes(column)) {
		console.log(`Column family ${hl(column)} does not exist\n`);
		return;
	}

	if (column === 'default') {
		console.log('You cannot drop the default column family');
		const answer = await ask(`Are you sure you want to clear all data? (y/N) `);
		if (answer !== 'y' && answer !== 'Y') {
			console.log();
			return;
		}
		const { time } = await run(() => dbs.default.drop());
		console.log(`Cleared ${hl('default')} ${note(`(${time}ms)`)}\n`);
		return;
	}

	const db = dbs[column] || RocksDatabase.open(dbPath, { name: column, ...defaultOpenOptions });
	const { time } = await run(() => db.drop());
	db.close();
	delete dbs[column];
	console.log(`Dropped ${hl(column)} ${note(`(${time}ms)`)}\n`);
	if (currentDB.name === column) {
		console.log(`Switching ${hl(currentDB.name)} => ${hl('default')}\n`);
		currentDB = dbs.default;
	}
}

async function getCommand(args) {
	const [key] = args;
	if (!key) {
		console.log(`Usage: ${hl('get <key>')}\n`);
		return;
	}
	const value = await currentDB.get(key);
	if (value === undefined) {
		console.log('Key not found\n');
		return;
	}
	console.log(value);
	console.log();
}

async function propCommand(args) {
	const [key] = args;
	if (!key) {
		console.log(`Usage: ${hl('prop <key>')}\n`);
		return;
	}
	const value = currentDB.getDBProperty(key);
	console.log(value);
	console.log();
}

function helpCommand() {
	console.log(`${hl('clear')}                      Clear all data in the current column family`);
	console.log(`${hl('columns')}                    List column families`);
	console.log(
		`${hl('count')}                      Count the number of keys in the current column family`
	);
	console.log(`${hl('drop <column>')}              Permanently drop a column family`);
	console.log(`${hl('exit')}                       Exit the REPL`);
	console.log(`${hl('get <key>')}                  Get the value of a key`);
	console.log(`${hl('help')}                       Show this help message`);
	console.log(
		`${hl('log [name] [file] [entry]')}  List the transaction log store names and log store files`
	);
	console.log(`${hl('prop <key>')}                 Get a RocksDB property (try "rocksdb.stats")`);
	console.log(`${hl('purge-logs <name>')}          Purge transaction log files older than 3 days`);
	console.log(`${hl('put <key> <value>')}          Set the value of a key`);
	console.log(`${hl('query [start] [end]')}        Query a range of keys`);
	console.log(`${hl('remove <key>')}               Delete a key`);
	console.log(
		`${hl('repl')}                       Open a JS sub-REPL; "db" refers to the current column family`
	);
	console.log(
		`${hl('stats')}                      Show the statistics for the current column family`
	);
	console.log(
		`${hl('use [column]')}               Create a new column family or switch to an existing one`
	);
	console.log();
}

async function logCommand(args) {
	const [name, file, entryStr] = args;
	const logs = currentDB.listLogs();
	if (name) {
		if (!logs.includes(name)) {
			console.log(`Log store ${hl(name)} does not exist\n`);
			return;
		}
		if (file) {
			const logPath = join(currentDB.path, 'transaction_logs', name, file);
			const log = parseTransactionLog(logPath);
			const entry = entryStr ? parseInt(entryStr) : undefined;
			if (!isNaN(entry)) {
				if (entry < 1 || entry > log.entries.length) {
					console.log(`Entry ${hl(entry)} does not exist\n`);
					return;
				}
				const { data } = log.entries[entry - 1];
				console.log(`Entry ${entry} (${data.length.toLocaleString()} bytes)\n`);
				console.log(
					hl(
						data
							.toString('hex')
							.match(/.{1,2}/g)
							.join(' ')
					)
				);
				console.log();
				return;
			}

			console.log(`Size      = ${hl(`${log.size.toLocaleString()} bytes`)}`);
			console.log(`Version   = ${hl(log.version)}`);
			console.log(`Timestamp = ${hl(new Date(log.timestamp).toISOString())}`);
			console.log(`Entries   = ${hl(log.entries.length)}`);
			let i = 1;
			for (const entry of log.entries) {
				console.log(
					`${String(i++).padStart(3)})  ${hl(new Date(entry.timestamp).toISOString())}  ${entry.length.toLocaleString()} bytes  ${entry.flags & 0x01 ? '' : note('(continued in next file)')}`
				);
			}
			console.log();
			return;
		}
		const logPath = join(currentDB.path, 'transaction_logs', name);
		const logFiles = await readdir(logPath);
		for (const logFile of logFiles) {
			if (!logFile.endsWith('.txnlog')) continue;
			console.log(hl(logFile));
		}
	} else if (logs.length > 0) {
		for (const log of logs) {
			console.log(hl(log));
		}
	} else {
		console.log('No log stores found');
	}
	console.log();
}

async function purgeLogsCommand(args) {
	const [name] = args;
	if (!name) {
		console.log(`Usage: ${hl('purge-logs <name>')}\n`);
		return;
	}
	const answer = await ask(`Are you sure you want to purge logs? (y/N) `);
	if (answer !== 'y' && answer !== 'Y') {
		console.log();
		return;
	}
	const { result: removed, time } = await run(() => currentDB.purgeLogs({ name }));
	console.log(
		`Purged ${removed.length} log file${removed.length === 1 ? '' : 's'} ${note(`(${time}ms)`)}`
	);
	for (const file of removed) {
		console.log(hl(file));
	}
	console.log();
}

async function putCommand(args) {
	const [key, ...valueParts] = args;
	if (!key || valueParts.length === 0) {
		console.log(`Usage: ${hl('put <key> <value>')}\n`);
		return;
	}
	let value = valueParts.join(' ');
	if (value.startsWith('"') && value.endsWith('"')) {
		value = value.slice(1, -1);
	}
	await currentDB.put(key, value);
	console.log();
}

async function queryCommand(args) {
	const startTime = Date.now();
	let count = 0;
	const [start, end] = args;
	try {
		for await (const { key } of currentDB.getRange({ start, end, values: false })) {
			try {
				const value = await currentDB.get(key);
				console.log(`${str(key)} = ${inspect(value, { colors: true, depth: 10 })}`);
			} catch (error) {
				console.error(`${str(key)} = ${bad(error)}`);
			}
			count++;
		}
		const endTime = Date.now();
		console.log(`\n${count} results ${note(`(${endTime - startTime}ms)`)}`);
	} catch (err) {
		console.log(bad(err.toString()));
	}
	console.log();
}

async function removeCommand(args) {
	const [key] = args;
	if (!key) {
		console.log(`Usage: ${hl('remove <key>')}\n`);
		return;
	}
	const { time } = await run(() => currentDB.remove(key));
	console.log(`Removed ${hl(key)} ${note(`(${time}ms)`)}\n`);
}

async function replCommand() {
	const history = [...rl.history];
	rl.close();
	r = repl.start({ prompt: styleText('magenta', 'js> '), useColors: true, useGlobal: false });
	Object.defineProperty(r.context, 'db', { get: () => currentDB, enumerable: true });
	r.on('SIGINT', () => r.close());
	await new Promise((resolve) => r.on('exit', resolve));
	r = null;
	console.log();
	rl = createRL();
	rl.history = history;
}

function statsCommand() {
	const stats = currentDB.getStats();
	const maxlen = Math.max(...Object.keys(stats).map((key) => key.length)) - 8;
	for (const [key, value] of Object.entries(stats)) {
		console.log(`${key.substring(8).padEnd(maxlen)} = ${hl(value)}`);
	}
	console.log();
}

async function useCommand(args) {
	const [name] = args;
	if (!name) {
		currentDB = dbs.default;
		console.log(`${hl(currentDB.name)}\n`);
		return;
	}
	if (name === currentDB.name) {
		console.log(`${hl(currentDB.name)}\n`);
		return;
	}

	const columns = currentDB.columns;
	if (!dbs[name]) {
		if (columns.includes(name)) {
			console.log(`Switching ${hl(currentDB.name)} => ${hl(name)}\n`);
		} else {
			const answer = await ask(`Create new column ${hl(name)}? (y/N) `);
			if (answer !== 'y' && answer !== 'Y') {
				console.log();
				return;
			}
			console.log(`Created ${hl(name)}\n`);
		}
		dbs[name] = RocksDatabase.open(dbPath, { name, ...defaultOpenOptions });
	} else {
		console.log(`Switching ${hl(currentDB.name)} => ${hl(name)}\n`);
	}
	currentDB = dbs[name];
}

async function main() {
	console.log(`rocksdb.js CLI v${versions['rocksdb-js']} (RocksDB v${versions.rocksdb})`);

	if (argv.help) {
		console.log(`\nUsage: ${hl('rocksdb-js [dbpath]')}`);
		console.log('\nOptions:');
		console.log(`  ${hl('-h, --help')}      Show this help message`);
		console.log(`  ${hl('-r, --readonly')}  Open the database in read-only mode`);
		console.log(`  ${hl('-v, --version')}   Show the version information`);
		console.log();
		process.exit(0);
	}

	if (argv.version) {
		process.exit(0);
	}

	console.log();
	if (argv.readonly) {
		console.log(`Mode:    ${warn('READ-ONLY')}`);
	}
	console.log(`Path:    ${hl(dbPath)}`);

	try {
		if (!existsSync(join(dbPath, 'CURRENT'))) {
			console.log();
			let answer = await ask('RocksDB database does not exist, create it? (y/N) ');
			if (answer !== 'y' && answer !== 'Y') {
				process.exit(0);
			}
			await mkdir(dbPath, { recursive: true });

			const count = await readdir(dbPath);
			if (count.length > 0) {
				answer = await ask('Directory not empty, are you sure you want to continue? (y/N) ');
				if (answer !== 'y' && answer !== 'Y') {
					process.exit(0);
				}
			}
		}

		currentDB = RocksDatabase.open(dbPath, { ...defaultOpenOptions });
		dbs[currentDB.name] = currentDB;

		console.log(wrapColumns(currentDB.columns) + '\n');
		console.log(`Type "help" for more information.`);

		while (true) {
			const line = (await ask('> ')).trim().split(/[ \t]+/);
			const command = line[0];
			if (!command) continue;
			const commandFn = COMMANDS[command];
			if (commandFn) {
				try {
					await commandFn(line.slice(1));
				} catch (error) {
					console.error(bad(error));
				}
			} else {
				console.log(`Unknown command: ${command}\n`);
			}
		}
	} catch (error) {
		if (error.message.includes('Resource temporarily unavailable')) {
			console.log();
			console.error(
				bad('Error: RocksDB is already open by another process, rerun with the read-only flag:\n')
			);
			console.log(hl(`  rocksdb-js --readonly ${dbPath}\n`));
			process.exit(1);
		}
		if (error.code !== 'ABORT_ERR') {
			console.error(bad(error));
			process.exit(1);
		}
	} finally {
		r?.close();
		for (const db of Object.values(dbs)) {
			db?.close();
		}
		rl.close();
	}
}

main();
