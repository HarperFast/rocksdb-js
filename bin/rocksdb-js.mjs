#!/usr/bin/env node

import {
	RocksDatabase,
	backups,
	parseTransactionLog,
	validateTransactionLogStore,
	versions,
} from '../dist/index.mjs';
import { once } from 'node:events';
import { existsSync, readdirSync } from 'node:fs';
import { mkdir, readdir, stat } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { stderr, stdin, stdout } from 'node:process';
import { createInterface } from 'node:readline/promises';
import repl from 'node:repl';
import { inspect, parseArgs, stripVTControlCharacters, styleText } from 'node:util';

const BACKUP_SUBCOMMANDS = ['backup', 'restore', 'verify', 'delete', 'purge'];

class CLIError extends Error {
	constructor(message, exitCode = 1) {
		super(message);
		this.exitCode = exitCode;
	}
}

// Backup and restore hold RocksDB's LOCK (a read-write open) so a live writer in another
// process is refused rather than raced; only a restore into an empty path skips the open.
const COMMANDS = [
	{
		name: 'backups',
		usage: '<dir> [subcommand] [arg]',
		summary: 'Manage backups (list, backup, restore, verify, delete, purge)',
		access: ([, subcommand]) =>
			subcommand === 'backup' ? 'read' : subcommand === 'restore' ? 'write' : 'none',
		locks: ([, subcommand]) => subcommand === 'backup',
		opens: ([, subcommand]) =>
			subcommand === 'backup' || (subcommand === 'restore' && !isEmptyPath(dbPath)),
		// A restore opens the database only for its lock, so that open must not purge aged
		// transaction-log segments before the restore is confirmed. '0ms' disables retention
		// (a numeric 0 reads as unset), and with it the age threshold must be 0 or a log write
		// never returns (#1079).
		openOptions: ([, subcommand]) =>
			subcommand === 'restore'
				? { transactionLogRetention: '0ms', transactionLogMaxAgeThreshold: 0 }
				: undefined,
		validate: validateBackupsArgs,
		run: backupsCommand,
		print: printBackupsResult,
	},
	{
		name: 'clear',
		summary: 'Clear all data in the current column family',
		access: 'write',
		run: clearCommand,
		print: ({ column, elapsedMs }) =>
			console.log(`Cleared ${hl(column)} ${note(`(${elapsedMs}ms)`)}`),
	},
	{
		name: 'columns',
		summary: 'List column families',
		access: 'read',
		run: () => currentDB.columns,
		print: (columns) => {
			for (const column of columns) console.log(hl(column));
		},
	},
	{
		name: 'compact',
		summary: 'Compact the current column family',
		access: 'write',
		run: compactCommand,
		print: printCompactResult,
	},
	{
		name: 'count',
		summary: 'Count the number of keys in the current column family',
		access: 'read',
		run: async () => {
			const { result: count, elapsedMs } = await timed(() => currentDB.getKeysCount());
			return { count, elapsedMs };
		},
		print: ({ count, elapsedMs }) => console.log(`Count: ${hl(count)} ${note(`(${elapsedMs}ms)`)}`),
	},
	{
		name: 'drop',
		usage: '<column>',
		summary: 'Permanently drop a column family',
		access: 'write',
		run: dropCommand,
		print: ({ column, cleared, elapsedMs }) =>
			console.log(`${cleared ? 'Cleared' : 'Dropped'} ${hl(column)} ${note(`(${elapsedMs}ms)`)}`),
	},
	{
		name: 'exit',
		summary: 'Exit the REPL',
		replOnly: true,
		access: 'none',
		run: () => process.exit(0),
	},
	{
		name: 'get',
		usage: '<key>',
		summary: 'Get the value of a key',
		access: 'read',
		run: getCommand,
		print: (value) => console.log(value),
	},
	{
		name: 'help',
		summary: 'Show this help message',
		access: 'none',
		run: () =>
			commandsFor(interactive).map(({ name, usage, summary }) => ({
				command: commandSignature(name, usage),
				summary,
			})),
		print: (rows) => (interactive ? printCommandTable(rows) : printHelp()),
	},
	{
		name: 'log',
		aliases: ['logs'],
		usage: '[name] [file] [entry]',
		summary: 'List the transaction log store names and log store files',
		access: 'read',
		run: logCommand,
		print: printLogResult,
	},
	{
		name: 'prop',
		usage: '<key>',
		summary: 'Get a RocksDB property (try "rocksdb.stats")',
		access: 'read',
		run: ([key]) => {
			return currentDB.getDBProperty(key) ?? null;
		},
		print: (value) => console.log(value),
	},
	{
		name: 'purge-logs',
		usage: '<name>',
		summary: 'Purge transaction log files older than 3 days',
		access: 'write',
		run: purgeLogsCommand,
		print: ({ removed, elapsedMs }) => {
			console.log(
				`Purged ${removed.length} log file${removed.length === 1 ? '' : 's'} ${note(`(${elapsedMs}ms)`)}`
			);
			for (const file of removed) console.log(hl(file));
		},
	},
	{
		name: 'put',
		usage: '<key> <value>',
		summary: 'Set the value of a key',
		access: 'write',
		creates: true,
		emptyValueAllowed: true,
		run: putCommand,
		print: ({ key }) => console.log(`Stored ${hl(key)}`),
	},
	{
		name: 'query',
		usage: '[start] [end]',
		summary: 'Query a range of keys',
		access: 'read',
		run: queryCommand,
		print: printQueryResult,
	},
	{
		name: 'remove',
		usage: '<key>',
		summary: 'Delete a key',
		access: 'write',
		run: removeCommand,
		print: ({ key, elapsedMs }) => console.log(`Removed ${hl(key)} ${note(`(${elapsedMs}ms)`)}`),
	},
	{
		name: 'repl',
		summary: 'Open a JS sub-REPL; "db" refers to the current column family',
		replOnly: true,
		access: 'none',
		run: replCommand,
	},
	{
		name: 'stats',
		summary: 'Show the statistics for the current column family',
		access: 'read',
		run: () => currentDB.getStats(),
		print: (stats) => {
			const maxlen = Math.max(...Object.keys(stats).map((key) => key.length)) - 8;
			for (const [key, value] of Object.entries(stats)) {
				console.log(`${key.substring(8).padEnd(maxlen)} = ${hl(value)}`);
			}
		},
	},
	{
		name: 'use',
		usage: '[column]',
		summary: 'Create a new column family or switch to an existing one',
		replOnly: true,
		access: 'none',
		run: useCommand,
	},
	{
		name: 'verify-logs',
		usage: '[name]',
		summary: 'Validate transaction log store files (all stores if no name)',
		access: 'read',
		run: verifyLogsCommand,
		print: printVerifyLogsResult,
		failed: (result) => !result.valid,
	},
];

const commandsByName = new Map();
for (const def of COMMANDS) {
	for (const name of [def.name, ...(def.aliases ?? [])]) commandsByName.set(name, def);
}

function commandsFor(isInteractive) {
	return COMMANDS.filter((def) => isInteractive || !def.replOnly);
}

function commandSignature(name, usage) {
	return usage ? `${name} ${usage}` : name;
}

function resolveAccess(def, args) {
	return typeof def.access === 'function' ? def.access(args) : def.access;
}

let argv;
let positionals;
try {
	({ values: argv, positionals } = parseArgs({
		allowPositionals: true,
		args: process.argv.slice(2),
		options: {
			column: { type: 'string', short: 'c' },
			help: { type: 'boolean', short: 'h' },
			json: { type: 'boolean' },
			readonly: { type: 'boolean', short: 'r' },
			version: { type: 'boolean', short: 'v' },
			yes: { type: 'boolean', short: 'y' },
		},
	}));
} catch (error) {
	const message = `${error.message}\nRun "rocksdb-js --help" for usage.`;
	console.error(
		process.argv.includes('--json') ? JSON.stringify({ error: message, code: error.code }) : message
	);
	process.exit(2);
}

const [dbArg, commandName, commandArgs] = commandsByName.has(positionals[0])
	? [undefined, positionals[0], positionals.slice(1)]
	: [positionals[0], positionals[1], positionals.slice(2)];
const dbPath = resolve(dbArg || process.cwd());
const interactive = commandName === undefined;
// No prototype: a column named "__proto__" must still be an own, enumerable entry.
const dbs = Object.create(null);
let currentDB = null;
let openReadOnly = Boolean(argv.readonly);
let r = null;
const defaultOpenOptions = {
	disableWAL: false,
	encoding: 'msgpack',
	freezeData: true,
	randomAccessStructure: true,
};

function openOptions(extra) {
	return { ...defaultOpenOptions, readOnly: openReadOnly, ...extra };
}

// Resolved backup dir -> BackupInfo[]; warmed by list operations, used for tab completion.
let backupListCache = { dir: null, list: null };

function completer(line) {
	const parts = line.trimStart().split(/[ \t]+/);
	const command = parts[0];

	if (parts.length <= 1) {
		const hits = [...commandsByName.keys()].filter((c) => c.startsWith(command));
		return [hits.length ? hits : [], line];
	}

	if ((command === 'drop' || command === 'use') && parts.length === 2) {
		const arg = parts[1];
		const columns = dbs.default?.columns ?? [];
		const hits = columns.filter((c) => c.startsWith(arg));
		return [hits.length ? hits : columns, arg];
	}

	if (
		(command === 'log' ||
			command === 'logs' ||
			command === 'verify-logs' ||
			command === 'purge-logs') &&
		currentDB
	) {
		if (parts.length === 2) {
			const arg = parts[1];
			const logs = currentDB.listLogs();
			const hits = logs.filter((l) => l.startsWith(arg));
			return [(hits.length ? hits : logs).map((s) => `${s} `), arg];
		} else if (parts.length === 3 && (command === 'log' || command === 'logs')) {
			const [name, store] = parts.slice(1);
			const logs = currentDB.listLogs();
			const hit = logs.find((l) => l === name);
			if (hit) {
				const logPath = join(currentDB.path, 'transaction_logs', name);
				const logFiles = readdirSync(logPath).filter(
					(f) => (!store || f.startsWith(store)) && f.endsWith('.txnlog')
				);
				return [logFiles.length ? logFiles.map((f) => `${f} `) : [], store, name];
			}
			return [[], store, name];
		}
	}

	if (command === 'backups') {
		if (parts.length === 2) {
			const arg = parts[1];
			return [completePath(arg), arg];
		}
		if (parts.length === 3) {
			const arg = parts[2];
			const hits = BACKUP_SUBCOMMANDS.filter((s) => s.startsWith(arg)).map((s) => `${s} `);
			return [hits.length ? hits : BACKUP_SUBCOMMANDS.map((s) => `${s} `), arg];
		}
		if (parts.length === 4 && backupListCache.dir === resolve(parts[1]) && backupListCache.list) {
			const subcommand = parts[2];
			const arg = parts[3];
			let candidates = [];
			if (['restore', 'verify', 'delete'].includes(subcommand)) {
				candidates = backupListCache.list.map((b) => String(b.backupId));
			} else if (subcommand === 'purge') {
				candidates = backupListCache.list.map((_, i) => String(i + 1));
			}
			return [candidates.filter((c) => c.startsWith(arg)).map((c) => `${c} `), arg];
		}
	}

	return [[], line];
}

function completePath(partial) {
	const sep = partial.lastIndexOf('/');
	const dir = sep === -1 ? '.' : partial.slice(0, sep + 1);
	const prefix = sep === -1 ? partial : partial.slice(sep + 1);
	try {
		return readdirSync(dir, { withFileTypes: true })
			.filter((entry) => entry.name.startsWith(prefix))
			.map((entry) => `${sep === -1 ? '' : dir}${entry.name}${entry.isDirectory() ? '/' : ' '}`);
	} catch {
		return [];
	}
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
	iface.on('close', () => {
		rlClosed = true;
	});
	iface.on('error', (err) => {
		console.error(bad(`stdin error: ${err.message ?? err}`));
		process.exit(1);
	});
	return iface;
}

// Only the REPL reads commands from stdin; one-shot mode must not consume it.
let rl = null;
let lineIterator = null;
let rlClosed = false;
let ctrlC = false;
let currentAbortController = null;

function startRL() {
	rl = createRL();
	lineIterator = rl[Symbol.asyncIterator]();
	rlClosed = false;
}

function whenAborted(signal) {
	return new Promise((_resolve, reject) => {
		signal.addEventListener(
			'abort',
			() => reject(Object.assign(new Error('The operation was aborted'), { code: 'ABORT_ERR' })),
			{ once: true }
		);
	});
}

function str(text) {
	try {
		if (typeof text === 'symbol') {
			return `Symbol(${text.description ?? ''})`;
		}
		return String(text);
	} catch {
		return inspect(text, { colors: useColors, depth: 10 });
	}
}

const useColors = stdout.hasColors?.() ?? false;
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

function formatHexDump(data) {
	const bytesPerRow = 16;
	const lines = [];
	for (let offset = 0; offset < data.length; offset += bytesPerRow) {
		const chunk = data.subarray(offset, offset + bytesPerRow);
		const hexParts = [];
		for (let i = 0; i < bytesPerRow; i++) {
			if (i === 8) hexParts.push('');
			hexParts.push(i < chunk.length ? chunk[i].toString(16).padStart(2, '0') : '  ');
		}
		let ascii = '';
		for (const byte of chunk) {
			ascii += byte >= 0x20 && byte <= 0x7e ? String.fromCharCode(byte) : '.';
		}
		lines.push(
			`${note(offset.toString(16).padStart(8, '0'))}  ${hl(hexParts.join(' '))}  ${note('|')}${hl(ascii)}${note('|')}`
		);
	}
	return lines.join('\n');
}

function formatBytes(bytes) {
	if (bytes < 1024) return `${bytes} B`;
	if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)} KB`;
	if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(2)} MB`;
	return `${(bytes / 1024 / 1024 / 1024).toFixed(2)} GB`;
}

function formatTimestamp(timestamp) {
	return Number.isFinite(timestamp) && timestamp > 0
		? new Date(timestamp).toISOString()
		: String(timestamp);
}

async function timed(fn) {
	const start = Date.now();
	const result = await fn();
	return { elapsedMs: Date.now() - start, result };
}

// Returns the next line, or `null` once the iterator itself reports `done`. `close` can
// fire before lineIterator's already-buffered lines are drained, so `rlClosed` never
// gates reading — only resume()/setPrompt()/prompt()/pause(), which throw
// ERR_USE_AFTER_CLOSE once closed.
async function ask(prompt) {
	if (!rlClosed) {
		rl.resume();
		rl.setPrompt(prompt);
		rl.prompt();
	}
	// A second next() would queue behind this one and steal the next real line out from
	// under a Ctrl-C retry below, so retries re-await this same promise instead.
	const pending = lineIterator.next();
	while (true) {
		try {
			currentAbortController = new AbortController();
			const result = await Promise.race([pending, whenAborted(currentAbortController.signal)]);
			ctrlC = false;
			if (result.done) return null;
			if (!rlClosed) rl.pause();
			return result.value;
		} catch (err) {
			if (err.code !== 'ABORT_ERR') throw err;
			if (rlClosed) continue;
			rl.clearLine();
			rl.prompt();
		}
	}
}

// One-shot prompts go to stderr so stdout carries only results.
async function confirm(question) {
	if (argv.yes) return true;
	let answer;
	if (interactive) {
		answer = await ask(`${question} (y/N) `);
	} else if (stdin.isTTY) {
		const iface = createInterface({ input: stdin, output: stderr });
		try {
			// EOF can close the interface without settling the question.
			answer = await new Promise((resolve, reject) => {
				iface.once('close', () => resolve(null));
				iface
					.question(`${question} (y/N) `)
					.then(resolve, (error) => (error.code === 'ABORT_ERR' ? resolve(null) : reject(error)));
			});
		} finally {
			iface.close();
		}
	} else {
		throw new CLIError(`${stripVTControlCharacters(question)} Pass --yes to confirm.`);
	}
	return answer === 'y' || answer === 'Y';
}

async function confirmOrCancel(question) {
	if (!(await confirm(question))) throw new CLIError('Cancelled');
}

function invalidateBackupListCache() {
	backupListCache = { dir: null, list: null };
}

async function fetchBackupList(backupDir) {
	const list = await backups.list(backupDir);
	backupListCache = { dir: backupDir, list };
	return list;
}

function printBackupList(list) {
	if (list.length === 0) {
		console.log('No backups found');
		return;
	}
	for (const backup of list) {
		const ts = new Date(backup.timestamp * 1000).toISOString();
		console.log(
			`${String(backup.backupId).padStart(3)})  ${hl(ts)}  ${formatBytes(backup.size)}  ${backup.numberFiles} file${backup.numberFiles === 1 ? '' : 's'}`
		);
		if (backup.appMetadata) {
			console.log(`      ${note(`metadata: ${backup.appMetadata}`)}`);
		}
	}
}

async function requireBackupId(backupDir, idStr) {
	const list = await fetchBackupList(backupDir);
	const id = Number(idStr);
	if (!list.some((b) => b.backupId === id)) {
		throw new CLIError(`Backup ${id} does not exist`);
	}
	return id;
}

async function dirSize(dir) {
	let entries;
	try {
		entries = await readdir(dir, { withFileTypes: true });
	} catch {
		return 0;
	}
	let total = 0;
	for (const entry of entries) {
		const entryPath = join(dir, entry.name);
		if (entry.isDirectory()) {
			total += await dirSize(entryPath);
		} else if (entry.isFile()) {
			try {
				total += (await stat(entryPath)).size;
			} catch {
				// File removed while walking; skip it.
			}
		}
	}
	return total;
}

function closeAllDatabases() {
	for (const [name, db] of Object.entries(dbs)) {
		db?.close();
		delete dbs[name];
	}
	currentDB = null;
}

async function openDatabases(column, ifMissing, extra) {
	currentDB = RocksDatabase.open(dbPath, openOptions(extra));
	dbs[currentDB.name] = currentDB;
	if (!column || column === currentDB.name) return;
	if (!currentDB.columns.includes(column)) {
		if (ifMissing === 'default') return;
		if (ifMissing === 'error' || openReadOnly) {
			throw new CLIError(`Column family "${column}" does not exist`);
		}
		await confirmOrCancel(`Create new column ${hl(column)}?`);
	}
	dbs[column] = RocksDatabase.open(dbPath, openOptions({ ...extra, name: column }));
	currentDB = dbs[column];
}

async function ensureDatabaseExists(create) {
	if (existsSync(join(dbPath, 'CURRENT'))) return true;
	if (openReadOnly || !create) {
		throw new CLIError(`RocksDB database does not exist: ${dbPath}`);
	}
	if (!(await confirm('RocksDB database does not exist, create it?'))) return false;
	await mkdir(dbPath, { recursive: true });
	if ((await readdir(dbPath)).length > 0) {
		return confirm('Directory not empty, are you sure you want to continue?');
	}
	return true;
}

function isEmptyPath(path) {
	try {
		return readdirSync(path).length === 0;
	} catch (error) {
		if (error.code === 'ENOENT') return true;
		throw error;
	}
}

// The native layer reads backup ids and keep counts as uint32; larger values wrap.
const isUint32 = (text) => /^\d+$/.test(text) && Number(text) <= 0xffffffff;

function validateBackupsArgs([dirArg, subcommand = 'list', extra]) {
	// A blank directory would resolve to the working directory.
	if (!dirArg.trim()) throw new CLIError('Usage: backups <dir> [subcommand] [arg]', 2);
	if (subcommand !== 'list' && subcommand !== 'ls' && !BACKUP_SUBCOMMANDS.includes(subcommand)) {
		throw new CLIError(
			`Unknown subcommand: ${subcommand}\nValid subcommands: list, ${BACKUP_SUBCOMMANDS.join(', ')}`,
			2
		);
	}
	if (['restore', 'verify', 'delete', 'purge'].includes(subcommand) && extra === undefined) {
		const argName = subcommand === 'purge' ? '<keep-count>' : '<backup-id>';
		throw new CLIError(
			`Usage: backups ${dirArg} ${subcommand} ${argName}\nList the backups with "backups ${dirArg}"`,
			2
		);
	}
	if (['restore', 'verify', 'delete'].includes(subcommand) && !isUint32(extra)) {
		throw new CLIError(`Invalid backup id: ${extra}`, 2);
	}
	// Digits only: Number('') is 0, which would purge every backup.
	if (subcommand === 'purge' && !isUint32(extra)) {
		throw new CLIError(
			`Invalid keep count: ${extra}\nUsage: backups ${dirArg} purge <keep-count>`,
			2
		);
	}
}

async function backupsCommand(args) {
	const [dirArg, subcommand = 'list', extra] = args;
	const backupDir = resolve(dirArg);

	// The backup engine creates the directory when creating a backup; everything
	// else operates on an existing backup directory.
	if (subcommand !== 'backup' && !existsSync(backupDir)) {
		throw new CLIError(
			`Backup directory ${backupDir} does not exist\nCreate a first backup with "backups ${dirArg} backup"`
		);
	}

	if (subcommand === 'list' || subcommand === 'ls') {
		try {
			return { subcommand: 'list', backups: await fetchBackupList(backupDir) };
		} catch (error) {
			throw new CLIError(`Unable to list backups: ${error.message ?? error}`);
		}
	}

	if (subcommand === 'backup') {
		const { result: backupId, elapsedMs } = await timed(() => currentDB.backup(backupDir));
		invalidateBackupListCache();
		return { subcommand, backupId, elapsedMs };
	}

	switch (subcommand) {
		case 'restore': {
			if (backupDir === dbPath) {
				throw new CLIError('Backup directory and database directory must be different');
			}
			const backupId = await requireBackupId(backupDir, extra);
			await confirmOrCancel(`Are you sure you want to restore backup ${hl(backupId)}?`);
			const preferredColumn = currentDB?.name;
			closeAllDatabases();
			let elapsedMs;
			try {
				({ elapsedMs } = await timed(() => backups.restore(backupDir, dbPath, { backupId })));
			} catch (error) {
				// Don't reopen: the destructive restore may have purged the database
				// directory and a read-write open would create a new, empty database.
				const message = `Restore failed, the database may be incomplete: ${error}`;
				if (!interactive) throw new CLIError(message);
				console.error(bad(message));
				process.exit(1);
			}
			if (interactive) {
				await openDatabases(preferredColumn, 'default');
				console.log(wrapColumns(currentDB.columns));
			}
			return { subcommand, backupId, elapsedMs };
		}

		case 'verify': {
			const backupId = await requireBackupId(backupDir, extra);
			try {
				const { elapsedMs } = await timed(() =>
					backups.verify(backupDir, backupId, { verifyWithChecksum: true })
				);
				return { subcommand, backupId, elapsedMs };
			} catch (error) {
				throw new CLIError(`Verify failed: ${error}`);
			}
		}

		case 'delete': {
			const backupId = await requireBackupId(backupDir, extra);
			const sizeBefore = await dirSize(backupDir);
			const { elapsedMs } = await timed(() => backups.delete(backupDir, backupId));
			invalidateBackupListCache();
			const recoveredBytes = Math.max(0, sizeBefore - (await dirSize(backupDir)));
			return { subcommand, backupId, recoveredBytes, elapsedMs };
		}

		case 'purge': {
			const keepCount = Number(extra);
			const sizeBefore = await dirSize(backupDir);
			const { elapsedMs } = await timed(() => backups.purge(backupDir, keepCount));
			invalidateBackupListCache();
			const recoveredBytes = Math.max(0, sizeBefore - (await dirSize(backupDir)));
			return { subcommand, keepCount, recoveredBytes, elapsedMs };
		}
	}
}

function printBackupsResult(result) {
	const elapsed = note(`(${result.elapsedMs}ms)`);
	switch (result.subcommand) {
		case 'list':
			printBackupList(result.backups);
			return;
		case 'backup':
			console.log(`Created backup ${hl(result.backupId)} ${elapsed}`);
			return;
		case 'restore':
			console.log(`Restored backup ${hl(result.backupId)} ${elapsed}`);
			return;
		case 'verify':
			console.log(`Backup ${hl(result.backupId)} verified OK ${elapsed}`);
			return;
		case 'delete':
			console.log(
				`Deleted backup ${hl(result.backupId)}, recovered ${hl(formatBytes(result.recoveredBytes))} ${elapsed}`
			);
			return;
		case 'purge':
			console.log(
				`Purged backups, keeping ${hl(result.keepCount)}, recovered ${hl(formatBytes(result.recoveredBytes))} ${elapsed}`
			);
	}
}

async function clearCommand() {
	await confirmOrCancel('Are you sure you want to clear all data?');
	const { elapsedMs } = await timed(() => currentDB.clear());
	return { column: currentDB.name, elapsedMs };
}

async function compactCommand() {
	const sizeBefore = currentDB.getDBIntProperty('rocksdb.estimate-live-data-size') ?? 0;
	const { elapsedMs } = await timed(() => currentDB.compact());
	const sizeAfter = currentDB.getDBIntProperty('rocksdb.estimate-live-data-size') ?? 0;
	return { sizeBefore, sizeAfter, elapsedMs };
}

function printCompactResult({ sizeBefore, sizeAfter, elapsedMs }) {
	const ratio =
		sizeBefore > 0
			? `reduced by ${(((sizeBefore - sizeAfter) / sizeBefore) * 100).toFixed(2)}%`
			: sizeAfter > 0
				? `increased by ${sizeAfter.toLocaleString()} bytes`
				: 'no change';
	console.log(`Compacted, ${ratio} ${note(`(${elapsedMs}ms)`)}`);
	console.log(`${hl(sizeBefore.toLocaleString())} -> ${hl(sizeAfter.toLocaleString())} bytes`);
}

async function dropCommand([column]) {
	if (!dbs.default.columns.includes(column)) {
		throw new CLIError(`Column family "${column}" does not exist`);
	}

	if (column === 'default') {
		if (interactive) console.log('You cannot drop the default column family');
		await confirmOrCancel('Are you sure you want to clear all data in "default"?');
		const { elapsedMs } = await timed(() => dbs.default.drop());
		return { column, cleared: true, elapsedMs };
	}

	const db = dbs[column] || RocksDatabase.open(dbPath, openOptions({ name: column }));
	const { elapsedMs } = await timed(() => db.drop());
	db.close();
	delete dbs[column];
	if (currentDB.name === column) {
		if (interactive) console.log(`Switching ${hl(column)} => ${hl('default')}`);
		currentDB = dbs.default;
	}
	return { column, cleared: false, elapsedMs };
}

async function getCommand([key]) {
	const value = await currentDB.get(key);
	if (value === undefined) throw new CLIError('Key not found');
	return value;
}

function printCommandTable(rows, log = console.log) {
	const width = Math.max(...rows.map((row) => row.command.length)) + 2;
	for (const { command, summary } of rows) {
		log(`${hl(command.padEnd(width))} ${summary}`);
	}
}

async function logCommand([name, file, entryStr]) {
	const logs = currentDB.listLogs();
	if (!name) {
		const stores = [];
		for (const storeName of logs) {
			const storePath = join(currentDB.path, 'transaction_logs', storeName);
			const files = [];
			for (const logFile of await readdir(storePath)) {
				if (!logFile.endsWith('.txnlog')) continue;
				files.push({ file: logFile, size: (await stat(join(storePath, logFile))).size });
			}
			stores.push({ name: storeName, files });
		}
		return { view: 'stores', stores };
	}

	if (!logs.includes(name)) throw new CLIError(`Log store "${name}" does not exist`);

	if (!file) {
		const logPath = join(currentDB.path, 'transaction_logs', name);
		const files = [];
		for (const logFile of await readdir(logPath)) {
			if (!logFile.endsWith('.txnlog')) continue;
			const log = parseTransactionLog(join(logPath, logFile), { skipData: true });
			files.push({
				file: logFile,
				size: log.size,
				version: log.version,
				timestamp: log.timestamp,
				entries: log.entries.length,
				anomalies: log.anomalies,
				entryAnomalyCount: log.entryAnomalyCount,
			});
		}
		return { view: 'files', store: name, files };
	}

	const log = parseTransactionLog(join(currentDB.path, 'transaction_logs', name, file));
	if (entryStr !== undefined) {
		const entry = Number(entryStr);
		if (!Number.isInteger(entry) || entry < 1 || entry > log.entries.length) {
			throw new CLIError(`Entry ${entryStr} does not exist`);
		}
		const { data } = log.entries[entry - 1];
		return { view: 'entry', store: name, file, entry, length: data.length, data };
	}
	return {
		view: 'entries',
		store: name,
		file,
		anomalies: log.anomalies,
		entries: log.entries.map(({ timestamp, length, flags, anomalies }, i) => ({
			entry: i + 1,
			timestamp,
			length,
			flags,
			anomalies: anomalies ?? [],
		})),
	};
}

function printLogResult(result) {
	switch (result.view) {
		case 'stores': {
			if (result.stores.length === 0) {
				console.log('No log stores found');
				return;
			}
			for (const { name, files } of result.stores) {
				const maxlen = files.reduce((max, { file }) => Math.max(max, file.length), 0);
				console.log(hl(name));
				for (const { file, size } of files) {
					console.log(`  ${file.padEnd(maxlen)} ${note(`(${formatBytes(size)})`)}`);
				}
			}
			return;
		}
		case 'files': {
			for (const log of result.files) {
				console.log(hl(log.file));
				console.log(`  Size      = ${hl(`${log.size.toLocaleString()} bytes`)}`);
				console.log(`  Version   = ${hl(log.version)}`);
				console.log(`  Timestamp = ${hl(formatTimestamp(log.timestamp))}`);
				console.log(`  Entries   = ${hl(log.entries)}`);
				const totalAnomalies = log.anomalies.length + log.entryAnomalyCount;
				if (totalAnomalies > 0) {
					console.log(`  Anomalies = ${bad(totalAnomalies)}`);
					for (const anomaly of log.anomalies) {
						console.log(`    ${bad(`! ${anomaly}`)}`);
					}
					if (log.entryAnomalyCount > 0) {
						console.log(
							`    ${bad(`! ${log.entryAnomalyCount} entr${log.entryAnomalyCount === 1 ? 'y has' : 'ies have'} anomalies (run "log ${result.store} ${log.file}" for details)`)}`
						);
					}
				}
			}
			return;
		}
		case 'entries': {
			for (const anomaly of result.anomalies) {
				console.log(bad(`!  ${anomaly}`));
			}
			for (const entry of result.entries) {
				console.log(
					`${String(entry.entry).padStart(3)})  ${hl(formatTimestamp(entry.timestamp))}  ${entry.length.toLocaleString()} bytes  ${entry.flags & 0x01 ? note('(end of transaction)') : ''}`
				);
				for (const anomaly of entry.anomalies) {
					console.log(`     ${bad(`! ${anomaly}`)}`);
				}
			}
			return;
		}
		case 'entry':
			console.log(`Entry ${result.entry} (${result.length.toLocaleString()} bytes)\n`);
			console.log(formatHexDump(result.data));
	}
}

async function purgeLogsCommand([name]) {
	await confirmOrCancel('Are you sure you want to purge logs?');
	const { result: removed, elapsedMs } = await timed(() => currentDB.purgeLogs({ name }));
	return { removed, elapsedMs };
}

async function verifyLogsCommand([name]) {
	const logs = currentDB.listLogs();
	if (name && !logs.includes(name)) {
		throw new CLIError(`Log store "${name}" does not exist`);
	}
	const stores = [];
	for (const storeName of name ? [name] : logs) {
		const storePath = join(currentDB.path, 'transaction_logs', storeName);
		const { result, elapsedMs } = await timed(() => validateTransactionLogStore(storePath));
		stores.push({ name: storeName, ...result, elapsedMs });
	}
	const invalidStores = stores.filter((store) => !store.valid).length;
	return { valid: invalidStores === 0, invalidStores, stores };
}

function printVerifyLogsResult({ invalidStores, stores }) {
	if (stores.length === 0) {
		console.log('No log stores found');
		return;
	}
	for (const store of stores) {
		console.log(
			`${hl(store.name)}  ${store.valid ? 'OK' : bad('INVALID')} ${note(`(${store.elapsedMs}ms)`)}`
		);
		for (const error of store.errors) {
			console.log(`  ${bad(`! ${error}`)}`);
		}
		for (const warning of store.warnings) {
			console.log(`  ${warn(`! ${warning}`)}`);
		}
		for (const file of store.files) {
			console.log(
				`  ${file.file}  ${file.valid ? 'OK' : bad('INVALID')}  ${note(
					`${file.entries.toLocaleString()} entr${file.entries === 1 ? 'y' : 'ies'}, ${formatBytes(file.size)}`
				)}`
			);
			for (const error of file.errors) {
				console.log(`    ${bad(`! ${error}`)}`);
			}
			for (const warning of file.warnings) {
				console.log(`    ${warn(`! ${warning}`)}`);
			}
		}
	}
	if (invalidStores > 0) {
		console.log(
			`\n${bad(`${invalidStores} log store${invalidStores === 1 ? ' is' : 's are'} invalid`)}`
		);
	}
}

async function putCommand([key, ...valueParts]) {
	let value = valueParts.join(' ');
	// The REPL splits on whitespace, so it accepts quotes; a shell has already removed them.
	if (interactive && value.startsWith('"') && value.endsWith('"')) {
		value = value.slice(1, -1);
	}
	await currentDB.put(key, value);
	return { key, value };
}

// An undecodable value throws out of the range without its key; get() reports it on its row.
async function* queryCommand([start, end]) {
	let unreadable = 0;
	let lastKey;
	let rows = currentDB.getRange({ start, end })[Symbol.iterator]();
	try {
		while (true) {
			let step;
			try {
				step = rows.next();
			} catch (error) {
				const resume =
					lastKey === undefined ? { start, end } : { start: lastKey, exclusiveStart: true, end };
				const [key] = currentDB.getKeys({ ...resume, limit: 1 });
				if (key === undefined) throw error;
				try {
					await currentDB.get(key);
				} catch (readError) {
					unreadable++;
					lastKey = key;
					rows.return();
					rows = currentDB.getRange({ start: key, exclusiveStart: true, end })[Symbol.iterator]();
					yield { key, error: String(readError) };
					continue;
				}
				throw error;
			}
			if (step.done) break;
			lastKey = step.value.key;
			yield step.value;
		}
	} finally {
		rows.return();
	}
	if (unreadable > 0 && !interactive) {
		process.exitCode = 1;
		if (argv.json) reportError(new CLIError(`${unreadable} row(s) could not be read`));
	}
}

async function printQueryResult(rows) {
	const { result: count, elapsedMs } = await timed(async () => {
		let n = 0;
		for await (const { key, value, error } of rows) {
			if (error === undefined) {
				console.log(`${str(key)} = ${inspect(value, { colors: useColors, depth: 10 })}`);
			} else {
				console.error(`${str(key)} = ${bad(error)}`);
			}
			n++;
		}
		return n;
	});
	console.log(`\n${count} results ${note(`(${elapsedMs}ms)`)}`);
}

async function removeCommand([key]) {
	const { elapsedMs } = await timed(() => currentDB.remove(key));
	return { key, elapsedMs };
}

async function replCommand() {
	// A non-TTY stdin can't feed the sub-REPL interactively, and input already read out
	// of it has no defined recipient once this CLI's interface closes.
	if (!stdin.isTTY) {
		console.error(bad('The JS sub-REPL needs an interactive terminal; stdin is not one here.\n'));
		process.exit(1);
	}
	const history = [...rl.history];
	rl.close();
	r = repl.start({ prompt: styleText('magenta', 'js> '), useColors: true, useGlobal: false });
	Object.defineProperty(r.context, 'db', { get: () => currentDB, enumerable: true });
	r.on('SIGINT', () => r.close());
	await new Promise((resolve) => r.on('exit', resolve));
	r = null;
	startRL();
	rl.history = history;
}

async function useCommand([name]) {
	if (!name || name === currentDB.name) {
		currentDB = name ? currentDB : dbs.default;
		console.log(hl(currentDB.name));
		return;
	}
	if (!dbs[name]) {
		if (currentDB.columns.includes(name)) {
			console.log(`Switching ${hl(currentDB.name)} => ${hl(name)}`);
		} else {
			if (openReadOnly) throw new CLIError(`Column family "${name}" does not exist`);
			await confirmOrCancel(`Create new column ${hl(name)}?`);
			console.log(`Created ${hl(name)}`);
		}
		dbs[name] = RocksDatabase.open(dbPath, openOptions({ name }));
	} else {
		console.log(`Switching ${hl(currentDB.name)} => ${hl(name)}`);
	}
	currentDB = dbs[name];
}

// A function, not an arrow: `this[key]` is the original value, before Buffer#toJSON ran.
function toJSON(key, value) {
	const original = this[key];
	if (ArrayBuffer.isView(original)) {
		return Buffer.from(original.buffer, original.byteOffset, original.byteLength).toString('hex');
	}
	if (typeof value === 'bigint') return value.toString();
	if (value instanceof Map) return Object.fromEntries(value);
	if (value instanceof Set) return [...value];
	return value;
}

function writeJSON(stream, value) {
	stream.write(`${JSON.stringify(value, toJSON)}\n`);
}

async function emitResult(def, result) {
	if (argv.json) {
		if (result?.[Symbol.asyncIterator]) {
			let first = true;
			stdout.write('[');
			for await (const item of result) {
				if (!stdout.write(`${first ? '' : ','}\n${JSON.stringify(item, toJSON)}`)) {
					await once(stdout, 'drain');
				}
				first = false;
			}
			// Closed only on success: a scan that fails partway leaves the array open, so a
			// JSON consumer fails instead of accepting a truncated result as complete.
			stdout.write(first ? ']\n' : '\n]\n');
		} else {
			writeJSON(stdout, result ?? null);
		}
		return;
	}
	if (def.print && result !== undefined) await def.print(result);
}

function reportError(error) {
	const message = error instanceof CLIError ? error.message : String(error);
	if (argv.json) {
		writeJSON(stderr, { error: stripVTControlCharacters(message), code: error.code });
	} else {
		console.error(styleText('red', message, { stream: stderr }));
	}
}

function assertAllowed(def, access) {
	if (argv.readonly && access === 'write') {
		throw new CLIError(
			`"${def.name}" writes to the database and is not available in read-only mode`,
			2
		);
	}
}

function assertUsage(def, args) {
	const required = def.usage?.match(/<[^>]+>/g)?.length ?? 0;
	// A blank required argument (an unset shell variable) must never reach a default:
	// `purge-logs ""` would name every log store. Only put's value may be empty.
	const blank = args
		.slice(0, required)
		.some((arg, i) => !arg.trim() && !(def.emptyValueAllowed && i === required - 1));
	if (args.length < required || blank) {
		throw new CLIError(`Usage: ${commandSignature(def.name, def.usage)}`, 2);
	}
	def.validate?.(args);
}

async function runCommand(def, args) {
	assertUsage(def, args);
	assertAllowed(def, resolveAccess(def, args));
	const result = await def.run(args);
	if (!interactive && def.failed?.(result)) process.exitCode = 1;
	await emitResult(def, result);
}

function printHelp(log = console.log) {
	log(`Usage:
  ${hl('rocksdb-js [dbpath] [options]')}                    Start the interactive REPL
  ${hl('rocksdb-js [dbpath] <command> [args...] [options]')} Run one command and exit

${hl('dbpath')} defaults to the current directory. If the first argument is a command name, the
current directory is used; address a database directory that shares a command's name as
${hl('./<name>')}. Put ${hl('--')} before arguments that start with "-".

A one-shot command prints its result to stdout and errors to stderr, and exits 0 on
success, 1 on failure, and 2 on a usage error. Commands that only read open the database
read-only: a point-in-time read that does not need the lock another process holds, but
that can fail if that process is compacting at the time.

Options (-c, --json and -y need a command; without one they are a usage error):
  ${hl('-c, --column <name>')}  Column family to use (default: "default")
  ${hl('-h, --help')}           Show this help message
  ${hl('    --json')}           Print results (and errors, to stderr) as JSON
  ${hl('-r, --readonly')}       Open the database in read-only mode (REPL or command)
  ${hl('-v, --version')}        Show the version information
  ${hl('-y, --yes')}            Answer "yes" to confirmation prompts; required for a
                       destructive command when stdin is not a terminal

Commands:`);
	printCommandTable(
		COMMANDS.map(({ name, usage, summary, replOnly }) => ({
			command: commandSignature(name, usage),
			summary: replOnly ? `${summary} (REPL only)` : summary,
		})),
		log
	);
	log(`
Examples:
  ${hl('rocksdb-js ./data')}                        Open the REPL on ./data
  ${hl('rocksdb-js ./data count')}                  Count the keys in the default column family
  ${hl('rocksdb-js ./data get user:1 --json')}      Print one value as JSON
  ${hl('rocksdb-js ./data -c users query a m')}     Query a range of the "users" column family
  ${hl('rocksdb-js ./data verify-logs')}            Exit non-zero if any transaction log is invalid
  ${hl('rocksdb-js ./data backups ./bk backup')}    Create a backup in ./bk`);
}

function versionString() {
	return `rocksdb.js CLI v${versions['rocksdb-js']} (RocksDB v${versions.rocksdb})`;
}

async function openOrExplain(ifMissing, extra) {
	try {
		await openDatabases(argv.column, ifMissing, extra);
	} catch (error) {
		if (error?.code !== 'ERR_DATABASE_LOCKED') throw error;
		throw Object.assign(
			new CLIError(
				interactive
					? `RocksDB is already open by another process; rerun with --readonly:\n  rocksdb-js --readonly ${dbPath}`
					: 'RocksDB is already open by another process'
			),
			{ code: error.code }
		);
	}
}

async function runOnce() {
	const def = commandsByName.get(commandName);
	if (!def) {
		throw new CLIError(`Unknown command: ${commandName}\nRun "rocksdb-js --help" for usage.`, 2);
	}
	if (def.replOnly) {
		throw new CLIError(`"${def.name}" is only available in the REPL`, 2);
	}
	assertUsage(def, commandArgs);
	const access = resolveAccess(def, commandArgs);
	assertAllowed(def, access);
	if (def.opens ? def.opens(commandArgs) : access !== 'none') {
		openReadOnly = argv.readonly || (access === 'read' && !def.locks?.(commandArgs));
		if (!(await ensureDatabaseExists(Boolean(def.creates)))) throw new CLIError('Cancelled');
		await openOrExplain(def.creates ? 'create' : 'error', def.openOptions?.(commandArgs));
	}
	await runCommand(def, commandArgs);
}

async function runREPL() {
	startRL();
	console.log(versionString());
	console.log();
	if (argv.readonly) {
		console.log(`Mode:    ${warn('READ-ONLY')}`);
	}
	console.log(`Path:    ${hl(dbPath)}`);

	if (!existsSync(join(dbPath, 'CURRENT'))) console.log();
	if (!(await ensureDatabaseExists(true))) return;
	await openOrExplain('create');

	console.log(wrapColumns(currentDB.columns) + '\n');
	console.log(`Type "help" for more information.`);

	while (true) {
		const raw = await ask('> ');
		if (raw === null) break;
		const line = raw.trim().split(/[ \t]+/);
		const command = line[0];
		if (!command) continue;
		const def = commandsByName.get(command);
		console.log();
		if (def) {
			try {
				await runCommand(def, line.slice(1));
			} catch (error) {
				reportError(error);
			}
		} else {
			console.error(bad(`Unknown command: ${command}`));
		}
		console.log();
	}
}

async function main() {
	// `rocksdb-js db query | head` closes the pipe early; that is not a failure.
	stdout.on('error', (error) => {
		if (error.code !== 'EPIPE') throw error;
		let exitCode = process.exitCode ?? 0;
		try {
			closeAllDatabases();
		} catch (closeError) {
			reportError(closeError);
			exitCode = 1;
		}
		process.exit(exitCode);
	});

	if (argv.help) {
		console.log(`${versionString()}\n`);
		printHelp();
		return;
	}
	if (argv.version) {
		if (argv.json) {
			writeJSON(stdout, { 'rocksdb-js': versions['rocksdb-js'], rocksdb: versions.rocksdb });
		} else {
			console.log(versionString());
		}
		return;
	}

	// An explicitly empty path or column must not fall through to the default.
	const blankOption =
		dbArg !== undefined && !dbArg.trim()
			? 'Usage: the database path must not be empty (omit it to use the current directory)'
			: argv.column !== undefined && !argv.column.trim()
				? 'Usage: --column needs a column family name'
				: undefined;
	if (blankOption) {
		reportError(new CLIError(blankOption));
		process.exitCode = 2;
		return;
	}

	const oneShotOptions = ['column', 'json', 'yes'].filter((name) => argv[name] !== undefined);
	if (interactive && oneShotOptions.length > 0) {
		const flags = oneShotOptions.map((name) => `--${name}`).join(', ');
		console.error(`Error: specify a command (${flags} only applies to a one-shot command)\n`);
		printHelp(console.error);
		process.exitCode = 2;
		return;
	}

	try {
		await (interactive ? runREPL() : runOnce());
	} catch (error) {
		if (!interactive || error.code !== 'ABORT_ERR') {
			reportError(error);
			process.exitCode = error.exitCode ?? 1;
		}
	} finally {
		r?.close();
		closeAllDatabases();
		rl?.close();
	}
}

main();
