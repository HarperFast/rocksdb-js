import { RocksDatabase } from '../../src/index.ts';
import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

// Usage: fork-wbm-arena-block-size.mts <rootDir>
//
// A child process because the WriteBufferManager is process-global: these cases need a process
// that has never had one, then step its budget, which would leak into other test files sharing the
// Vitest process. Prints one `ARENA <json>` line mapping each case to the per-column-family
// `arena_block_size` RocksDB persisted in the database's latest OPTIONS file.
const root = process.argv[2];
if (!root) {
	console.error('Usage: fork-wbm-arena-block-size.mts <rootDir>');
	process.exit(1);
}

const KiB = 1024;
const MiB = 1024 * KiB;

function persistedArenaBlockSizes(dbPath: string): Record<string, number> {
	const latest = readdirSync(dbPath)
		.filter((name) => name.startsWith('OPTIONS-'))
		.sort((a, b) => Number(a.slice(8)) - Number(b.slice(8)))
		.at(-1);
	if (!latest) throw new Error(`no OPTIONS file in ${dbPath}`);
	const sizes: Record<string, number> = {};
	let family: string | undefined;
	for (const line of readFileSync(join(dbPath, latest), 'utf8').split('\n')) {
		const section = /^\[CFOptions "([^"]*)"\]/.exec(line);
		if (section) {
			family = section[1];
			continue;
		}
		const match = /^\s*arena_block_size=(\d+)/.exec(line);
		if (match && family !== undefined) sizes[family] = Number(match[1]);
	}
	return sizes;
}

const results: Record<string, Record<string, number>> = {};

function openCase(caseName: string, options: Record<string, unknown> = {}): RocksDatabase {
	const dbPath = join(root, caseName);
	const db = RocksDatabase.open(dbPath, options);
	db.putSync('k', 'v');
	results[caseName] = persistedArenaBlockSizes(dbPath);
	return db;
}

openCase('noManager').close();
openCase('noManagerSmallWriteBuffer', { writeBufferSize: 256 * KiB }).close();

RocksDatabase.config({ writeBufferManagerSize: 64 * MiB });
openCase('budget64MiB').close();
openCase('budget64MiBSmallWriteBuffer', { writeBufferSize: 256 * KiB }).close();
openCase('budget64MiBUnalignedWriteBuffer', { writeBufferSize: 100_000 }).close();
openCase('budget64MiBExplicit', { arenaBlockSize: 200_000 }).close();

RocksDatabase.config({ writeBufferManagerSize: 512 * MiB });
const live = openCase('budget512MiB');

RocksDatabase.config({ writeBufferManagerSize: 4096 * MiB });
openCase('budget4GiB').close();

// A family created on an already-open database uses the budget of the manager that database holds,
// read at creation; the existing family keeps what it was opened with.
RocksDatabase.config({ writeBufferManagerSize: 160 * MiB });
const late = RocksDatabase.open(join(root, 'budget512MiB'), { name: 'late' });
late.putSync('k', 'v');
results.lateFamily = persistedArenaBlockSizes(join(root, 'budget512MiB'));

// A runtime size of 0 stops new databases attaching the manager, but one that already holds it is
// still charged against the live budget, so a family it creates still scales with that budget.
RocksDatabase.config({ writeBufferManagerSize: 0 });
const detached = RocksDatabase.open(join(root, 'budget512MiB'), { name: 'afterDisable' });
detached.putSync('k', 'v');
results.afterDisableFamily = persistedArenaBlockSizes(join(root, 'budget512MiB'));
openCase('afterDisableNewDatabase').close();
openCase('afterDisableExplicit', { arenaBlockSize: 300_000 }).close();

detached.close();
late.close();
live.close();

console.log(`ARENA ${JSON.stringify(results)}`);
