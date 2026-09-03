import { RocksDatabase } from '../../src/index.ts';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

// Usage: fork-wbm-reopen-stall.mts <dbPath> <create|reopen> <mode> [maintain]
//
// `create` builds the column families and exits; `reopen` opens the same families again — the
// restart this regression is about — reports the per-family `max_write_buffer_size_to_maintain`
// RocksDB actually applied, then writes well past the WriteBufferManager budget.
//
// It runs as a child process because the failure mode is a HANG, not an exception: `put()` calls
// `store.putSync()` before returning its promise, so a stalled write blocks the JS thread and no
// in-process timer can fire. Only the parent's kill is a deadline that survives that.
const [dbPath, phase, mode, maintainArg] = process.argv.slice(2);

if (!dbPath || (phase !== 'create' && phase !== 'reopen')) {
	console.error('Usage: fork-wbm-reopen-stall.mts <dbPath> <create|reopen> <mode> [maintain]');
	process.exit(1);
}

const COLUMN_FAMILIES = 4;
const WRITE_BUFFER_SIZE = 1024 * 1024;
// Comfortably above the post-fix retention (one flushed memtable per family) and far below the
// pre-fix one (`maxWriteBufferNumber` 16 * `writeBufferSize` = 16MB per family).
const BUDGET = 32 * 1024 * 1024;
const value = Buffer.alloc(8 * 1024, 1);

RocksDatabase.config({
	writeBufferManagerSize: BUDGET,
	writeBufferManagerAllowStall: true,
	writeBufferManagerCostToCache: true,
});

const options: Record<string, unknown> = {
	writeBufferSize: WRITE_BUFFER_SIZE,
	pessimistic: mode === 'pessimistic',
};
if (maintainArg !== undefined) {
	options.maxWriteBufferSizeToMaintain = Number(maintainArg);
}

const databases = Array.from({ length: COLUMN_FAMILIES }, (_, i) =>
	RocksDatabase.open(join(dbPath, 'db'), { ...options, name: `cf${i}` } as never)
);

// RocksDB rotates LOG on every open, so the live file holds this open's options blocks only.
function maintainPerColumnFamily(): Record<string, number> {
	const out: Record<string, number> = {};
	const text = readFileSync(join(dbPath, 'db', 'LOG'), 'utf8');
	for (const match of text.matchAll(
		/Options for column family \[([^\]]+)\][\s\S]*?max_write_buffer_size_to_maintain: (-?\d+)/g
	)) {
		out[match[1]] = Number(match[2]);
	}
	return out;
}

try {
	if (phase === 'create') {
		for (let i = 0; i < 128; i++) {
			databases[i % COLUMN_FAMILIES].putSync(Buffer.from(`create-${i}`), value);
		}
	} else {
		console.log(`MAINTAIN ${JSON.stringify(maintainPerColumnFamily())}`);
		// 48MB across the families: more than the budget several times over, so retained history
		// the budget cannot release wedges every writer here and this loop never returns.
		let written = 0;
		for (let i = 0; written < 48 * 1024 * 1024; i++) {
			databases[i % COLUMN_FAMILIES].putSync(Buffer.from(`reopen-${i}`), value);
			written += value.length;
		}
		console.log('WROTE');
	}
} finally {
	for (const db of databases) db.close();
}
