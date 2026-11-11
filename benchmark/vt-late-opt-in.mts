/**
 * Verification-table participation microbenchmark: the cost of a late `verificationTable: true`
 * open that converts a column family other handles hold without the option, and of reads and
 * writes through a converted handle, against a handle that participated from its first open.
 *
 *   node --expose-gc benchmark/vt-late-opt-in.mts [--phase <group>]... [--keys 10000] [--rounds 5]
 *
 * Groups (`--phase`) and what each prints (ns/op per round, then the median):
 *   - open-native: a `verificationTable: true` open of a column family another opted-in handle
 *     holds (no conversion).
 *   - open-late: the same open while a handle without the option holds the column family, which
 *     converts it (on a build without conversion this throws, so it reports `unsupported`).
 *   - native / converted: through a handle that participated from its first open, or one a later
 *     opt-in converted: `fresh-*` is `getSync(key, { expectedVersion })` answered FRESH from the
 *     table, `populate-*` a `getSync(key, { populateVersion: true })` read (slots already
 *     seeded), and `txn-native` a one-key transaction.
 *   - write: `put-plain` / `txn-plain`, `putSync` and a one-key transaction through a handle on a
 *     column family nothing has opted in to.
 *
 * Runs against `src` (native type stripping), so the same file measures whichever binary sits in
 * `build/Release`. Run it under `bench-run`.
 */
import { RocksDatabase } from '../src/index.ts';
import { FRESH_VERSION_FLAG } from '../src/load-binding.ts';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { parseArgs } from 'node:util';

const { values: args } = parseArgs({
	options: {
		phase: { type: 'string', multiple: true },
		keys: { type: 'string', default: '10000' },
		rounds: { type: 'string', default: '5' },
		opens: { type: 'string', default: '200' },
	},
});
const KEYS = Number(args.keys);
const ROUNDS = Number(args.rounds);
const OPENS = Number(args.opens);
const VERSION = 1.7e12;

const root = mkdtempSync(join(process.env.BENCH_TMPDIR ?? tmpdir(), 'vt-late-opt-in-'));
const path = join(root, 'db');

function value(version: number): Buffer {
	const buffer = Buffer.alloc(64);
	buffer.writeDoubleBE(version, 0);
	return buffer;
}

const keys = Array.from({ length: KEYS }, (_, i) =>
	Buffer.from(`key-${String(i).padStart(8, '0')}`)
);

function median(values: number[]): number {
	const sorted = [...values].sort((a, b) => a - b);
	return sorted[Math.floor(sorted.length / 2)];
}

function report(name: string, perRound: number[]): void {
	console.log(
		`${name.padEnd(20)} median ${median(perRound).toFixed(1).padStart(9)} ns/op  rounds ${perRound.map((n) => n.toFixed(1)).join(' ')}`
	);
}

function timeRounds(name: string, ops: number, run: () => void): void {
	run();
	const perRound: number[] = [];
	for (let round = 0; round < ROUNDS; round++) {
		globalThis.gc?.();
		const start = process.hrtime.bigint();
		run();
		perRound.push(Number(process.hrtime.bigint() - start) / ops);
	}
	report(name, perRound);
}

const keepAlive = new RocksDatabase(path, { encoding: false }).open();

function seedFamily(db: RocksDatabase): void {
	for (const key of keys) db.putSync(key, value(VERSION));
}

let openSequence = 0;
function openRounds(name: string, holdPlain: boolean): void {
	const perRound: number[] = [];
	for (let round = 0; round <= ROUNDS; round++) {
		const plains: RocksDatabase[] = [];
		const names: string[] = [];
		for (let i = 0; i < OPENS; i++) {
			const familyName = `cf-${openSequence++}`;
			names.push(familyName);
			const holder = new RocksDatabase(path, {
				encoding: false,
				name: familyName,
				verificationTable: holdPlain ? undefined : true,
			}).open();
			holder.putSync(keys[0], value(VERSION));
			plains.push(holder);
		}
		const opened: RocksDatabase[] = [];
		globalThis.gc?.();
		const start = process.hrtime.bigint();
		try {
			for (const familyName of names) {
				opened.push(
					new RocksDatabase(path, {
						encoding: false,
						name: familyName,
						verificationTable: true,
					}).open()
				);
			}
		} catch (error) {
			if ((error as { code?: string }).code === 'ERR_VERIFICATION_TABLE_CONFLICT') {
				console.log(`${name.padEnd(20)} unsupported (this build refuses the opt-in)`);
				for (const db of [...opened, ...plains]) db.close();
				return;
			}
			throw error;
		}
		const elapsed = Number(process.hrtime.bigint() - start) / OPENS;
		if (round > 0) perRound.push(elapsed);
		for (const db of [...opened, ...plains]) db.close();
	}
	report(name, perRound);
}

function readPhases(label: 'native' | 'converted'): void {
	const familyName = `read-${label}`;
	let plain: RocksDatabase | undefined;
	let reader: RocksDatabase;
	if (label === 'converted') {
		plain = new RocksDatabase(path, { encoding: false, name: familyName }).open();
		seedFamily(plain);
		const cached = new RocksDatabase(path, {
			encoding: false,
			name: familyName,
			verificationTable: true,
		});
		try {
			cached.open();
		} catch (error) {
			if ((error as { code?: string }).code === 'ERR_VERIFICATION_TABLE_CONFLICT') {
				console.log(`fresh-${label}`.padEnd(20) + ' unsupported (this build refuses the opt-in)');
				plain.close();
				return;
			}
			throw error;
		}
		reader = plain;
		cached.close();
	} else {
		reader = new RocksDatabase(path, {
			encoding: false,
			name: familyName,
			verificationTable: true,
		}).open();
		seedFamily(reader);
	}
	for (const key of keys) reader.getSync(key, { populateVersion: true });
	// Keys sharing a slot answer FRESH from a database read instead; time table hits only.
	const tableHits = keys.filter((key) => reader.verifyVersion(key, VERSION));
	timeRounds(`fresh-${label}`, tableHits.length, () => {
		for (const key of tableHits) {
			if (reader.getSync(key, { expectedVersion: VERSION }) !== FRESH_VERSION_FLAG) {
				throw new Error('expected FRESH');
			}
		}
	});
	timeRounds(`populate-${label}`, KEYS, () => {
		for (const key of keys) reader.getSync(key, { populateVersion: true });
	});
	if (label === 'native') {
		timeRounds('txn-native', KEYS, () => {
			for (const key of keys)
				reader.transactionSync((txn) => reader.putSync(key, value(VERSION), { transaction: txn }));
		});
	}
	reader.close();
	plain?.close();
}

function writePhases(): void {
	const plain = new RocksDatabase(path, { encoding: false, name: 'write-plain' }).open();
	seedFamily(plain);
	timeRounds('put-plain', KEYS, () => {
		for (const key of keys) plain.putSync(key, value(VERSION));
	});
	timeRounds('txn-plain', KEYS, () => {
		for (const key of keys)
			plain.transactionSync((txn) => plain.putSync(key, value(VERSION), { transaction: txn }));
	});
	plain.close();
}

const phases = new Set(args.phase ?? ['open-native', 'open-late', 'native', 'converted', 'write']);
try {
	if (phases.has('open-native')) openRounds('open-native', false);
	if (phases.has('open-late')) openRounds('open-late', true);
	if (phases.has('native')) readPhases('native');
	if (phases.has('converted')) readPhases('converted');
	if (phases.has('write')) writePhases();
} finally {
	keepAlive.close();
	rmSync(root, { recursive: true, force: true });
}
