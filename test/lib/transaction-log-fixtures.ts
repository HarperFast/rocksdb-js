import { mkdirSync, readdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

export interface LazyTransactionLogSegment {
	path: string;
	sequence: number;
}

export function writeLazyTransactionLogSegments(
	logDirectory: string,
	fileHeaderSize: number,
	entryHeaderSize: number,
	token: number
): LazyTransactionLogSegment {
	mkdirSync(logDirectory, { recursive: true });
	const header = Buffer.alloc(fileHeaderSize);
	header.writeUInt32BE(token, 0);
	header.writeUInt8(1, 4);
	header.writeDoubleBE(Date.now(), 5);

	const entry = Buffer.alloc(entryHeaderSize + 1);
	entry.writeDoubleBE(Date.now(), 0);
	entry.writeUInt32BE(1, 8);
	entry.writeUInt8(1, 12);

	for (let sequence = 64; sequence >= 1; sequence--) {
		const contents = sequence === 64 ? Buffer.concat([header, entry]) : header;
		writeFileSync(join(logDirectory, `${sequence}.txnlog`), contents);
	}

	let highestSeen = 0;
	for (const file of readdirSync(logDirectory)) {
		const sequence = Number.parseInt(file, 10);
		if (sequence < highestSeen) {
			return { path: join(logDirectory, file), sequence };
		}
		highestSeen = sequence;
	}
	throw new Error('Could not construct a lazily discovered transaction log segment');
}

export interface CraftedEntry {
	timestamp: number;
	flags: number;
	data: string;
}

export function writeCraftedSegment(
	path: string,
	entries: CraftedEntry[],
	fileHeaderSize: number,
	entryHeaderSize: number,
	token: number
): void {
	const header = Buffer.alloc(fileHeaderSize);
	header.writeUInt32BE(token, 0);
	header.writeUInt8(1, 4);
	header.writeDoubleBE(entries[0]?.timestamp ?? Date.now(), 5);
	const parts = [header];
	for (const entry of entries) {
		const data = Buffer.from(entry.data);
		const entryHeader = Buffer.alloc(entryHeaderSize);
		entryHeader.writeDoubleBE(entry.timestamp, 0);
		entryHeader.writeUInt32BE(data.length, 8);
		entryHeader.writeUInt8(entry.flags, 12);
		parts.push(entryHeader, data);
	}
	writeFileSync(path, Buffer.concat(parts));
}
