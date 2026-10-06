import { type RocksDatabase, Transaction } from '../../src/index.ts';

export type Park = {
	transaction: Transaction;
	commit: Promise<unknown>;
	start: number;
};

function value(version: number): Buffer {
	const buffer = Buffer.alloc(16);
	buffer.writeDoubleBE(version, 0);
	return buffer;
}

/** Stages a write to `key`, so its verification-table slot stays locked until abort or commit. */
export function holdLock(db: RocksDatabase, key: Buffer): Transaction {
	// The process-wide table is created on its first version lookup; until then writes lock nothing.
	db.populateVersion(key, 1.6e12);
	const holder = new Transaction(db.store, { coordinatedRetry: true });
	holder.putSync(key, value(2.1e12));
	return holder;
}

/**
 * Commits a coordinated-retry transaction that conflicts on `key` (written after its read
 * snapshot), so it parks behind whatever holds `key`'s slot.
 */
export async function parkBehind(db: RocksDatabase, key: Buffer): Promise<Park> {
	const transaction = new Transaction(db.store, { coordinatedRetry: true });
	await transaction.get(key);
	await db.put(key, value(2.2e12));
	transaction.putSync(key, value(2.3e12));
	const start = performance.now();
	return { transaction, commit: transaction.commit(), start };
}
