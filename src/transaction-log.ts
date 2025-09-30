import { NativeTransactionLog } from './load-binding';

export class TransactionLog {
	#log: NativeTransactionLog;

	constructor(path: string) {
		this.#log = new NativeTransactionLog(path);
	}

	add() {
		// add an entry to the transaction log
	}

	commit() {
		// do we need this?
		// what does this do?
	}

	query() {
		// this is basically getRange()
	}
}
