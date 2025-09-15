import { type NativeDatabase, NativeTransactionLog } from './load-binding';

export class TransactionLog {
	#log: NativeTransactionLog;

	constructor(context: NativeDatabase) {
		this.#log = new NativeTransactionLog(context);
	}

	getRange() {
		//
	}
}
