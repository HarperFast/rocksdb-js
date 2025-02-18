// import { commit } from './util/load-binding.js';
export class Transaction {
	transactionPointer: number
	constructor(pointer: number) {
		this.transactionPointer = pointer;
	}
	commit(): Promise<void> {
		// primarily handled by native bindings which should write all the transaction log entries and then commit the
		// transaction
		return new Promise((resolve, reject) => commit(this.transactionPointer, (error) => {
			if (error) reject(error);
			else resolve();
		}));
	}
}