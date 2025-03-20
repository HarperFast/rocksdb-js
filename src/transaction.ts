import { DBI } from './dbi';
import { Store } from './store.js';
import type { NativeTransaction } from './util/load-binding.js';

export class Transaction extends DBI {
	txn: NativeTransaction;

	constructor(store: Store) {
		super(store);
		this.txn = store.db.createTransaction();
	}

	async abort() {
		//
	}
	
	async commit() {
		//
	}
}
