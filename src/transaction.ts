import { DBI } from './dbi';
import { Store } from './store.js';
import { Txn } from './util/load-binding.js';

export class Transaction extends DBI {
	txn: Txn;

	constructor(store: Store) {
		super(store);
		this.txn = new Txn();
	}

	async abort() {
		//
	}
	
	async commit() {
		//
	}
}
