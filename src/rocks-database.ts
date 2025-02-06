export class RocksDatabase {
	path: string;
	state: 'open' | 'closed' | 'opening' | 'closing' = 'closed';

	constructor(path: string) {
		this.path = path;
	}

	async open() {
		if (this.state !== 'open') {
			// do open
		}

		return this;
	}
}

