import { join } from 'node:path';
import { tmpdir } from 'node:os';

export function generateDBPath() {
	return join(
		tmpdir(),
		`testdb-${Math.random().toString(36).substring(2, 15)}`
	);
}
