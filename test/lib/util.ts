import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { randomBytes } from 'node:crypto';

export function generateDBPath() {
	return join(
		tmpdir(),
		`testdb-${randomBytes(8).toString('hex')}`
	);
}