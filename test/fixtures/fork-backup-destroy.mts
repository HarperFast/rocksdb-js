import { RocksDatabase } from '../../src/index.ts';
import { rmSync } from 'node:fs';

const path = process.argv[2];
const backupPath = `${path}-backup`;
const db = RocksDatabase.open(path);
db.putSync('key', 'value');

try {
	const backup = db.backup(backupPath);
	const startedAt = Date.now();
	db.destroy();
	const destroyDuration = Date.now() - startedAt;
	await backup;
	if (destroyDuration < 400) {
		throw new Error(`Destroy did not wait for the directory backup (${destroyDuration}ms)`);
	}
} finally {
	rmSync(backupPath, { force: true, recursive: true });
}
