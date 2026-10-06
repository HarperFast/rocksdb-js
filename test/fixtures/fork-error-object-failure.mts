import { backups } from '../../src/index.ts';

// The native error builder creates the JS Error through the global Object.create, so replacing it
// fails that builder on every call. Run in a child process so the patched global cannot leak.
const missingDir = process.argv[2];
const realCreate = Object.create;

const sentinel = { sentinel: 'object-create-threw' };
Object.create = () => {
	throw sentinel;
};
try {
	await backups.list(missingDir).then(
		() => {
			throw new Error('Expected backups.list to reject');
		},
		(error) => {
			if (error !== sentinel) throw new Error(`Expected the thrown value, got ${String(error)}`);
		}
	);
} finally {
	Object.create = realCreate;
}

Object.create = 0 as unknown as typeof Object.create;
try {
	await backups.list(missingDir).then(
		() => {
			throw new Error('Expected backups.list to reject');
		},
		(error) => {
			if (!(error instanceof Error)) throw new Error(`Expected an Error, got ${String(error)}`);
		}
	);
} finally {
	Object.create = realCreate;
}

console.log('settled');
