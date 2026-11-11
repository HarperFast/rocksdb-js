// Child for test/child-process.test.ts: `exit` writes to both streams and exits 3;
// `hang` reports READY and keeps its event loop alive until killed.
const mode = process.argv[2];

process.stderr.write('probe stderr\n');
if (mode === 'exit') {
	process.stdout.write('probe stdout\n');
	process.exitCode = 3;
} else {
	process.stdout.write('READY\n');
	setInterval(() => {}, 1000);
}
