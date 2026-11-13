const mode = process.argv[2];

process.stderr.write('probe stderr\n');
if (mode === 'exit') {
	process.stdout.write('probe stdout\n');
	process.exitCode = 3;
} else {
	process.stdout.write('READY\n');
	setInterval(() => {}, 1000);
}
