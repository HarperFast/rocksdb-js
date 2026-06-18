#!/usr/bin/env bash
# macOS-local repro harness for the intermittent benchmark heap corruption
# ("malloc(): unaligned tcache chunk detected" on Linux CI). AddressSanitizer
# is unusable on this host (the ASan runtime hangs at init even for a trivial
# binary), so we use Apple's Guard Malloc (libgmalloc) instead: each allocation
# gets its own page with a guard page after it, so an out-of-bounds write faults
# immediately, and freed pages are unmapped so a use-after-free faults too.
#
# Default (no MALLOC_PROTECT_BEFORE) places each allocation flush against the
# trailing guard page → catches overflow past the end (the usual tcache
# corruptor). MallocScribble fills freed memory and, with the page unmap,
# surfaces use-after-free.
#
# Requires the plain (non-ASan) Release .node already built.
set -uo pipefail

export DYLD_INSERT_LIBRARIES=/usr/lib/libgmalloc.dylib
export MallocScribble=1
export MallocGuardEdges=1
export CI=1

ITERS="${ITERS:-40}"
BENCHES="${BENCHES:-benchmark/worker-put-sync.bench.ts benchmark/worker-get-sync.bench.ts benchmark/worker-transaction-log.bench.ts benchmark/realistic-load.bench.ts benchmark/transaction-log.bench.ts}"
LOG="${LOG:-gmalloc-fail.log}"

for i in $(seq 1 "$ITERS"); do
	echo "=== gmalloc iteration $i / $ITERS @ $(date +%T) ==="
	out="$(node --expose-gc ./node_modules/vitest/vitest.mjs bench --run $BENCHES 2>&1)"
	code=$?
	if [ $code -ne 0 ] || printf '%s' "$out" | grep -qiE "GuardMalloc.*(pointer being freed|bounds|already|not allocated)|malloc: \*\*\*|Segmentation fault|SIGSEGV|SIGBUS|EXC_BAD_ACCESS|signal SIGABRT|unaligned"; then
		printf '%s\n' "$out" > "$LOG"
		echo "!!! crash/guard fault on iteration $i (exit=$code) — see $LOG"
		exit 1
	fi
done
echo "completed $ITERS iterations with no fault"
