# Mutex contention investigation (#462)

Measured on 2026-09-16 against main `b4d104562e7d5353b6d7a4412e0a8b48ee805062`:
macOS 26.6.2 arm64, 14 logical CPUs, 36 GiB RAM, Node 24.16.0, RocksDB 11.8.1,
Release native binding, local APFS storage. This is a local workload study, not a
cross-platform capacity claim.

## Finding and change

The strongest steady-state contention was the descriptor's shared commit-completion
mutex. It serialized registration, dispatch, and accounting for otherwise independent
worker environments. Each environment now has a shared completion object with its own
mutex. A DBHandle caches that object on its owning JS thread; each dispatched commit
retains it until completion. Reopening the same native handle resets the cache.

This preserves mutual exclusion between TSFN calls and env cleanup. Atomics on the
pending count or TSFN pointer alone cannot keep Node from freeing the TSFN during a
call. Cold release paths retain the registry lock until the completion is released,
so a concurrent env-cleanup hook cannot miss an entry being released. The sole nested
lock order is registry → completion. Commit queue order, operation admission/drain,
log append serialization, and the legacy libuv mode are unchanged.

The first experiment retained a registry lookup for every registration. It improved
eight-worker log throughput but added a second registration lock and regressed the
single-worker case. Caching at the existing DBHandle owner removes that extra hot-path
lock. Fixed hash buckets were considered and rejected after independent planning
review: collisions leave unrelated environments serialized, and a fixed set of maps
and mutexes adds memory to every database, including idle databases.

## Profiling evidence

Apple `sample` collected native stacks at a requested 1 ms interval. The sustained
baseline driver used four workers, eight outstanding transactions per worker, and
100-byte log entries, with WAL disabled and no database key writes. It completed
2,958,027 transactions in 10.0004 seconds; the log's transaction counter matched the
reported completion count. Its profiled throughput is not the comparison baseline.

| Nearest binding ancestor of a kernel mutex wait | Thread samples |
| ----------------------------------------------- | -------------: |
| `registerCommitCompletion`                      |          2,588 |
| `finishCommitCompletion`                        |          2,374 |
| `dispatchCommitCompletion`                      |            230 |
| Transaction registry add/get/remove             |          2,529 |
| `TransactionHandle::addLogEntry`                |             97 |
| Commit queue enqueue/run                        |            174 |
| Other                                           |             89 |
| Total                                           |          8,081 |

Commit-completion paths account for 64.25% of these mutex-wait samples. This is **not**
64.25% of elapsed time, CPU usage, or lock acquisitions. Threads are sampled separately;
condition-variable sleeps, event-loop waits, and `writev` are separate categories.
No kernel mutex wait was sampled inside the store's `writeBatch` on this workload.
Short uncontended locks can still have a cost below the sampling resolution.

A separate profile of existing worker-put, worker-log, realistic-load and tight-log
benchmarks showed the same completion/transaction-registry paths, plus startup
serialization in `DBRegistry::OpenDB`. The full stress profile was dominated by open/close
registry locking during its many-handle workload. Those setup/teardown waits must not
be mistaken for steady-state transaction-log append contention.

The candidate profile on the same four-worker/8-outstanding, 10-second workload
completed 2,917,655 transactions. Completion register/dispatch/finish accounted for
128 of 2,373 kernel mutex-wait thread samples (5.4%), with transaction add/get/remove
now accounting for 1,935. The baseline and candidate collected 7,620 and 7,428
main-thread samples respectively; raw totals are not normalized timing estimates.
This supports the intended removal of cross-env completion contention, while the
separate unprofiled trials below determine the throughput claim.

## Paired throughput comparison

Five independent 3-second trials per version/workload; baseline/candidate order alternates by repetition. Each trial counts completed transactions. Runs were unprofiled and did not overlap tests or builds. The baseline is the supplied Release binding from the base checkout; the candidate was rebuilt locally. Binary SHA-256 identities and every trial are retained in [the raw results](mutex-contention-2026-09-16.json), where candidate trials use the label `cached`.

The measured candidate source was `b7a23da2`. Through `f9f87aa5`, production changes
after that measurement were comments and the `completionStateMutex` member rename.
The subsequent admission-order fix is described below; the table is the original
measurement, not a fresh measurement of that fix.

| Workload         | Base median txns/s (range) | Candidate median txns/s (range) | Median change |
| ---------------- | -------------------------: | ------------------------------: | ------------: |
| log, 1 worker(s) |  285,814 (268,902–296,796) |       296,388 (287,185–299,151) |         +3.7% |
| log, 4 worker(s) |  339,454 (332,123–350,653) |       329,680 (320,238–346,135) |         -2.9% |
| log, 8 worker(s) |  185,836 (178,970–188,273) |       258,480 (255,425–284,300) |        +39.1% |
| put, 4 worker(s) |  475,821 (463,921–485,965) |       500,384 (478,962–507,948) |         +5.2% |

The eight-worker log result improves substantially across all five trials. The single-worker and four-worker put results are smaller gains. **Four-worker log throughput is 2.9% lower at the median**, with overlapping ranges; this is not a universal speedup. Treat the small differences as workload/host-sensitive and the eight-worker result as the strongest evidence. These ranges describe the observed trials, not confidence intervals. No p99 latency claim is made.

The cost is one cached shared pointer per database handle, one shared pointer per in-flight native commit, and a mutex-bearing object per active descriptor/environment pair. These references do not keep a Node environment alive; TSFN pending accounting retains that responsibility.

## Remaining synchronization decisions

| Area                                                      | Decision and reason                                                                                                                                                                                                           |
| --------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `TransactionLogStore::writeMutex`                         | Retain: append bytes, rotation, failure retirement and log-position publication are one ordered protocol. The default commit lane already serializes async appends; synchronous commits and legacy mode still need exclusion. |
| Store `dataSetsMutex`                                     | Retain: protects the file map, uncommitted-position collection and coherent current/frozen mapping transition. An atomic sequence number does not make those collections or their lifetimes safe.                             |
| Store `transactionBindMutex`                              | Retain: binding, pending accounting and the closing decision form a multi-field protocol. It was a small share of sampled contention; any combined atomic state needs its own lifecycle proof and measurements.               |
| Store `flushedStateMutex`                                 | Retain: serializes the stream and offset of `txn.state`, including I/O; a scalar atomic cannot replace stream ownership.                                                                                                      |
| Transaction/file registries and `txnsMutex`               | Remaining measurable candidate. Maps and shared object acquisition require synchronization. Consider reducing transaction-ID lookups on the log-entry path before attempting lock-free reclamation.                           |
| File `fileMutex` / `indexMutex`                           | Retain: own handles, mappings and index construction. Read/rotate/purge coherence is more than an atomic size field.                                                                                                          |
| VT writer mutex                                           | Retain: protects tracker acquisition and reclamation as well as slot transitions. An atomic slot pointer alone permits a reader to acquire a freed tracker.                                                                   |
| Event/lock callback mutexes                               | Retain: serialize TSFN dispatch with environment cleanup. This is the same lifetime invariant as commit completions.                                                                                                          |
| Commit queues, park timeouts, watchdogs                   | Already use condition variables with queue/deadline/stop predicates. Sleeping background threads are expected, not evidence for spinning.                                                                                     |
| Descriptor operation drain                                | Already uses C++20 atomic wait/notify.                                                                                                                                                                                        |
| Async-work drain                                          | Uses an atomic count plus timed condition-variable waits. Any polling removal must preserve bounded shutdown and close/admission behavior; the broader lifecycle work is tracked in #784.                                     |
| Settings, iterator close, backup stream and OS file locks | No measured reason here to change their ownership/exclusion contracts. Kernel backup locks also coordinate processes, which in-process atomics cannot do.                                                                     |

The practical action is to reduce the scope of a contended lock while keeping its
lifetime guarantee. Broad replacement of mutexes with atomics is not justified by
these profiles.

## Reproducing the throughput workload

After building the bundle and Release binding, run from the repository root:

```sh
node benchmark/mutex-contention.mts --workers=4 --seconds=3 --mode=log
node benchmark/mutex-contention.mts --workers=4 --seconds=3 --mode=put
```

`--concurrency` defaults to 8 per worker. `log` appends one 100-byte entry per transaction;
`put` updates a distinct key per worker/lane with a 100-byte value. Both use async
transactions with `disableWAL: true`. The driver awaits every counted transaction,
checks log totals or final values, and emits JSON with count, elapsed seconds,
throughput and process CPU time. Worker startup/open and final verification/teardown
are outside the timed interval; there is no separate JIT warmup. Each trial uses a new
database on the repository's volume. These workloads do not measure fsync durability
latency or combined database-write-plus-log transactions.

The existing Vitest worker benchmarks use worker messages for iterations, and their
`concurrent()` wrapper can return with operations outstanding. Their displayed Hz is
benchmark iterations per second, not directly comparable to the driver's completed
transactions per second. They remain useful representative smoke/comparison workloads;
use the driver for explicit completed-operation counts.

## Stability scope and follow-up

Passing stress tests and a sampled profile do not prove the absence of races or
deadlocks. The changed protocol still requires its lock-order and env-lifetime proof.
Linux/Windows/runtime coverage belongs to CI; no ThreadSanitizer result is claimed from
this macOS investigation.

Executed candidate validation: `pnpm test` passed 69 files / 1,002 tests (8 skipped);
`pnpm test:native` passed 221 tests (3 platform-specific skips); `pnpm test:stress`
passed all 5 files / 9 tests; `pnpm check` passed. The new reopen test ran across
all four transaction-option variants. The teardown fixture also verified that a live
environment's commit completes while another worker environment is terminated.

A separate native reproduction confirmed [#860](https://github.com/HarperFast/rocksdb-js/issues/860):
`tryClose()` releases its written-position lock before latching admission closed.
A transaction can bind, finish `writeBatch()` and reduce the pending count back to zero
between those checks. A temporary scheduling hook before phase 3 reproduced
`pending=0`, one real uncommitted position, and `tryClose() == true`. This is a
pre-existing close-protocol defect, not fixed by the completion-lock change. Its
production trigger is a concurrent destructive log purge; no customer incident or
JavaScript-level data loss was reproduced. Keep the binding lock until a separately
reviewed admission/drain fix resolves that handoff.

The investigation also found [#859](https://github.com/HarperFast/rocksdb-js/issues/859):
the DB-instance stress test awaits an empty promise array in its worker-close phase.
That test passed locally, but its close acknowledgement gap limits what the pass proves.
The logged-transaction stress test's title says 10k while its worker count is 1,000;
results should be interpreted using the executed count (30,000 total).

Outside review also identified [#861](https://github.com/HarperFast/rocksdb-js/issues/861):
the native teardown test's parent does not kill/reap a hung fixture when Vitest times
out. The successful run here does not exercise that failure path. The broader
transaction close/admission contract remains [#784](https://github.com/HarperFast/rocksdb-js/issues/784).
Follow-up review found that this PR's cache dereference added a crash point within that
window. A deterministic child/worker regression pauses the commit after capturing its
descriptor, completes foreign `shutdown()`, then resumes admission. The old PR head
`f9f87aa5`, with only the test seam added, crashed with SIGSEGV in all four combinations
of cold/warm cache and single/two-lane mode. Admission now publishes the descriptor
operation count and checks closing before accessing the cache. A winning shutdown
therefore rejects before dereferencing the reset handle; a winning commit makes shutdown
wait. Setup cleanup also retains the descriptor pin until its operation count is released.
This fixes the new cache access, while earlier transaction-entry reads and the broader
#784 admission contract remain outside this patch's guarantee.

The separate read-heavy work in [#545](https://github.com/HarperFast/rocksdb-js/pull/545)
and its baseline [#546](https://github.com/HarperFast/rocksdb-js/pull/546) was not evaluated
by this change. Those subscriber/short-range workloads are a distinct follow-up to the
commit-side profile here.
