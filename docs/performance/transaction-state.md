# Transaction state CAS cost (#1013)

The state cell prevents a failed commit's completion from reviving a handle that a
foreign `close()` has aborted. Its lifecycle transitions add atomic read-modify-write
work on transaction paths, including the read-only transaction abort path.

## Work per lifecycle

Counts below are for the normal uncontended paths, excluding existing atomics in
admission, async-work tracking, and teardown. A CAS uses acquire-release ordering on
success and relaxed ordering on failure.

| Path                                               | State-cell work replacing plain state accesses                                                                                                 |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| Successful `commit()` or `commitSync()`            | Two strong CAS operations: admission and successful completion. The subsequent `close()` loads `Committed` and performs no state CAS.          |
| Failed attempt returned to the caller for retry    | Two strong CAS operations: admission and revert to `Pending`. Each subsequent attempt pays its own admission and completion/revert.            |
| Explicit abort of a pending transaction            | One successful weak CAS to `Aborted`; the subsequent `close()` loads `Aborted` without another state CAS.                                      |
| Closing a pending or committing transaction        | One successful weak CAS in the uncontended case. A racing revert or spurious failure can repeat the loop; observing a terminal state stops it. |
| Per-operation state gates (`get`, `put`, `remove`) | Relaxed atomic loads, with no CAS. These remain advisory and do not protect `txn` from concurrent teardown.                                    |

Setup-error cleanup also attempts `Committing` → `Pending`; a concurrent abort makes
that CAS fail without reviving the handle. No mutex, allocation, or process-global
counter was added by the state cell. It is per transaction, but async execution can
still transfer its cache line between the owning JS thread and a commit worker.

## Local measurement

[The standalone benchmark](../../benchmark/transaction-state.cc) compares the actual
`TransactionStateCell` with a single-threaded plain-field reference: unconditional
admission/success stores, a checked revert, and checked abort stores. Both use the
same byte-sized enum to isolate atomic access cost (the old production enum was
int-sized). Compiler barriers keep accesses between phases from being eliminated;
they emit no hardware fences. This measures only the state operations, initialization,
and loop overhead, not RocksDB, N-API, rollback, log I/O, or scheduling.

Run from the repository root with Clang or GCC:

```sh
clang++ -std=c++20 -O3 -DNDEBUG -Isrc/binding benchmark/transaction-state.cc -o /tmp/transaction-state-bench
/tmp/transaction-state-bench
```

Measured against `b05ca26f05f6fee7255e427b850ccb0b57e97336` on 2026-11-09,
macOS 27.0.1 arm64, Apple Clang 21.0.0. Each variant gets a 100,000-iteration warmup,
then nine paired trials of 5,000,000 lifecycles, alternating variant order. No builds
or tests ran concurrently with the measurement. The shared host was not isolated
or CPU-pinned; nearby load averages were 1.52 / 1.62 / 1.70, so small timings are
host-sensitive. Generated assembly retained `casalb` for the atomic transitions
and ordinary byte loads/stores for the reference.

[Raw trials](transaction-state-2026-11-09.csv), nanoseconds per lifecycle:

| Lifecycle                                           | Plain median (range) | Atomic median (range) | Difference of medians |
| --------------------------------------------------- | -------------------: | --------------------: | --------------------: |
| Successful commit and close                         |  0.368 (0.353–0.490) |   1.854 (1.697–2.389) |             +1.486 ns |
| One failed attempt, then successful retry and close |  0.603 (0.602–0.609) |   3.140 (3.137–3.147) |             +2.537 ns |
| Explicit abort and close                            |  0.477 (0.260–0.510) |   1.066 (1.058–1.120) |             +0.589 ns |

**Performance:** Successful commits add two state CAS operations in place of plain
stores; failed attempts also pay an admission and a revert CAS, and explicit aborts
add one successful CAS. The local uncontended state-only comparison measured
+1.486 ns per successful commit/close and +0.589 ns per explicit abort/close at the
median, buying an indivisible transition that cannot overwrite a concurrent abort.
The per-record gates use relaxed loads, not RMW operations. These measurements do
not establish an end-to-end throughput regression or bound cross-core cache-line
transfer, contention, or costs on x86/other hosts. In particular, the single-threaded
benchmark does not measure the JS-thread-to-commit-worker handoff.
