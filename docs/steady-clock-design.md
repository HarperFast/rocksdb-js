# Process-wide steady clock

## Intent

Harper's native HNSW plane (HarperFast/harper#2658) needs one temporal domain shared by the main
thread and every `worker_threads` worker so that an index owner can publish "coverage boundary
captured at T" and a query worker can decide whether its view is more than N ms behind. Two
properties are needed at once: cross-worker comparability (a sample taken in worker A is ordered
against a sample taken later in worker B) and real elapsed time (a 3 000 ms lag bound or a
30 000 ms deadline measures 3 or 30 real seconds regardless of what the wall clock does).

Neither property is available from the JS runtimes uniformly. `process.hrtime.bigint()`,
`performance.now()`, `Bun.nanoseconds()` and `process.uptime()` all share an origin across
workers under Node but are per-worker under Bun (a fresh worker read about 12 ms while its
parent read about 2.1 s). `Date.now()` is shared but steps with the host clock. rocksdb-js's own
`getMonotonicTimestamp()` is shared and strictly increasing, but it is a wall-clock ratchet: after
a backward step it advances one `nextafter` per call until the wall catches up, so elapsed time
measured from it stalls and deadlines stretch.

## Invariant this change enforces

A steady-clock sample taken anywhere in the process — any thread, any Node/Bun/Deno env that has
loaded this `.node` — is comparable with every other sample in the process, and the difference
between two samples is real elapsed time, independent of wall-clock steps. The existing
`getMonotonicTimestamp()` contract (wall-clock epoch milliseconds, strictly increasing
process-wide, the source of transaction timestamps and log batch keys) is untouched: the two
clocks are separate values with separate contracts, and nothing routes one through the other.

## Chosen

Add a **module-level export** `steadyClockNow(): number` to the binding root (next to
`currentThreadId`, `tryFileLock` — no `RocksDatabase` instance or open handle required) that
returns `std::chrono::steady_clock::now()` converted to **fractional milliseconds** as a JS
`number`.

Native shape, all in the Node-free `core/platform.{h,cpp}` so GoogleTest covers it:

```cpp
int64_t steadyClockNanoseconds();                 // steady_clock::now() since its (unspecified) origin
double  steadyClockMilliseconds(int64_t nanoseconds); // pure conversion, tested for range/precision
double  getSteadyClockNow();                       // = steadyClockMilliseconds(steadyClockNanoseconds())
```

One N-API function `steadyClockNow` in `binding.cpp` calls `getSteadyClockNow()` and returns a
double. `load-binding.ts` declares and re-exports it; `index.ts` exports it publicly.

### Contract (what the README will state)

| Property | Value |
|---|---|
| Units / return type | milliseconds with a fractional part, `number` (IEEE double) |
| Origin | unspecified, fixed for the life of the OS boot session (Linux/macOS/Windows all read a since-boot counter). **Not** the Unix epoch; never compare with `Date.now()` or `getMonotonicTimestamp()` |
| Lifetime of comparability | every sample in one process is comparable, across all worker threads and all envs that load the binding, including workers started or restarted at any later time. Not durable, not comparable across process restarts or hosts. (On the listed platforms the counter is host-wide, so same-host processes happen to agree; this is not part of the contract.) |
| Monotonicity | non-decreasing across all threads: a sample taken after another (in real time) is `>=` it |
| Uniqueness | **not** unique. Two samples in the same clock tick, or two distinct nanosecond readings that round to the same double, are equal. Consumers that need "strictly later" must treat `==` as "not later" or pair the sample with a sequence number. No wall-time ratchet is substituted |
| Resolution | the platform clock's: 1 ns on Linux (`CLOCK_MONOTONIC`) and macOS (`CLOCK_MONOTONIC_RAW`), 100 ns typical on Windows (`QueryPerformanceCounter`) |
| Precision of the double | the value is milliseconds since boot, so its ulp grows with uptime: 15 ps at 1 day, 3.8 ns at 1 year, 61 ns at 10 years, 0.5 µs at 100 years. Sub-microsecond for any realistic uptime; nanosecond-exact for the first 104 days (2^53 ns). Rounding is monotone, so ordering of distinct readings is never inverted, only occasionally collapsed to equality |
| Range | `int64` nanoseconds covers 292 years of uptime before the native reading itself would overflow |
| Wall-clock independence | the value is a function of the steady clock only: `settimeofday`/NTP steps forward or backward do not move it. NTP frequency slew is platform-defined (`CLOCK_MONOTONIC` is slewed; `CLOCK_MONOTONIC_RAW` and QPC are not) |
| Suspend | platform-defined and **not** part of the contract: Linux `CLOCK_MONOTONIC` and macOS `CLOCK_MONOTONIC_RAW` do not advance while the host is suspended; Windows QPC behavior across sleep is not guaranteed by this API. Elapsed time across a host suspend is unspecified |
| Thread safety | a single clock read, no shared state, safe from any thread |
| Cost | one `clock_gettime`/QPC call plus a double conversion; no allocation, no lock, no wall-clock read, no database or log I/O, no per-worker calibration |
| Runtimes / platforms | Node, Bun, Deno on Linux, macOS, Windows (the CI matrix); the native reading is the standard library's `steady_clock`, so any platform the binding builds on is supported |
| Open handle required | no |

### Why milliseconds as `number` rather than bigint nanoseconds

The binding's entire public time surface is `number` milliseconds (`getMonotonicTimestamp()`,
`getOldestSnapshotTimestamp()`, `Date.now()`-shaped options), and the only bigint anywhere in the
addon is a generic key-decoding branch in `napi/helpers.cpp`. A bigint result would (a) allocate a
heap BigInt per call on the hot path, (b) force consumers into bigint arithmetic for a 3 000 ms
compare and a `BigInt64Array` to share a boundary through a `SharedArrayBuffer`, and (c) be the
first bigint in the API. The precision table above shows the double loses nothing a consumer can
use: the platform clocks resolve to 1 ns (Linux/macOS) or 100 ns (Windows), and the double keeps
sub-µs precision for a century of uptime. The rounding is proven monotone in a GoogleTest, so the
ordering guarantee is not weakened.

## Approaches considered

| Axis | Candidate | Fact that rejects it |
|---|---|---|
| **Different layer** — fix in the JS runtime or the consumer | Use `performance.timeOrigin + performance.now()` (or hrtime) and reconcile origins across workers in Harper | Bun samples `Instant::now()` and `SystemTime` separately per VM (bun-v1.4.0 `src/jsc/VirtualMachine.rs`, `Performance.cpp`), so `timeOrigin` is a wall-clock reading taken at worker start: a worker created after a wall step has an origin off by the step. The runtime does not expose a shared steady origin at all; only native code can read the OS clock directly |
| **Deeper cause** — make the existing clock steady | Change `getMonotonicTimestamp()` to derive from `steady_clock` (or add a steady floor to its ratchet) | Its value is persisted: it is the transaction timestamp, the transaction-log batch key, and replication/replay adopt it via `setTimestamp()` (docs/transaction-timestamp-integrity-design.md; rocksdb-js#825 seeds its floor from retained logs). It must stay Unix-epoch milliseconds comparable across processes and restarts. A since-boot steady value is neither; the two contracts are incompatible in one number, and the task settles that this API is not changed |
| **Do less** — existing mechanism / accept-and-detect | Keep `getMonotonicTimestamp()` and either document that lag is understated after a backward step, or replace the elapsed bound with strict-ordering-only checks; or build a calibration/heartbeat protocol over `Date.now()` | Understated lag after a step defeats the 3 000 ms bound the consumer exists to enforce (deadlines stretch until the wall catches up). Strict-only checks cannot express "no more than 3 s behind". A calibration protocol re-derives, in JS with message latency, what one `clock_gettime` already provides shared across threads. The requester explicitly rejected weakening the guarantee (task context) |
| **Chosen** | Additive module-level `steadyClockNow(): number` over `std::chrono::steady_clock` | One OS clock read gives both properties (shared domain, real elapsed) with no state, no I/O and no protocol. Additive, so every existing contract is preserved by construction |

Representation sub-choice, considered on the same facts: `bigint` nanoseconds (rejected above),
`number` nanoseconds (loses ns exactness at 104 days and is an unfamiliar unit next to every other
ms API here), `number` microseconds (no precision advantage over ms in a double — same 53-bit
mantissa — and a third unit). Milliseconds as `number` is consistent with the rest of the surface
and provably sufficient.

## Verification plan

- **GoogleTest** (`test/native/steady_clock_test.cc`, Node-free): conversion exactness and
  monotone rounding (including 1 µs steps strictly increasing at 10- and 100-year offsets),
  elapsed across a sleep matches a direct `steady_clock` bracket, samples from N threads fall inside
  the main thread's before/after brackets, a tight loop never decreases, and a regression that
  `getMonotonicTimestamp()` still returns strictly increasing epoch milliseconds when interleaved
  with steady samples.
- **Vitest** (`test/steady-clock.test.ts`, Node/Bun/Deno): parent samples bracket staggered and
  restarted workers' samples, with the stagger delay itself asserted (`worker >= parentBefore +
  stagger`) so a worker-relative origin fails; worker-internal elapsed progression across a sleep;
  parallel workers all inside the bracket; a control showing `performance.now()` from a worker
  fails the same bracket. Regression: `db.getMonotonicTimestamp()` and transaction timestamps stay
  in the epoch domain and strictly increasing with steady samples interleaved.
- **Wall-clock independence**: proven structurally (the accessor reads only `steady_clock`; the
  GoogleTest pins the accessor to a direct `steady_clock` reading) and, where `libfaketime` is
  available, by an opt-in child-process test (`ROCKSDB_JS_FAKETIME_LIB`) in which the child's
  `Date.now()` and `getMonotonicTimestamp()` are shifted by a day while its `steadyClockNow()`
  still falls inside the parent's bracket. The host clock is never modified. A `Date.now` stub is
  not used as evidence.
- Platform coverage: the CI matrix runs the native and JS suites on Linux, macOS and Windows for
  Node, Bun and Deno. Local validation is Linux/Node/Bun only; macOS and Windows results come from
  CI and are reported as such.
