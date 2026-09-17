# Process-wide steady clock

## Intent and invariant

HarperFast/harper#2658 needs to compare coverage boundaries between workers and measure elapsed
lag and deadlines independently of wall-clock steps. Every sample must share one steady temporal
domain within the process, including workers created later. Suspend behavior is platform-defined;
this API does not promise a portable suspend-inclusive deadline.

Bun 1.3.14/1.4.0 worker-local runtime clocks do not provide this common domain (the motivating
probe read about 2.1 seconds in the parent and 12 milliseconds in a new worker). The existing
`getMonotonicTimestamp()` reads `system_clock` and ratchets ties/rollback with `nextafter`
(`src/binding/core/platform.cpp`). It orders transaction timestamps but can stall after rollback.
That epoch-based contract and all transaction/log paths must remain unchanged.

## Chosen

Expose `steadyClockNow(): number` at the module root, requiring no database handle. The native
helper reads `std::chrono::steady_clock::now().time_since_epoch()` and converts its duration to
`std::chrono::duration<double, std::milli>`. There is no per-worker initialization, shared mutable
state, addon lock, wall-clock read, database/log I/O or native allocation. The N-API callback checks
`napi_create_double` with `NAPI_STATUS_THROWS`; engines may allocate a boxed JS number.

Values are fractional milliseconds from an unspecified origin fixed throughout the process.
Comparisons are meaningful across all workers of that process, but not across processes, restarts
or hosts, nor against epoch/transaction timestamps. Samples are non-decreasing, not unique.
Equality cannot establish strict ordering: treat equal boundaries as "not later", or use a separate
sequence number. No atomic ratchet is applied to this clock.

### Representation, range and precision

`number` matches this package's millisecond duration APIs and avoids bigint arithmetic for the
consumer's 3000 ms lag bound. Bigint nanoseconds would retain every represented native tick, but
this API permits equality and does not require nanosecond-exact identities. Neither representation
is promised allocation-free at the JS boundary.

The selected standard libraries use signed 64-bit nanosecond durations (about 292 years in either
direction from their origin). Conversion to double does not narrow that range. Positive conversion
and scaling preserve non-decreasing order, though adjacent ticks can collapse to equality.
The output spacing is about 1.9 ns at 100 days from the origin, 61 ns at 10 years and 0.49 µs at
100 years. Integer-to-double rounding also contributes conversion error: at 100 years its half-ulp
is 256 ns, followed by up to 244 ns of millisecond rounding, less than 0.51 µs total per sample.
This is representation precision, not an accuracy or resolution guarantee. Precision depends on
distance from the native origin, not time since JS startup.

### Supported platforms

Node, Bun and Deno all call the same compiled native function on Linux, macOS and Windows.
The standard library owns the clock source; runtime VM startup does not reset it.

- Linux libstdc++ uses `CLOCK_MONOTONIC`, which excludes suspend and can be frequency-slewed by NTP.
  [GCC implementation](https://github.com/gcc-mirror/gcc/blob/master/libstdc%2B%2B-v3/src/c%2B%2B11/chrono.cc)
- Apple libc++ uses `CLOCK_MONOTONIC_RAW`, which includes suspend on macOS. Do not infer Apple's
  semantics from the identically named Linux clock.
  [libc++ implementation](https://github.com/llvm/llvm-project/blob/main/libcxx/src/chrono.cpp)
- Windows MSVC uses `QueryPerformanceCounter` converted to nanoseconds; the counter frequency
  determines actual resolution (a common 10 MHz counter ticks every 100 ns, but the C++ duration's
  period is 1 ns). QPC includes standby/hibernate and is independent of UTC adjustments.
  [MSVC implementation](https://github.com/microsoft/STL/blob/main/stl/inc/__msvc_chrono.hpp),
  [QPC contract](https://learn.microsoft.com/en-us/windows/win32/sysinfo/acquiring-high-resolution-time-stamps)

These sources were inspected on 2026-09-17. None supplies a portable nanosecond accuracy guarantee.
Wall-clock steps do not affect these sources; frequency adjustment and suspend semantics are
separate properties. No promise is made that runtime timers use the same suspend policy.

## Approaches considered

- **Different layer:** repair Bun or calibrate clocks in Harper. A Bun fix is unavailable to
  already-deployed runtimes; wall-time calibration reintroduces step sensitivity and an independent
  cross-worker calibration protocol adds synchronization error to every boundary.
- **Deeper cause:** make `getMonotonicTimestamp()` steady. Its values are durable epoch transaction
  identities and log keys; changing their domain breaks the persisted/replay contract. Another
  alternative is explicit suspend-inclusive OS clocks, but that changes the requested portable
  `steady_clock` contract and requires separate clock selection on each platform.
- **Do less:** reuse the wall-clock ratchet or enforce only strict coverage. The former stalls
  elapsed lag after rollback; the latter discards the approved bounded-lag behavior. Runtime-local
  timers alone cannot compare two workers' coverage samples on Bun.
- **Chosen:** additive native steady accessor. One native domain supplies the missing cross-worker
  elapsed measurement without altering durable identities. Fractional milliseconds preserve far
  more precision than the consumer's millisecond budget; equality is explicitly not strict order.

## Verification

The end-to-end route is the real N-API export through `src/index.ts`, parent/worker bracketing on
Node and Bun, staggered/restarted and concurrent workers, elapsed progression, and built ESM/CJS
exports. A synthetic worker-local clock control must be rejected by the same brackets without
assuming a particular runtime's future clock behavior.

GoogleTest covers conversion boundaries (including long durations, adjacent ticks and equal
rounded samples), native thread brackets and elapsed progression. Native and JS tests interleave
steady reads with the unchanged transaction timestamp ratchet. Existing transaction and log suites
cover allocation/adoption, replay and ordering.

An opt-in Linux/Node test uses libfaketime in an isolated child, with monotonic clocks left real.
It steps the child's wall clock backward and forward and observes both `Date.now()` and the native
transaction timestamp. Steady deltas must still measure the intervening sleep. This exercises
native clock selection, not a JS Date stub; it never changes the host clock. CI installs libfaketime
for the Linux Node 24 run so this regression does not silently remain local-only.

Run this repository's full `pnpm build`, `pnpm check`, `pnpm test:native`, `pnpm test` and
`pnpm test:bun` gates. Local Windows/macOS execution is unavailable; their existing CI matrices
exercise native and runtime worker tests. Record actual results and limitations in the PR.

## Planning review and rollout

The initial design at `75caf59d` received `Framing-Verdict: chosen-approach-sound` from the prior
session's planning review. Its allocation, precision, built-export and within-process testing
findings were adopted. Resume review uses the authorized no-Claude CLI policy; the final PR records
that verdict. Source inspection corrected the earlier draft's Windows period and macOS suspend
claims; the API's platform-defined suspend contract is unchanged.

Harper must consume matching JS declarations/bundles and native prebuilds from a release containing
this change, or build this branch from source. Existing 2.9.1 binaries do not provide the export.
No package is published here and Harper consumer changes remain in HarperFast/harper#2658.
