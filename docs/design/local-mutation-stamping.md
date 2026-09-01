# Design: commit-time local mutation stamping (dual-clock stage 1)

Status: revision 2 for planning review (issue #811). Stage 1 of the three-clock model per the
reviewed dual-timestamp plan behind HarperFast/harper#2409 (rulings D1–D4); companion adoption
issue HarperFast/harper#2412 (stages 0b/2), apply-side HarperFast/harper-pro#790, replay
fidelity HarperFast/harper#2411.

Revision 2 adopts the round-1 review's corrections: a durable receiver-clock floor with reserved
ceilings (origin-keyed log timestamps cannot seed it — §3.7); caller-supplied time never
advances the receiver clock beyond a bounded skew (§3.2); finite-domain validation and
state-gating on `setTimestamp` (§3.2); the floor file doubling as a one-way persistent enable
marker (§3.1); rejection of short values instead of silent skip, and restore-on-failure for the
caller buffer (§3.3); forced v1 rotation + sticky v2 + per-entry payload CRC in v2 (§3.4);
env-only crash seams (§5); lock-free static assertion, contended p95/p99, and disabled-mode
budgets (§6). It also narrows the ordering contract to what consumers actually use
(uniqueness + claim monotonicity, not durable-write order) with the rationale in §3.2, and
prices the reviewer's additional framing options in §4.

## 1. Problem and goal

Harper on RocksDB collapses three identities into the first 8-byte word of every record value:
the record **version** (origin-defined, legitimately non-unique per key), the **local write
identity** (what VT/cache freshness and subscription staleness need — unique per write), and the
**resume cursor** (the transaction-log key, origin clock domain). harper#2065 made version ≠
commit time legitimate, and every consumer that assumed equality is now subtly wrong (see the
plan's consumer table). The end state is three clocks with one owner each:

| Clock | Meaning | Owner / uniqueness |
|---|---|---|
| Record version | Conflict resolution, CRDT ordering, replica convergence | Origin-defined; legitimately NON-unique per key |
| Local mutation stamp (`localTime`) | "This exact write on this node": VT/cache freshness, write identity, subscription staleness | Receiver-locally assigned at commit, unique per write — no replicated-write opt-out |
| Resume cursor | Per-origin log position for replication resume | Per-node transaction-log key stays in the origin's clock domain, unchanged |

Stage 1 (this repo): rocksdb-js assigns a monotonically increasing local stamp at commit into
the first word of each record encoding and into each transaction-log batch, with a
keep-if-greater shortcut so normal writes pay nothing new. The capability ships **dormant**
behind an explicit per-column-family enable: harper's current decoder maps the record first word
straight to `version` (`RecordEncoder.ts:455-466` — `version: localTime`, same number) and would
silently discard source versions if stamping activated before harper's distinct-version decoder
(harper#2412 stage 2). Stages 1 and 2 are not independently deployable; the enable is a single
atomic gate on the reader/writer pair. **No activation default is added anywhere in this
change.**

## 2. Current mechanics this builds on (code-traced)

- `TransactionHandle::startTimestamp` is set at construction from the process-global strictly
  increasing `getMonotonicTimestamp()` (`transaction_handle.cpp:93`, `core/platform.cpp:136-155`)
  and is JS-overridable via `setTimestamp()` (`transaction.cpp:1285-1310`, which today accepts
  any number > 0 — NaN/Infinity pass, and it is callable in any state). Harper sets it to the
  **origin's** version when applying replicated writes (`DatabaseTransaction.ts:405,621`,
  fed from `replicationConnection.ts:4028`).
- The log batch is created with `startTimestamp` at the first `addLogEntry`
  (`transaction_handle.cpp:188` — the batch key snapshots the txn timestamp *at that moment*);
  each entry's 13-byte header gets that timestamp stamped **at write time**
  (`transaction_log_file.cpp:595`, the `transaction_log_entry.h:46` "skip timestamp for now"
  comment). The batch timestamp is therefore already "the txn's clock at commit" — for local
  writes a near-commit-time local monotonic value; for replicated applies the origin's clock,
  which is what per-node resume cursors depend on (`replicationConnection.ts:4085-4088`:
  "version === audit-log key" is the documented cursor invariant).
- Entry timestamps are only *mostly* monotonic per file: `findPositionByTimestamp` builds a
  running-maxima index and documents that it gives no upper bound on an out-of-order log
  (`transaction_log_file.cpp:668-680, 748-766`).
- The commit paths (all four: sync `transaction.cpp:884-974`, legacy libuv `:836-878`,
  single-lane and two-lane commit-worker `:815-830`) share one ordered sequence:
  `executeLogWork` (log `writeBatch` → `committedPosition`) → `executeCommitWork`
  (`txn->Commit()` → `releaseIntent()` → `commitFinished` gated on `status.ok()`).
  On IsBusy/TryAgain the WAL batch is write-once (#668): `committedPosition` survives
  `resetTransaction()` and `addLogEntry` refuses a re-stage (`transaction_handle.cpp:147-162`),
  so a retry never rewrites the log.
- The VT keys freshness on the first 8 bytes of the value
  (`VerificationTable::extractVersionFromValue`, `core/verification_table.cpp:183-189`) and
  refuses FRESH/publication for values carrying `VERSION_NOT_UNIQUE_FLAG` (#766,
  `verification_table.h:53-68`).
- **No per-descriptor committed-stamp watermark exists today** (grep over `db_descriptor.*`);
  the only timestamp watermark is the per-log-store `latestTimestamp`
  (`transaction_log_store.cpp:855-858`) — which tracks **batch keys**, i.e. the origin clock for
  replicated applies, so it cannot recover a receiver-local maximum (§3.7).
- Non-transactional writes: `Database::PutSync`/`RemoveSync` (`database.cpp:2077-2216`) write
  directly with `db->Put`/`Delete` under a VT write-intent lock, on the caller's JS thread —
  *not* through the commit lanes.
- RocksDB copies key/value bytes into the WriteBatch rep at `txn->Put` time; the batch is
  walkable via the public `WriteBatch::Iterate(Handler*)` with
  `PutCF(uint32_t cf_id, const Slice& key, const Slice& value)` handing slices that point into
  the batch's own rep (`deps/rocksdb/include/rocksdb/write_batch.h:236-247,377`;
  `Transaction::GetWriteBatch()` → `WriteBatchWithIndex::GetWriteBatch()`). rocksdb-js never
  enables `protection_bytes_per_key` (grep: no `protection` in `src/binding` outside file-lock
  prose), so no per-entry integrity metadata exists to invalidate.

## 3. Design

### 3.1 Enable gate (dormant by default) and the persistent enable marker

New per-column-family open option `commitStamping: boolean` (TS `store.ts` → parsed into
`DBOptions` → stored on the CF's `ColumnFamilyDescriptor` on the process-global
`DBDescriptor`). Default **false** — with it false (or absent), every code path below is inert:
no record bytes change, no log format change, no VT change, and the only added default-path work
is the branch that checks the flag (no claim, no clock read, no batch walk, no allocation).

Because stamping changes durable bytes, agreement is enforced on two axes:

- **In-process**: all handles/envs on one CF must agree, following the compression option's
  cross-open discipline — a second open of an already-open CF with an **explicitly different**
  `commitStamping` value is rejected in `DBRegistry::OpenDB`; an unspecified value inherits the
  live one.
- **Across restarts**: enabling stamping durably writes the CF's name into the database's
  **stamp-floor file** (§3.7) before the first stamped commit. On open, a CF listed in the floor
  file that is opened without `commitStamping: true` **fails closed** with an explicit error —
  a one-way marker, so a restart that forgets the option cannot silently interleave unstamped
  first words into a stamped CF. (Un-enabling is a deliberate manual operation: delete the
  marker entry with the documented CLI/util, never implicit.) The harper-side durable
  data-format floor (harper#2412 stage 2) remains the reader/writer pairing authority; this
  marker is the storage layer's own fail-closed backstop, covering non-harper users too.

### 3.2 Stamp assignment: keep-if-greater against a bounded, durable receiver clock

New `DBDescriptor` state (all process-global per database):

- `std::atomic<uint64_t> localStampWatermark` — float64 bit pattern of the last claimed stamp
  (positive doubles compare correctly as uint64; `static_assert(std::atomic<uint64_t>::is_always_lock_free)`).
- `std::atomic<uint64_t> stampReserve` — the durably persisted ceiling (§3.7): no stamp above
  it may ever be handed out before a higher ceiling is durable.

One Node-free function (`core/local_stamp.{h,cpp}`, GoogleTest-covered, clock injectable for
tests):

```
claimLocalStamp(watermark, reserve, candidate, now) -> double
  # keep-if-greater, bounded by receiver time and the durable reserve
  loop:
    wm = watermark.load(acquire)
    if candidate > wm and candidate <= now + MAX_KEPT_SKEW_MS:
      if candidate > reserve: extendReserveBlocking(candidate)     # §3.7, off-path in practice
      if watermark.CAS(wm -> candidate): return candidate          # keep path
      continue                                                     # lost race, re-check
    candidate = max(now', nextafter(wm))   # re-stamp path; now' = getMonotonicTimestamp()
```

- **Keep path** (the overwhelmingly common case: `candidate` = the txn's `startTimestamp`, taken
  from the same monotonic clock at txn creation, and nothing committed past it): one clock read,
  two atomic loads, one CAS. **Zero allocations, no mutex, no syscall beyond the vDSO clock.**
  This is the "normal writes pay nothing" shortcut.
- **Re-stamp path** (contention; a caller-set timestamp at/below the watermark — every
  replicated apply whose origin time is behind local commits; or a candidate beyond the skew
  bound): a fresh receiver-time value claimed the same way.
- **Uniqueness is by construction**: the CAS claim admits any given double at most once per
  database, including when two origins supply the same timestamp or a caller supplies a
  duplicate — the loser re-stamps.
- **Caller-supplied time never owns the receiver clock.** `MAX_KEPT_SKEW_MS` (constant, 1 hour)
  bounds how far any kept candidate may sit above receiver time, so
  `watermark ≤ now + MAX_KEPT_SKEW_MS` always holds: a hostile or buggy
  `setTimestamp(Number.MAX_VALUE)` cannot push the clock toward float exhaustion (it simply
  re-stamps at receiver time; harper additionally caps source-reported versions at `now` since
  #2065, so real workloads keep). `setTimestamp` itself is hardened: reject non-finite values
  and values ≥ 8.64e15 (harper's MAX_DATE_TIMESTAMP domain), and reject calls when the
  transaction is not `Pending` — which also removes the JS-thread-vs-commit-lane data race on
  `startTimestamp`, since the commit entry point snapshots the candidate on the JS thread
  before any lane work runs.

**Ordering contract — deliberately uniqueness, not durable-write order.** Stamps are claimed
before `txn->Commit()`, and sync commits/direct puts are not serialized with the async lanes,
so a stamp's numeric order can differ from RocksDB durable-write order across concurrent
writers. No surveyed consumer needs the latter: VT freshness and subscription staleness compare
for **equality** (write identity), replication resume and boot replay order by **log
key/position**, and the broadcaster holds a live position-based iterator. The contract is
therefore: (a) unique per write per database; (b) strictly above every stamp *claimed* earlier
(claim monotonicity — which implies per-key monotonicity for all non-retry commits, since a
key's later writer claims after the earlier writer's claim); (c) bounded above by receiver time
plus `MAX_KEPT_SKEW_MS`; (d) never reused across restart (§3.7). The single per-key exception:
a pinned retry (below) can commit a key at a stamp claimed before a conflicting winner's — an
equality-safe, position-consistent skew that per-key-order consumers must not rely on
(documented; harper's post-#2409 staleness checks are equality-based).

**Pinning across retries.** The stamp is finalized **once per durable WAL batch**: a new
`TransactionHandle::localStamp` field survives `resetTransaction()` exactly like
`committedPosition`, and finalization reuses it whenever
`committedPosition.logSequenceNumber > 0` (the #668 write-once condition). A retried commit
whose log batch is already durable re-applies the *same* stamp to its re-put records — the only
alternative would diverge record first words from the durable batch stamp (§5 F7). A retry with
no durable batch re-finalizes fresh.

**Finalization site**: the top of `executeLogWork` (which runs first on every async lane —
two-lane, single-lane, legacy — and is a deliberate pass-through even for txns with no log
batch) and the equivalent point in `CommitSync` before its inline log stage; the candidate is
the snapshot taken at the commit entry point on the JS thread. That places finalization before
the batch header write (which needs the stamp) and before `txn->Commit` (which needs the
records patched), on the thread that owns the txn at that moment.

Non-transactional `Database::PutSync` claims with `candidate = getMonotonicTimestamp()`
(always the keep path in practice) and stamps the value buffer before `db->Put`.

### 3.3 Record stamping: pre-stamp at put, patch-on-restamp at commit

On a stamped CF, rocksdb-js **owns the first 8 bytes of every value**. A put with
`value.length < 8` on a stamped CF is **rejected with a clear error** (it cannot carry the
word; silent skip would leave holes in the enabled-CF contract). Harper's records are always
≥ 12 bytes. Two-phase stamping:

1. **Pre-stamp at `putSync`**: write the candidate (`startTimestamp`) as a big-endian float64
   over bytes 0..8 of the caller's value buffer immediately before `txn->Put` copies it into
   the WriteBatch. Cost: an 8-byte write into a hot buffer RocksDB is about to memcpy anyway.
   The overwritten 8 bytes are saved on the stack and **restored if the Put fails**, so a
   failed write does not clobber the caller's buffer. (On success the mutation is part of the
   documented contract: on a stamped CF the first word is rocksdb-js's; harper stage 2 writes
   its version into the distinct-version word instead.) The value pre-stamped is recorded on
   the handle (`preStampValue`, plus a flag when puts spanned more than one candidate value —
   only possible via `setTimestamp` between puts, which state-gating still permits while
   `Pending`).
2. **Patch at commit, only when needed**: after finalization, if `localStamp != preStampValue`
   (re-stamp path, pinned-retry path, or mixed pre-stamp values), walk the write batch once via
   `WriteBatch::Iterate` and overwrite bytes 0..8 of each stamped-CF put's value **in place** —
   a fixed-width substitution into the batch's own rep through the slice `Iterate` hands the
   handler. O(batch), zero allocations, skipped entirely on the keep path.

   *Why in-place substitution is sound here, and its guardrails*: the slices point at `rep_`
   bytes of a non-const `WriteBatch` this transaction exclusively owns before `Commit`; a
   fixed-width overwrite of value bytes changes no lengths, offsets, types, or keys, so the
   WBWI index stays valid. The RocksDB version is pinned by this repo (11.8.1 prebuilds), and
   the enabled path asserts at open that no per-entry protection metadata is configured
   (`protection_bytes_per_key == 0` everywhere this binding constructs options) — the one known
   mechanism that would make value bytes integrity-carrying. A GoogleTest exercises the
   substitution against a real `WriteBatchWithIndex` (patch → `GetFromBatch` → commit →
   re-read). The supported-API alternative — re-`Put` each patched entry, letting WBWI
   last-write-wins shadow the original — was considered and rejected as the default: it doubles
   batch memory and WAL bytes for exactly the workload where the patch path is hot (replicated
   applies), a durable cost taken to avoid a well-fenced in-process write. If a future RocksDB
   bump breaks the assertions, the re-`Put` fallback is the escape hatch (build-time switch),
   not a silent degradation.

Deletes carry no value and need no stamp (their write identity lives in the log entry; a
delete-only transaction still claims a stamp for its batch, and `committedLocalTime` — §3.6 —
reports it). `Merge` is not exposed by this binding. The patch handler stamps only CFs with
stamping enabled (cf_id → descriptor lookup), so a txn spanning stamped and unstamped CFs stays
correct.

### 3.4 The transaction-log batch stamp — and the log-architecture decision

The batch header timestamp (the log key) **does not change domain**: it remains the value
snapshotted from the txn at `addLogEntry` — the local monotonic clock for ordinary writes, the
origin's clock for replicated applies. Resume cursors depend on that (acceptance constraint).
What stage 1 adds is the batch's **local stamp**:

- **Keep path with stamp == batch key** (the ordinary local write): the existing entry header
  already carries exactly the stamp. Nothing new is written.
- **Stamp ≠ batch key** (every behind-the-watermark replicated apply, contended local commits,
  pinned retries, `setTimestamp`-after-`addLogEntry`): the stamp must travel with the batch.
  **How is the open architecture decision** (issue requirement: decided with measurements
  before any disk format is committed):

**Variant (i) — in-band only.** Entries whose batch stamp ≠ key carry the stamp in-band:
new entry flag `TRANSACTION_LOG_ENTRY_LOCAL_STAMP_FLAG (0x02)`; the 8-byte BE-float64 stamp is
prepended to the payload extent, so the header's length field covers it and every length-driven
scanner (recovery, resync, validation) is untouched — only entry *decode* and the writer learn
the flag. Every entry of the batch carries it, so mid-batch seeks see it. Decode-side
validation fails closed: a flagged entry whose payload extent is shorter than the extension, or
whose in-band stamp is non-finite, is a corrupt frame, never delivered data.

Segments that may contain flagged entries are **file-format v2**, and v2 also adds a 4-byte
CRC32C of the payload extent to every entry (in the extension region, ahead of the optional
stamp): today's readers refuse any version ≠ 1 (`transaction_log_file.cpp:216-219`), so a
pre-stage-1 binary fails closed on a v2 file instead of misreading stamped payloads — a
per-file local downgrade gate complementing harper#2412's store-level floor — and the CRC
closes the documented Windows torn-payload hole (`transaction_log_file.h` invariant: a durable
header with a partially durable payload over pre-extended zero fill reads as complete without a
payload checksum), which stamped entries must not be exposed to. **Activation on an existing
store forces a rotation**: under the store write mutex, the first stamped `writeBatch` against
an active v1 segment rotates it before writing, and v2 is sticky for the store thereafter (a
store flag persisted via the segment headers themselves) — an old reader never encounters
flagged entries inside a v1 file. Local-cursor seeks scan forward from a position or
approximate-key checkpoint.

**Variant (ii) — receiver-local ordered journal.** A second, receiver-local log keyed by the
local stamp, with per-origin resume metadata per entry (origin log name + key or position), so
local-stamp seeks are independently indexable and per-origin cursors can be recovered from one
journal. Costs a second append path per commit and a full second file-lifecycle surface
(rotation, recovery, retirement markers, purge, backup snapshot, validation — each an
invariant-bearing subsystem in this repo).

**Measurements** (this machine, release build, sync commits, 128B payloads; harness in the
issue's dispatch log):

| Measure | Result | Bearing |
|---|---|---|
| Full log-append path, marginal per commit | **+0.93µs** on a 9.2µs no-log commit (+10%); +0.44µs per extra entry | Variant (ii)'s steady-state write tax (lower bound — a real journal adds its own rotation/fsync/recovery costs) |
| Sequential scan rate, `query()` drain | **10.1M entries/s** (1.4 GB/s, mmap) | Variant (i)'s seek cost is O(scan distance) at this rate: a 100k-entry scan ≈ 10ms |
| Seek by timestamp via the existing per-file binary index | 60µs cold / 6µs warm | The index machinery an in-band stamp could reuse later if a true local-stamp seek API is ever needed |

**Who actually needs local-stamp seeks** (from the harper/harper-pro consumer survey):

- The cross-thread broadcaster (`transactionBroadcast.ts`) holds a **live reusable iterator**
  (a position checkpoint by construction) and only re-seeks on restart/fallback — its real
  defect today (a back-dated origin entry gated out at `:202` because the cursor is a
  timestamp) is fixed by having the stamp *available on each entry*, not by a seekable stamp
  index.
- Replication resume is **origin-keyed by requirement** (the invariant this change must not
  break) — no local-stamp seek.
- The CRDT resequencing walk (`Table.ts:2436-2760`) needs **origin-version** seeks into
  per-node logs — unaffected, and the reason the origin key domain stays.
- Table `startTime` subscriptions and MQTT durable sessions resume by timestamp values in the
  mostly-monotonic key domain today and tolerate bounded out-of-order; in-band stamps keep that
  working and make the cursor domain well-defined.
- Boot replay resumes by **physical position** (`startFromLastFlushed`) — already variant
  (i)'s model.

A middle option also exists — variant (i) plus a **sparse per-segment sidecar index** of
in-band stamps — and is deliberately *not* part of stage 1: in-band stamps are mostly-monotonic
per file for the same reason entry keys are today, so the existing running-maxima index pattern
(or a sidecar) can be added later without a format change if a real local-stamp seek consumer
appears. That future-compatibility is itself a point for (i).

**Recommendation: variant (i)**, on the evidence that no consumer needs an indexed local-stamp
seek (every local-order consumer either holds a position or tolerates bounded scan), the scan
fallback costs ~0.1µs/entry, and variant (ii) buys that unneeded index for +10% on every logged
commit plus a doubled file-lifecycle invariant surface. **This ruling is the task owner's**
(issue requirement); implementation of §3.4's format work waits on it.

### 3.5 VT freshness: superseding #766 without breaking it

No native VT change. The VT already keys freshness on the first word
(`extractVersionFromValue`), which under stamping becomes the local stamp — unique per write by
construction — so version equality once again proves write identity and producers stop needing
`VERSION_NOT_UNIQUE_FLAG` for newly stamped writes. The #766 refusal logic **stays as is**: a
legacy (pre-enable) value still carrying the flag is still refused FRESH/publication — the flag
read is per value, which is exactly the right granularity for a lazily migrating store. New
tests pin both directions (stamped value verifies FRESH; flagged legacy value still refuses)
alongside the untouched existing `verification-table.test.ts` suite.

### 3.6 Read-side exposure: `getEntry` and the distinct-version word

New TS-level `getEntry(key, options?)` on `RocksDatabase`/`Store` returning
`{ value, localTime, version } | undefined`, parsing the value-header contract the VT already
defines: `localTime` = first word (BE float64 at 0); if the 4-byte metadata word at offset 8
carries `VERSION_HEADER_TAG` and the new **`HAS_DISTINCT_VERSION_FLAG = 0x20000`** producer flag
(exported from the binding's constants next to `VERSION_NOT_UNIQUE_FLAG = 0x10000`, so harper
stage 2 sets the same bit this reads; 0x20000 is unused in harper's record-metadata bitmap),
`version` = the 8-byte BE float64 at offset 12; otherwise `version = localTime`. Non-finite
words are surfaced as `undefined` rather than fed onward. Meaningful only for stores whose
producer writes the header contract (as with the VT flag read); documented as such. Also new:
`Transaction#committedLocalTime` (the finalized stamp, readable after commit — on the keep path
it equals `getTimestamp()`; defined for delete-only transactions too), so a producer can learn
the stamp without re-reading the record. `setTimestamp`/`getTimestamp` doc units are corrected
to milliseconds in passing (typings currently say seconds).

### 3.7 Durable receiver-clock floor (restart safety)

The receiver clock must never re-mint a stamp that any durable artifact already carries, even
across a wall-clock regression. Origin-keyed state cannot provide this: the log store's
`latestTimestamp` tracks **batch keys**, and a re-stamped batch's local stamp exists only
in-band (or in the journal under variant (ii)) — e.g. origin key 1, local stamp 1000, crash,
clock restarts at 500: seeding from keys emits duplicates. A purged segment can also have held
the maximum. So the floor is first-class and receiver-owned:

- A small **stamp-floor file** per database (`<dbPath>/LOCAL_STAMP_FLOOR`), written with the
  same durability discipline as the transaction log's append-boundary markers (temp + fsync +
  atomic rename for creation; fixed-extent overwrite + fsync for updates; token + complemented
  value so a torn write fails load closed). It carries: the **reserve ceiling** and the set of
  stamped CF names (the §3.1 enable marker).
- **Reserve invariant**: no stamp above the persisted ceiling is ever returned by
  `claimLocalStamp`. The ceiling is extended ahead of need (`RESERVE_WINDOW_MS`, e.g. 5
  minutes above `max(now, watermark)`) by an off-thread task triggered when the watermark
  crosses `ceiling − margin`; the commit path blocks on extension only if claims outrun the
  asynchronous extension (pathological — bounded and surfaced, never silent). On the ordinary
  keep path the check is one atomic load and compare.
- **On open**: watermark and monotonic floor seed from the persisted ceiling (plus, as a
  belt-and-suspenders max, discovered log-store `latestTimestamp`s). A missing floor file on a
  database with stamped CFs recorded elsewhere is impossible by construction (the marker and
  ceiling share the file, written before the first stamped commit); a database that never
  enabled stamping has no file and no new open-time work.

This removes rev-1's accepted no-log residual entirely: the floor holds with logs, without
logs, and across purges, because it never derives from log content.

## 4. Approaches considered (four axes, same root cause)

| Axis | Option | Assessment |
|---|---|---|
| Higher layer | **A. JS-layer stamping** — harper assigns the stamp and writes it into the first word itself at save time | Cannot satisfy uniqueness or keep-if-greater: the JS layer cannot see the committed watermark at commit time (commits finalize on the commit lanes, possibly after other envs' commits), cannot patch the already-copied WriteBatch on the re-stamp path, and every worker/env would need a shared claim protocol rocksdb-js already owns natively. LMDB solves it inside the engine for the same reason (lmdb-js substitutes the placeholder at commit). Rejected. |
| This layer (chosen) | **B. Native commit-time stamping behind a dormant per-CF gate** (this design, with rev-2's durable floor, bounded skew, and hardened inputs) | Owns the only place where "at commit, above every prior claim" is knowable and where the batch bytes are still patchable; zero default-path cost via pre-stamp + keep-if-greater; dormant until harper's paired decoder (stage 2). |
| Lower layer | **C. RocksDB user-defined timestamps** (comparator-suffixed UDT) | Changes the key encoding and comparator of existing CFs (on-disk incompatible, bulk migration), puts the timestamp beside the *key* rather than in the value word that replication payloads and harper's decoder read, and UDT solves point-in-time reads, not write-identity stamping. Also does nothing for the consumer-side conflation. Rejected. |
| Do less | **D. No format change: fix the broadcaster's gate with its existing physical iterator position, keep the #766 VT refusal, measure the remaining caching hole** | Repairs one consumer (the broadcaster restart gap) but leaves the caching hole permanent — every version-reusing write (the `Math.max` clamp on out-of-order merges, `_recordRelocate`, same-key-in-txn) stays unvouchable forever, which is a standing correctness-shaped tax on exactly the merge-heavy workloads the VT exists for; leaves MQTT/table cursors in an ill-defined clock domain; leaves LMDB/RocksDB semantics divergent. This is the status-quo-plus option the three-round harper plan already rejected in favor of staged adoption. Rejected here, though its broadcaster fix is compatible with and complementary to stage 1. |
| Deeper cause (VT-only) | **E. Native in-memory mutation generation for the VT** — publish a synthetic per-write generation instead of the value word; migrate local subscriptions to physical log positions | Priced: solves only the process-local VT slice (a generation can be returned alongside reads and verified without any format change), but provides **no durable write identity** — nothing for cache-vs-store comparison across restarts, tombstone-prune/blob-scan write identity (harper#2412 stage 2's cleanup migration), LMDB-parity `localTime` on records, or a well-defined cursor domain for MQTT/table resumes. Those need the stamp in the bytes. Rejected as the primary; its position-migration half overlaps D's broadcaster fix. |
| Log architecture (within B) | **(i) in-band stamps (+ CRC'd v2)** vs **(ii) receiver-local journal**, with a sparse sidecar index as (i)'s future extension | §3.4 — measured; recommendation (i); ruling owed by the task owner before format work. |

## 5. Native atomicity contract and fault-injection matrix

**Contract.** On a stamping-enabled CF, for every transaction whose RocksDB commit becomes
durable: every stamped record's first word, the batch's local stamp (in-band or key), and the
value later exposed by `committedLocalTime` are the **same** double, and that double was claimed
exactly once, below the durable reserve ceiling. No crash at any boundary can yield a committed
record whose first word differs from its durable batch's stamp, and no restart can re-mint a
stamp any durable artifact carries. (Cross-crash *replay* — harper re-applying durable log
batches whose RocksDB commit was lost — constructs new transactions and therefore new stamps;
replay fidelity in the version domain is harper#2411's per-write API, out of scope here and
unchanged by this design.)

Structural argument: the stamp is finalized before the batch header is written and before
`txn->Commit`, is immutable once any durable artifact exists (pinning via `committedPosition`),
both consumers (record patch, batch write) read the same field on the same thread, and the
reserve is durable before any stamp under it is issued. The fault matrix then *proves* it at
every boundary via a test-only crash seam: **`ROCKSDB_JS_CRASH_POINT`, read from the
environment at startup only** (spawned-child pattern; no runtime setter is exported — an
externally callable `_exit` switch in a production binding is an outage primitive). Each armed
point calls `_exit` so nothing "cleans up":

| # | Crash point (seam) | Durable at crash | Recovery assertion (child respawn + reopen) |
|---|---|---|---|
| F1 | after stamp finalize, before `writeBatch` | reserve only | no batch visible, no records; next claims exceed the pre-crash watermark (reserve seeding) |
| F2 | mid `writeBatchToFile` (partial append, after first writev chunk) | torn entry prefix | `recoverTail`/boundary-marker retirement behaves exactly as today with v2/flagged entries; the v2 payload CRC rejects the Windows durable-header/partial-payload case; no records committed; `validateTransactionLogStore` clean |
| F3 | after `writeBatch`, before `txn->Commit` | full flagged batch | batch readable with correct in-band stamp and CRC; records absent; a replay-style re-apply after reopen produces records whose *new* stamps exceed the recovered reserve (never equal to any durable stamp) |
| F4 | after `txn->Commit`, before `commitFinished` | batch + records | record first words == batch stamp; recovered floor ≥ that stamp |
| F5 | after `commitFinished`, before completion callback | batch + records + watermark bookkeeping | same as F4 (bookkeeping is in-memory) |
| F6 | ENOSPC / short append (existing injection machinery) | partial batch, segment retired | retirement/rotation invariants unchanged with stamped entries; commit never ran; no stamp divergence |
| F7 | IsBusy after durable batch, crash before retry commits | batch at stamp S | reopened store exposes the batch at S; the no-crash variant of this test proves a same-process retry commits records at exactly S (pinning), never a fresh stamp |
| F8 | crash between reserve extension and dependent claims (forced-small `RESERVE_WINDOW_MS`) | old or new ceiling | either ceiling recovers to a value ≥ every issued stamp; clock regressed below ceiling on reopen still cannot duplicate |

F2/F6 also run on Windows (pre-extended zero-padded tail semantics, invariant 5): the length
field covering the in-band extension keeps every existing scanner correct, and the CRC is what
makes the F2 assertion provable there.

## 6. Performance budgets (numeric pass/fail, not comparative-only)

Benchmarked enabled-vs-disabled in the same process, interleaved, release build; asserted (not
just reported) in a stress-gated spec. "Zero default-path cost" means precisely: with no
stamped CF touched, no claim, no clock read, no batch walk, and no allocation are added — one
flag branch only — and budget B0 pins it.

| # | Scenario | Budget (fail threshold) |
|---|---|---|
| B0 | Feature compiled in, CF **not** enabled — uncontended single-put sync commit vs baseline build behavior | p50 ratio ≤ **1.02**; absolute p50 within noise of pre-change baseline recorded in the PR |
| B1 | Enabled, uncontended single-put commit, keep path | p50 ratio enabled/disabled ≤ **1.05**; p99 ratio ≤ **1.10** |
| B2 | Enabled, forced re-stamp every commit (`setTimestamp(1)`), single put | p50 ratio ≤ **1.25** |
| B3 | Enabled, 10k-put batch, keep path | ratio ≤ **1.05** |
| B4 | Enabled, 10k-put batch, forced re-stamp (full Iterate patch walk) | added cost ≤ **0.2µs per record** (≤ 2ms per 10k batch) |
| B5 | Keep-path claim allocation count | **0** heap allocations — GoogleTest with an operator-new counter around the Node-free `claimLocalStamp` + pre-stamp helpers |
| B6 | Contended: 4 worker envs committing concurrently (mixed keep/re-stamp), all three commit modes (legacy/single-lane/two-lane) + sync | p95 and p99 ratios ≤ **1.25**; uniqueness asserted across the full interleaving |
| B7 | Lock-freedom | `static_assert(std::atomic<uint64_t>::is_always_lock_free)` compiled on all platforms; claim function exercised from N threads in GTest for progress + uniqueness |
| B8 | Logged re-stamp storage overhead (variant (i)) | flagged-entry overhead = 12B/entry (8B stamp + 4B CRC amortized) — asserted in the format test; write-path throughput ratio for a fully flagged log ≤ **1.10** vs unflagged |

## 7. Dormancy / adoption contract

- `commitStamping` absent/false ⇒ zero behavior change; the full existing suite must pass
  unchanged on all platforms (including Windows) with the feature merely compiled in (plus B0).
- Enabled ⇒ record first words, the durable floor/marker file, and (per the §3.4 ruling) log
  format v2 segments. harper must not enable before its distinct-version decoder + durable
  data-format floor (harper#2412 stage 2); cluster-level activation additionally needs #2412's
  peer gate (new-format bytes and semantics must not reach peers below the format floor) —
  recorded there, not re-owned here. rocksdb-js enforces what it can locally: v2 segment
  headers fail old readers closed, the floor file fails a forgetful reopen closed, and the
  cross-open conflict check keeps one process from mixing stamped and unstamped writers on a
  CF.
- The per-node transaction-log **key domain is untouched** in both variants and on both paths
  (keep and re-stamp): resume cursors and `findPositionByTimestamp` semantics are unchanged.

## 8. Verification route

- **Unit (Vitest)**: stamp uniqueness/keep semantics across txn + non-txn writes; caller-buffer
  pre-stamp visibility and restore-on-failed-put; short-value rejection on stamped CFs;
  `setTimestamp` hardening (NaN/Infinity/≥8.64e15/huge-but-finite rejected; non-Pending
  rejected; between-puts value still patched); `setTimestamp`-after-`addLogEntry` (batch key
  keeps its snapshot, stamp diverges → flagged entry); multi-CF txn with mixed
  stamped/unstamped CFs; delete-only txns; `getEntry` word parsing incl.
  `HAS_DISTINCT_VERSION_FLAG` and non-finite words; `committedLocalTime`; VT: stamped value
  FRESH round-trip, flagged legacy value still refused (#766 tests untouched and green); log:
  flagged-entry read/write round-trip, malformed flagged entries (short payload, non-finite
  stamp, bad CRC) fail closed, v1-active-segment activation forces rotation and v2 stickiness,
  v2 header refusal by a v1-only reader, purge/rotation with flagged entries; floor file:
  create/extend/torn-write-fails-closed/enable-marker-fail-closed; dormant-mode byte-identical
  write assertions.
- **GoogleTest (Node-free)**: `claimLocalStamp` keep/re-stamp/skew-cap/reserve semantics with
  an injected clock (regression, no-log, post-purge scenarios), uniqueness under threads,
  B5/B7; WriteBatch fixed-width substitution round-trip.
- **Fault injection**: §5 matrix as spawned-child crash tests (fixtures pattern), F2/F6 on
  Windows CI.
- **Benchmarks**: §6 budgets asserted; existing bench suite for regression context.
- **End-to-end route**: full `pnpm test` on Node/Bun/Deno + Windows CI (ThinLTO/gyp caveats per
  repo history) proves dormant compatibility. Activation end-to-end (harper encoder preserving
  origin versions, apply-side two-word behavior, broadcaster resume on stamps, mixed-version
  cluster) is **harper#2412's rollout gate on the paired adoption branch — recorded there as a
  cross-repo gate, deliberately not claimed as coverage by this repository**.
