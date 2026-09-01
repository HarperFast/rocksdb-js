# Design: commit-time local mutation stamping (dual-clock stage 1)

Status: design for review (issue #811). Stage 1 of the three-clock model per the reviewed
dual-timestamp plan behind HarperFast/harper#2409 (rulings D1–D4); companion adoption issue
HarperFast/harper#2412 (stages 0b/2), apply-side HarperFast/harper-pro#790, replay fidelity
HarperFast/harper#2411.

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
  and is JS-overridable via `setTimestamp()` (`transaction.cpp:1285-1310`). Harper sets it to the
  **origin's** version when applying replicated writes (`DatabaseTransaction.ts:405,621`,
  fed from `replicationConnection.ts:4028`).
- The log batch is created with `startTimestamp` at the first `addLogEntry`
  (`transaction_handle.cpp:188`); each entry's 13-byte header gets that timestamp stamped **at
  write time** (`transaction_log_file.cpp:595`, the `transaction_log_entry.h:46` "skip timestamp
  for now" comment). The batch timestamp is therefore already "the txn's clock at commit" — for
  local writes it is a near-commit-time local monotonic value; for replicated applies it is the
  origin's clock, which is what per-node resume cursors depend on
  (`replicationConnection.ts:4085-4088`: "version === audit-log key" is the documented cursor
  invariant).
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
  (`transaction_log_store.cpp:855-858`).
- Non-transactional writes: `Database::PutSync`/`RemoveSync` (`database.cpp:2077-2216`) write
  directly with `db->Put`/`Delete` under a VT write-intent lock.
- RocksDB copies key/value bytes into the WriteBatch rep at `txn->Put` time; the batch is
  walkable via the public `WriteBatch::Iterate(Handler*)` with
  `PutCF(uint32_t cf_id, const Slice& key, const Slice& value)` handing slices that point into
  the batch's own rep (`deps/rocksdb/include/rocksdb/write_batch.h:236-247,377`;
  `Transaction::GetWriteBatch()` → `WriteBatchWithIndex::GetWriteBatch()`). rocksdb-js does not
  enable `protection_bytes_per_key`, so patching value bytes in place does not invalidate batch
  integrity metadata (verify at implementation: assert the option stays 0 on stamped CFs).

## 3. Design

### 3.1 Enable gate (dormant by default)

New per-column-family open option `commitStamping: boolean` (TS `store.ts` → parsed into
`DBOptions` → stored on the CF's `ColumnFamilyDescriptor` on the process-global
`DBDescriptor`). Default **false** — with it false (or absent), every code path below is
byte-for-byte inert: no record bytes change, no log format change, no VT change, no perf change.

Because stamping changes durable bytes, all handles/envs on one CF must agree, following the
compression option's cross-open discipline: a second in-process open of an already-open CF with
an **explicitly different** `commitStamping` value is rejected in `DBRegistry::OpenDB`; an
unspecified value inherits the live one. (Unlike compression there is no persisted-OPTIONS
dimension: the option is process-lifetime, chosen by the caller each open; harper#2412's durable
data-format floor is the cross-restart gate, deliberately owned at the harper layer where the
decoder lives.)

### 3.2 Stamp assignment: keep-if-greater against a descriptor watermark

New `DBDescriptor` field `std::atomic<uint64_t> localStampWatermark` (float64 bit pattern;
positive doubles compare correctly as uint64, same trick the VT uses). One new Node-free
function (`core/local_stamp.{h,cpp}`, GoogleTest-covered):

```
claimLocalStamp(watermark, candidate) -> double
  loop:
    wm = watermark.load(acquire)
    if candidate > wm:
      if watermark.CAS(wm -> candidate): return candidate   // keep path
      continue                                              // lost race, re-check
    candidate = max(getMonotonicTimestamp(), nextafter(wm)) // re-stamp path
```

- **Keep path** (the overwhelmingly common case: `startTimestamp` was taken from the same
  monotonic clock at txn creation and nothing committed past it): one atomic load + one CAS.
  **Zero allocations, no lock, no syscall.** This is the "normal writes pay nothing" shortcut.
- **Re-stamp path** (contention, or a caller-set timestamp at/below the watermark — every
  replicated apply whose origin time is behind local commits): a fresh monotonic value claimed
  the same way. The CAS claim is what makes the stamp **unique per write by construction**,
  including when two origins supply the same timestamp or a caller supplies a duplicate: at most
  one commit can claim any given value; the loser re-stamps.
- A kept caller-supplied *future* timestamp (source-reported `lastModified` ahead of local
  clock) becomes the local stamp and advances the watermark — same forward-jump semantics
  `getMonotonicTimestamp()` already has, and uniqueness still holds via the claim.

The stamp is finalized **once per durable WAL batch**: a new `TransactionHandle::localStamp`
field survives `resetTransaction()` exactly like `committedPosition`, and finalization reuses it
whenever `committedPosition.logSequenceNumber > 0` (the #668 write-once condition). A retried
commit whose log batch is already durable therefore re-applies the *same* stamp to its re-put
records — the only alternative would diverge record first words from the durable batch stamp
(see §5 F7). A retry with no durable batch re-finalizes fresh.

Finalization site: the top of `executeLogWork` (which runs first on every async lane — two-lane,
single-lane, and legacy — and is a deliberate pass-through even for txns with no log batch) and
the equivalent point in `CommitSync` before its inline log stage. That places it before the
batch header write (which needs the stamp) and before `txn->Commit` (which needs the records
patched), on the thread that owns the txn at that moment.

Non-transactional `Database::PutSync` claims a stamp the same way with
`candidate = getMonotonicTimestamp()` (always the keep path in practice) and stamps the value
buffer before `db->Put`.

### 3.3 Record stamping: pre-stamp at put, patch-on-restamp at commit

On a stamped CF, rocksdb-js **owns the first 8 bytes of every value ≥ 8 bytes** (values shorter
than 8 bytes are not stamped — they cannot carry the word; the VT already treats them as
version 0). Two-phase:

1. **Pre-stamp at `putSync`**: write `startTimestamp` as a big-endian float64 over bytes 0..8 of
   the caller's value buffer immediately before `txn->Put` copies it into the WriteBatch. Cost:
   an 8-byte write into a hot buffer that RocksDB is about to memcpy anyway. (The caller's buffer
   is deliberately mutated — on a stamped CF the first word is rocksdb-js's, and harper stage 2
   writes its version into the distinct-version word instead.)
2. **Patch at commit, only when needed**: after finalization, if `localStamp !=` the value
   pre-stamped at put time (re-stamp path, pinned-retry path, or a `setTimestamp()` call between
   puts — detected by comparing against a `preStampValue` recorded per txn, with a flag when puts
   were pre-stamped under more than one value), walk the write batch once via
   `WriteBatch::Iterate` and overwrite bytes 0..8 of each put's value in place
   (`const_cast` on the slice into the batch's own rep; keys/offsets untouched so the WBWI index
   is unaffected). O(batch) with zero allocations; skipped entirely on the keep path.

Deletes carry no value and need no stamp (their write identity lives in the log entry).
`Merge` is not exposed by this binding. The patch handler stamps only CFs with stamping enabled
(cf_id → descriptor lookup), so a txn spanning stamped and unstamped CFs stays correct.

### 3.4 The transaction-log batch stamp — and the log-architecture decision

The batch header timestamp (the log key) **does not change domain**: it remains
`startTimestamp` — the local monotonic clock for ordinary writes, the origin's clock for
replicated applies. Resume cursors depend on that (acceptance constraint). What stage 1 adds is
the batch's **local stamp**:

- **Keep path** (stamp == `startTimestamp`): the existing entry header already carries exactly
  the stamp. Nothing new is written; the format is unchanged.
- **Re-stamp path** (stamp ≠ key — every behind-the-watermark replicated apply, contended local
  commits, pinned retries): the stamp must travel with the batch. **How is the open
  architecture decision** (issue requirement: decided with measurements before any disk format
  is committed):

**Variant (i) — in-band only.** Entries whose batch stamp ≠ key carry the stamp in-band:
new entry flag `TRANSACTION_LOG_ENTRY_LOCAL_STAMP_FLAG (0x02)`; the 8-byte BE-float64 stamp is
prepended to the payload extent (so the header's length field covers it and every
length-driven scanner — recovery, validation, resync — is untouched; only entry *decode* and
the writer learn the flag). Every entry of the batch carries it, so mid-batch seeks see it.
Segments that may contain flagged entries are created as **file-format v2**: today's readers
refuse any version ≠ 1 (`transaction_log_file.cpp:216-219`), so a pre-stage-1 binary fails
closed on a v2 file instead of misreading stamped payloads — a per-file local downgrade gate at
the log level, complementing harper#2412's store-level floor. A stamping-enabled store writes
new segments as v2; existing v1 segments stay readable. Local-cursor seeks scan forward from a
position or approximate-key checkpoint.

**Variant (ii) — receiver-local ordered journal.** A second, receiver-local log keyed by the
local stamp, with per-origin resume metadata per entry (origin log name + key or position), so
local-stamp seeks are independently indexable and per-origin cursors can be recovered from one
journal. Costs a second append path per commit and a full second file-lifecycle surface
(rotation, recovery, retirement markers, purge, backup snapshot, validation — each of which is
an invariant-bearing subsystem in this repo).

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
  defect today (a back-dated origin entry gated out at `:202` because the cursor is a timestamp)
  is fixed by having the stamp *available on each entry*, not by a seekable stamp index.
- Replication resume is **origin-keyed by requirement** (the invariant this change must not
  break) — no local-stamp seek.
- The CRDT resequencing walk (`Table.ts:2436-2760`) needs **origin-version** seeks into
  per-node logs — unaffected, and the reason the origin key domain stays.
- Table `startTime` subscriptions and MQTT durable sessions resume by timestamp values in the
  mostly-monotonic key domain today and tolerate bounded out-of-order; in-band stamps keep that
  working and make the cursor domain well-defined.
- Boot replay resumes by **physical position** (`startFromLastFlushed`) — already variant (i)'s
  model.

**Recommendation: variant (i)**, on the evidence that no consumer needs an indexed local-stamp
seek (every local-order consumer either holds a position or tolerates bounded scan), the scan
fallback costs ~0.1µs/entry, and variant (ii) buys that unneeded index for +10% on every logged
commit plus a doubled file-lifecycle invariant surface. Variant (i) also leaves the door open:
in-band stamps are mostly-monotonic per file for the same reason entry keys are today, so the
existing running-maxima index pattern can index them later without a format change.
**This ruling is the task owner's** (issue requirement); implementation of §3.4's format work
waits on it.

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
`version` = the 8-byte BE float64 at offset 12; otherwise `version = localTime`. Meaningful only
for stores whose producer writes the header contract (as with the VT flag read); documented as
such. Also new: `Transaction#committedLocalTime` (the finalized stamp, readable after commit —
on the keep path it equals `getTimestamp()`), so a producer can learn the stamp without
re-reading the record. `setTimestamp`/`getTimestamp` doc units are corrected to milliseconds in
passing (typings currently say seconds).

### 3.7 Watermark recovery across restart

At open (and when a log store loads), the descriptor watermark is raised to every discovered
store's `latestTimestamp` (`fetch_max`), so after a restart under a regressed wall clock, new
stamps still land above every logged stamp. Residual (documented): a database that never writes
transaction logs has no durable stamp floor, so a wall-clock regression across restart could
re-mint a first-word value an existing record already carries — the same exposure harper's
version word has today (its versions come from the same clock), made strictly narrower by the
log-store seeding, and VT-safe within a process run regardless (the in-process claim is
monotonic; VT state does not survive restart).

## 4. Approaches considered (four axes, same root cause)

| Axis | Option | Assessment |
|---|---|---|
| Do less | **A. JS-layer stamping** — harper assigns the stamp and writes it into the first word itself at save time | Cannot satisfy uniqueness or the keep-if-greater contract: the JS layer cannot see the committed watermark at commit time (commits finalize on the commit lanes, possibly after other envs' commits), cannot patch the already-copied WriteBatch on the re-stamp path, and every worker/env would need a shared claim protocol rocksdb-js already owns natively. This is precisely how LMDB solves it (lmdb-js substitutes the placeholder at commit, inside the engine) — the engine owns commit time. Rejected. |
| Different layer | **B. Native commit-time stamping behind a dormant per-CF gate** (this design) | Owns the only place where "at commit, above every prior commit" is knowable and where the batch bytes are still patchable; zero default-path cost via pre-stamp + keep-if-greater; dormant until harper's paired decoder (stage 2). |
| Deeper cause | **C. RocksDB user-defined timestamps** (`comparator with timestamp`, native UDT) | Changes the key encoding and comparator of existing CFs (on-disk incompatible, bulk migration), puts the timestamp beside the *key* rather than in the value word that replication payloads and harper's decoder read, and UDT semantics (point-in-time reads) solve a different problem than write-identity stamping. Rejected. |
| Higher layer / accept | **D. Keep the single-word status quo + #766 refusal forever** | Leaves the VT unable to vouch for any version-reusing write (a permanent caching hole on exactly the merge-heavy workloads that need it), leaves subscription staleness and MQTT/table resume cursors in an ill-defined clock domain, and leaves LMDB/RocksDB semantics divergent. Rejected by the reviewed plan (three rounds). |
| Log architecture (within B) | **(i) in-band stamps** vs **(ii) receiver-local journal** | §3.4 — measured; recommendation (i); ruling owed by the task owner before format work. |

## 5. Native atomicity contract and fault-injection matrix

**Contract.** On a stamping-enabled CF, for every transaction whose RocksDB commit becomes
durable: every stamped record's first word, the batch's local stamp (in-band or key), and the
value later exposed by `committedLocalTime` are the **same** double, and that double was claimed
exactly once from the descriptor watermark. No crash at any boundary can yield a committed
record whose first word differs from its durable batch's stamp. (Cross-crash *replay* — harper
re-applying durable log batches whose RocksDB commit was lost — constructs new transactions and
therefore new stamps; replay fidelity in the version domain is harper#2411's per-write API, out
of scope here and unchanged by this design.)

Structural argument: the stamp is finalized before the batch header is written and before
`txn->Commit`, is immutable once any durable artifact exists (pinning via `committedPosition`),
and both consumers (record patch, batch write) read the same field on the same thread. The
fault matrix then *proves* it at every boundary, via a new test-only crash seam
(`setCrashPointForTesting(name)` binding export + `ROCKSDB_JS_CRASH_POINT` env for spawned
children, following `test_seam.h` conventions; each point calls `_exit` so nothing "cleans up"):

| # | Crash point (seam) | Durable at crash | Recovery assertion (child respawn + reopen) |
|---|---|---|---|
| F1 | after stamp finalize, before `writeBatch` | nothing | no batch visible, no records; watermark unaffected |
| F2 | mid `writeBatchToFile` (partial append, after first writev chunk) | torn entry prefix | `recoverTail` truncates / boundary-marker retires exactly as today with v2/flagged entries in the file; no records committed; `validateTransactionLogStore` clean |
| F3 | after `writeBatch`, before `txn->Commit` | full flagged batch | batch readable with correct in-band stamp; records absent; re-open + replay-style re-apply produces records whose *new* stamps exceed the recovered watermark (never equal to a stamp already durable elsewhere) |
| F4 | after `txn->Commit`, before `commitFinished` | batch + records | record first words == batch stamp; watermark seed ≥ that stamp |
| F5 | after `commitFinished`, before completion callback | batch + records + watermark bookkeeping | same as F4 (bookkeeping is in-memory) |
| F6 | ENOSPC / short append (existing injection machinery) | partial batch, segment retired | retirement/rotation invariants unchanged with stamped entries; commit never ran; no stamp divergence |
| F7 | IsBusy after durable batch, crash before retry commits | batch at stamp S | reopened store exposes the batch at S; a subsequent same-process retry (no crash variant of this test) commits records at exactly S (pinning), never a fresh stamp |

F2/F6 also run on Windows (pre-extended zero-padded tail semantics, invariant 5) — the length
field covering the in-band stamp is what keeps every existing scanner correct there.

## 6. Performance budgets (numeric pass/fail, not comparative-only)

Benchmarked enabled-vs-disabled in the same process, interleaved, sync commits, release build;
asserted (not just reported) in a stress-gated spec:

| # | Scenario | Budget (fail threshold) |
|---|---|---|
| B1 | Uncontended single-put commit, keep path | p50 ratio enabled/disabled ≤ **1.05** |
| B2 | Forced re-stamp every commit (`setTimestamp(1)`), single put | p50 ratio ≤ **1.25** |
| B3 | 10k-put batch, keep path | ratio ≤ **1.05** |
| B4 | 10k-put batch, forced re-stamp (full Iterate patch walk) | added cost ≤ **0.2µs per record** (≤ 2ms per 10k batch) |
| B5 | Keep-path claim allocation count | **0** heap allocations — GoogleTest with an operator-new counter around the Node-free `claimLocalStamp` + pre-stamp helpers |
| B6 | Lock inventory | no new mutex acquisition on the keep path — enforced structurally (the claim function touches one `std::atomic` and is GTest-exercised from N threads for progress/uniqueness) |

## 7. Dormancy / adoption contract

- `commitStamping` absent/false ⇒ zero behavior change; the full existing suite must pass
  unchanged on all platforms (including Windows) with the feature merely compiled in.
- Enabled ⇒ record first words and (per the §3.4 ruling) log format v2 segments; harper must not
  enable before its distinct-version decoder + durable data-format floor (harper#2412 stage 2).
  rocksdb-js enforces what it can locally: v2 segment headers fail old readers closed; the
  cross-open conflict check keeps one process from mixing stamped and unstamped writers on a CF.
- The per-node transaction-log **key domain is untouched** in both variants and on both paths
  (keep and re-stamp): resume cursors and `findPositionByTimestamp` semantics are unchanged.

## 8. Verification route

- **Unit (Vitest)**: stamp uniqueness/keep semantics across txn + non-txn writes; caller-buffer
  pre-stamp visibility; `setTimestamp`-between-puts patch; multi-CF txn with mixed
  stamped/unstamped CFs; `getEntry` word parsing incl. `HAS_DISTINCT_VERSION_FLAG`;
  `committedLocalTime`; VT: stamped value FRESH round-trip, flagged legacy value still refused
  (#766 tests untouched and green); log: flagged-entry read/write round-trip, v2 header
  refusal by a v1-only reader path, purge/rotation with flagged entries; dormant-mode
  byte-identical write assertions.
- **GoogleTest (Node-free)**: `claimLocalStamp` keep/re-stamp/uniqueness under threads, B5/B6.
- **Fault injection**: §5 matrix as spawned-child crash tests (fixtures pattern).
- **Benchmarks**: §6 budgets asserted; existing bench suite for regression context.
- **End-to-end route**: full `pnpm test` on Node/Bun/Deno + Windows CI (ThinLTO/gyp caveats per
  repo history); harper is not modified by this change — its adoption is harper#2412, and the
  dormant default is what keeps current harper (which knows nothing of the option) unaffected.
