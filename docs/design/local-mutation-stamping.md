# Design: commit-time local mutation stamping (dual-clock stage 1)

Status: revision 6 (implemented) — planning gate cleared twice (`Framing-Verdict:
chosen-approach-sound`, rounds 3 and 5; graded codex leg each round), with the task owner's
log-architecture ruling recorded and folded in between (§3.4). Issue #811. Stage 1 of the three-clock model per the
reviewed dual-timestamp plan behind HarperFast/harper#2409 (rulings D1–D4); companion adoption
issue HarperFast/harper#2412 (stages 0b/2), apply-side HarperFast/harper-pro#790, replay
fidelity HarperFast/harper#2411.

Revision history. Rev 2 adopted round 1's corrections: a durable receiver-clock floor
(origin-keyed log timestamps cannot seed it), bounded caller-time skew, `setTimestamp`
finite-domain + state validation, short-value rejection, caller-buffer restore, forced v1
rotation + sticky v2, env-only crash seams, lock-free assertion and contended budgets. Rev 3
adopts round 2's: the claim loop no longer has an unsatisfiable-bound spin after wall-clock
rollback (the skew bound gates only _caller-candidate keeps_, and the logical clock may run
ahead of a regressed wall clock — §3.2); the durable floor and enable marker move from a
sidecar file into a **hidden internal RocksDB metadata column family**, so backup, backup
stream, checkpoint, and restore inherit them with no new copy lifecycle (§3.7); origin-keyed
`latestTimestamp` seeding is dropped entirely (remotely influenceable — §3.7); batch
re-stamping uses **supported RocksDB APIs only** — no `const_cast` into the batch rep (§3.3);
all finalization failures convert to commit status instead of unwinding into `CommitWorker`
(§3.2); the caller's buffer is restored unconditionally after `Put` (§3.3); `commitStamping` is
three-state (§3.1); v2 CRC applies to flagged entries only, so unflagged v2 entries stay
byte-identical to v1 entries (§3.4); `getEntry`/log-decode bounds and finite-domain validation
(§3.6, §3.4); B0 becomes a structural counter assertion and B6 gains direct-put contention
(§6); lifecycle tests (backup/restore/checkpoint/stream/read-only/drop-CF/reserve concurrency)
added (§8). Rev 4 folds in round 3's post-clearance corrections: value staging via `SliceParts`
(the caller's buffer is never touched at all — no mutate-and-restore race on shared memory,
§3.3); the re-put fallback is **deleted** (it reorders `Put→Delete` sequences) — re-stamping is
order-preserving full reconstruction through `RebuildFromWriteBatch` with conflict-tracking
acceptance gates (§3.3); markers are keyed by column-family ID, not name, with dropped-ID rows
retained, making drop/recreate races structurally impossible (§3.7); the metadata CF is created
exclusively and self-identifies with a schema-magic row — a pre-existing user CF of that name
fails closed with a migration error (§3.7); mixed DB+log backups get a floor capture in the
log-snapshot section reconciled at restore (§3.7); a clean-close floor row plus open-time
reserve extension bound restart skew and cold-open latency (§3.7); CRC coverage is defined as
stamp + payload, excluding only the CRC field (§3.4); no-unwind is enforced at the commit-lane
boundary with a non-allocating failure path (§3.2); the §3.1 marker-inheritance contradiction
is resolved (marker + unspecified ⇒ enabled, §3.1); `setTimestamp`'s non-finite rejection and
state-gating are documented as deliberate, dormant-mode-visible bug fixes (§3.2); claim-order
(not per-key commit-order) monotonicity is a documented public contract with the cross-repo
consumer audit recorded as an activation gate (§3.2); the measurement harness ships in-repo
(§3.4). **Rev 5 records the owner's ruling and simplifies to the stamp-as-key model**: variant
(i) refined — on an enabled store the local stamp IS the batch key (first word of entry and
record alike); the clock that diverges is the record _version_, owned by the harper layer.
Consequently there is **no log-format change at all**: rev 3/4's flagged-entry extension,
payload CRC, and v2 header gate are withdrawn (rotation returns in rev 6 — at the key-domain
flip, for ordering hygiene rather than format gating); enabled stores gain
strict per-log key monotonicity (the claim moves under the store append lock for logged
transactions); seek-by-local-stamp becomes the existing binary-index seek; and the issue's
"per-node log key domain stays origin clock" acceptance line is amended per the ruling —
dormant unchanged, receiver-domain keys at activation (per-hop cursors, LMDB's existing model),
with the cursor migration owned by harper#2412 (§3.4). **Rev 6 folds in the round-4 review of
the amended design**: the gate is split two-tier — per-CF markers gate the _record value_
format, while the **log key domain is a database-wide one-way marker** (logs are shared across
CFs via the store registry, so a per-CF gate cannot own a per-log property; once flipped, every
batch in every log of that database is stamp-keyed regardless of initiating CF, and active
segments rotate at the flip so the strict-monotonicity claim is scoped to post-activation
segments — §3.1, §3.4); first activation requires the enabling open to be the descriptor's sole
handle (no opportunistic marker publication racing in-flight writers; metadata-CF
initialization is defined as recoverable across a crash between CF creation and schema row —
§3.1); the clean-close floor row is durably invalidated during open, in the same synchronous
batch as the open-time reserve extension, before any claim (§3.7); reserve extension never runs
while holding a log store's write mutex (pre-reserve outside the lock — §3.2); the reserve
extender gets an explicit lifecycle (descriptor-pinned, registered in `operationsInFlight`,
drained at close, failures caught at every scheduling boundary — §3.2); restore-time floor
reconciliation is a defined pending-floor artifact protocol with crash coverage (§3.7);
database/backup metadata is declared trusted with an extreme-ceiling warning (§3.7); budgets
gain dormant same-process numeric thresholds, an overlapping-transaction re-stamp-rate
benchmark, and a forced-reserve-rollover contention test (§6); and rev-5 editing leftovers
(§7 key-domain bullet, §8 caller-buffer/marker-cleanup/re-put mentions) are reconciled.

## 1. Problem and goal

Harper on RocksDB collapses three identities into the first 8-byte word of every record value:
the record **version** (origin-defined, legitimately non-unique per key), the **local write
identity** (what VT/cache freshness and subscription staleness need — unique per write), and the
**resume cursor** (the transaction-log key, origin clock domain). harper#2065 made version ≠
commit time legitimate, and every consumer that assumed equality is now subtly wrong (see the
plan's consumer table). The end state is three clocks with one owner each:

| Clock                              | Meaning                                                                                     | Owner / uniqueness                                                                                                                                        |
| ---------------------------------- | ------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Record version                     | Conflict resolution, CRDT ordering, replica convergence                                     | Origin-defined; legitimately NON-unique per key                                                                                                           |
| Local mutation stamp (`localTime`) | "This exact write on this node": VT/cache freshness, write identity, subscription staleness | Receiver-locally assigned at commit, unique per write — no replicated-write opt-out                                                                       |
| Resume cursor                      | Per-origin log position for replication resume                                              | Per-hop: a subscriber's cursor indexes the log it reads, in the local clock of the node that wrote it (LMDB's model today; see §3.4's amended constraint) |

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
  (`transaction_handle.cpp:188` — the batch key snapshots the txn timestamp _at that moment_);
  each entry's 13-byte header gets that timestamp stamped **at write time**
  (`transaction_log_file.cpp:595`, the `transaction_log_entry.h:46` "skip timestamp for now"
  comment). The batch timestamp is therefore already "the txn's clock at commit" — for local
  writes a near-commit-time local monotonic value; for replicated applies the origin's clock,
  which is what per-node resume cursors depend on (`replicationConnection.ts:4085-4088`:
  "version === audit-log key" is the documented cursor invariant).
- Entry timestamps are only _mostly_ monotonic per file: `findPositionByTimestamp` builds a
  running-maxima index and documents that it gives no upper bound on an out-of-order log
  (`transaction_log_file.cpp:668-680, 748-766`).
- The commit paths (all four: sync `transaction.cpp:884-974`, legacy libuv `:836-878`,
  single-lane and two-lane commit-worker `:815-830`) share one ordered sequence:
  `executeLogWork` (log `writeBatch` → `committedPosition`) → `executeCommitWork`
  (`txn->Commit()` → `releaseIntent()` → `commitFinished` gated on `status.ok()`).
  `executeLogWork` today catches only `writeBatch` exceptions (`transaction.cpp:360-363`), and
  `CommitWorker::run` invokes tasks with **no** surrounding catch (`commit_worker.h:115-135`) —
  any new throwing work placed on the lanes must convert its own failures to `state->status`.
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
  replicated applies, so it can neither recover a receiver-local maximum nor be trusted as a
  seed (a replicated future key is remotely influenceable input).
- Non-transactional writes: `Database::PutSync`/`RemoveSync` (`database.cpp:2077-2216`) write
  directly with `db->Put`/`Delete` under a VT write-intent lock, on the caller's JS thread —
  _not_ through the commit lanes.
- RocksDB copies key/value bytes into the WriteBatch rep at `txn->Put` time. The batch is
  walkable via the public `WriteBatch::Iterate(Handler*)` (const, const slices), and
  `Transaction::RebuildFromWriteBatch(WriteBatch*)` exists as a supported way to replace a
  transaction's batch contents (`deps/rocksdb/include/rocksdb/utilities/transaction.h:707`);
  per-entry protection metadata is never enabled by this binding.
- Backup copies RocksDB live files plus (optionally) transaction-log snapshots
  (`database/backup.cpp`); checkpoint delegates to RocksDB's `Checkpoint`
  (`database/checkpoint.cpp`); the backup stream enumerates RocksDB live files. **Anything in a
  column family rides all of these for free; any sidecar file rides none of them.**

## 3. Design

### 3.1 Enable gate (dormant by default) and the persistent enable marker

New per-column-family open option `commitStamping?: boolean`, **three-state** (mirroring the
compression option's explicit-vs-inherit discipline):

- `true` — enable; on first enable, durably record the CF in the metadata CF (§3.7) before any
  stamped commit.
- **unspecified** — inherit: the live in-process value if the CF is already open, else the
  durable marker's value (a marked CF opened without the option **is enabled** — the data
  format demands stamping, so the binary that can stamp must stamp; requiring every opener to
  repeat the option would make an option-less reopen an integrity hazard), else off.
- explicit `false` — assert-off: conflicts (throws at open) with a live-enabled handle or a
  durable marker, rather than silently bypassing the stamped-CF contract. Un-marking a CF is
  not provided in stage 1 (a verified rewrite migration would be required first; until then the
  marker is one-way).

Default (absent) on a never-marked CF ⇒ off, and every code path below is inert: no record
bytes change, no log format change, no VT change; the dormant claim is precisely **one flag
branch and zero new calls, allocations, or I/O** (asserted structurally, §6 B0 — a numeric
dormant regression gate would be noise-bound, so benchmarks are reported, not asserted). A
second in-process open of an already-open CF with an explicitly different value is rejected in
`DBRegistry::OpenDB`.

**The gate is two-tier, because its two effects have different owners.** Per-CF marker rows
gate the _record value_ format (rocksdb-js owning the first word of values on that CF). The
_log key domain_ is a **database-wide, one-way marker row**: transaction-log stores are
resolved per (database path, name) and shared by every CF's transactions
(`transaction.cpp:1369-1373`, `transaction_log_store_registry.cpp:193-229`), so a per-CF flag
cannot own a per-log property — with a split gate, an unstamped CF's transaction appending an
origin-keyed batch to the same physical log would silently break the domain promise. Once any
CF is enabled, the database's log key domain flips with it (first enable writes both rows in
one batch): **every subsequent batch in every log of that database is stamp-keyed, regardless
of which CF's transaction wrote it**, and each log's active segment is rotated at the flip so
post-activation segments are purely receiver-domain (the strict-monotonicity guarantee of
§3.4 is scoped to them; pre-activation segments keep their historical keys and the existing
mostly-monotonic reader tolerances).

**First activation is exclusive, not opportunistic.** Publishing the marker while other
handles are mid-write would let a racing direct `PutSync` (already inside `db->Put` having
observed stamping off) land an unstamped value in a just-marked CF — `OperationGuard` pins
lifetime, it does not exclude writes. First enable therefore succeeds only when the enabling
open is the descriptor's **sole live handle** (the normal deployment shape: options set at
boot, first open); otherwise the open fails with an explicit "activation requires exclusive
open" error. Later opens merely inherit/assert. Metadata-CF initialization is defined
recoverably: create the CF, then write the schema row — a crash between the two leaves an
_empty_ CF with no schema row, which the next open completes (writes the row) rather than
misclassifying as a hostile collision; a _non-empty_ CF without a valid schema row still fails
closed (§3.7).

The durable marker is the storage layer's own fail-closed backstop (covering non-harper users
too); the harper-side durable data-format floor (harper#2412 stage 2) remains the
reader/writer pairing authority. Known limitation, owned by #2412's peer/floor gates: a
**pre-stage-1 binary** knows neither the option nor the marker and would write unstamped first
words into a marked CF — package-floor enforcement before enable is #2412's deliverable
(with no log-format byte change there is no log-side fail-closed gate either — the marker
protects only binaries that know it, which is precisely why #2412's floor must land first).

### 3.2 Stamp assignment: keep-if-greater on a bounded, durable receiver clock

New `DBDescriptor` state (process-global per database):

- `std::atomic<uint64_t> localStampWatermark` — float64 bit pattern of the last claimed stamp
  (positive doubles compare correctly as uint64;
  `static_assert(std::atomic<uint64_t>::is_always_lock_free)`).
- `std::atomic<uint64_t> stampReserve` — the durably persisted ceiling (§3.7): no stamp above
  it is ever returned before a higher ceiling is durable.

One Node-free function (`core/local_stamp.{h,cpp}`, GoogleTest-covered, clock injectable):

```
claimLocalStamp(watermark, reserve, candidate, now) -> double
  loop:
    wm = watermark.load(acquire)
    keep = candidate > wm and candidate <= now + MAX_KEPT_SKEW_MS
    next = keep ? candidate : max(now, nextafter(wm))    # receiver-derived, no skew gate
    if next > reserve: extendReserve(next)               # §3.7; returns only once durable
    if watermark.CAS(wm -> next): return next
    # CAS lost: re-check against the new watermark
```

- **Keep path** (the overwhelmingly common case: `candidate` = the txn's `startTimestamp`,
  taken from the same monotonic clock at txn creation, and nothing committed past it): two
  atomic loads and one CAS — **and no clock read at all for internally generated candidates**:
  the handle records candidate _provenance_, and a candidate that came from
  `getMonotonicTimestamp()` (txn construction, direct puts) is already receiver time, so the
  skew check — the only consumer of `now` on this path — is skipped; only a caller-supplied
  (`setTimestamp`) candidate pays the vDSO read to validate skew. **Zero allocations, no
  mutex, no blocking** (the reserve is extended asynchronously ahead of need, §3.7).
- **Re-stamp path** (contention; a caller-set timestamp at/below the watermark — every
  replicated apply whose origin time is behind local commits; a candidate beyond the skew
  bound): `max(now, nextafter(wm))`. This value is **receiver-derived and accepted without a
  skew check**, so the loop always terminates: after a wall-clock rollback larger than
  `MAX_KEPT_SKEW_MS`, the logical clock simply runs ahead of the wall clock from the recovered
  floor (standard hybrid-logical-clock behavior) — no spin, no `CLOCK_BEHIND` failure mode,
  and uniqueness is unaffected because the floor never regresses.
- **Uniqueness is by construction**: the CAS admits any given double at most once per database;
  duplicate candidates (two origins supplying the same timestamp, a caller reusing one) lose
  the CAS or the `> wm` test and re-stamp.
- **Caller-supplied time never owns the receiver clock.** `MAX_KEPT_SKEW_MS` (constant, 1 hour)
  bounds how far a _kept caller candidate_ may sit above receiver time, so
  `watermark ≤ max(now, recovered floor) + MAX_KEPT_SKEW_MS` at all times: a hostile or buggy
  `setTimestamp(Number.MAX_VALUE)` re-stamps at receiver time instead of pushing the clock
  toward float exhaustion (harper additionally caps source-reported versions at `now` since
  #2065, so real workloads keep). `setTimestamp` itself is hardened: reject non-finite values
  and values ≥ 8.64e15 (harper's MAX_DATE_TIMESTAMP domain), and reject calls when the
  transaction is not `Pending` — which also removes the JS-thread-vs-commit-lane race on
  `startTimestamp`, since the commit entry point snapshots the candidate on the JS thread
  before any lane work runs. These two rejections are visible in dormant mode too and are
  **deliberate bug fixes, called out in the PR**: a NaN/Infinity timestamp already corrupts
  transaction-log key ordering today, and a post-dispatch `setTimestamp` is already meaningless
  (the batch key was snapshotted at `addLogEntry`) while racing a plain double across threads.
  Dormant-mode tests pin the new rejections explicitly.

**Overlap is the common re-stamp trigger, not replication alone**: a transaction outlived by
any concurrent commit finds the watermark above its construction-time candidate and re-stamps.
Measured (B10): the worst-case overlap shape costs ~4–30% at p95 for small transactions (median of paired passes); a LARGE
re-stamped batch pays the B4 rebuild on the commit lane (pipelined with the log lane in
two-lane mode). Adoption note for harper#2412: local writes should not round-trip stale
timestamps through `setTimestamp` (that converts receiver-time candidates into caller-supplied
ones and forfeits the keep path).

**Ordering contract — deliberately uniqueness, not durable-write order.** Stamps are claimed
before `txn->Commit()`, and sync commits/direct puts are not serialized with the async lanes,
so a stamp's numeric order can differ from RocksDB durable-write order across concurrent
writers. No surveyed consumer needs the latter: VT freshness and subscription staleness compare
for **equality** (write identity), replication resume and boot replay order by **log
key/position**, and the broadcaster holds a live position-based iterator. The contract is:
(a) unique per write per database; (b) strictly above every stamp _claimed_ earlier (claim
monotonicity — which implies per-key monotonicity for all non-retry commits, since a key's
later writer claims after the earlier writer's claim); (c) a kept caller candidate is bounded
by receiver time + `MAX_KEPT_SKEW_MS`; (d) never reused across restart (§3.7). The single
per-key exception: a pinned retry (below) can commit a key at a stamp claimed before a
conflicting winner's — an equality-safe, position-consistent skew that per-key-order consumers
must not rely on (documented **prominently in the public API docs**, not only here; harper's
post-#2409 staleness checks are equality-based, and the cross-repo audit that every consumer
uses equality/position rather than `>` ordering is an **activation gate recorded in
harper#2412**).

**Pinning across retries.** The stamp is finalized **once per durable WAL batch**: a new
`TransactionHandle::localStamp` field survives `resetTransaction()` exactly like
`committedPosition`, and finalization reuses it whenever
`committedPosition.logSequenceNumber > 0` (the #668 write-once condition). A retried commit
whose log batch is already durable re-applies the _same_ stamp to its re-put records — the only
alternative would diverge record first words from the durable batch stamp (§5 F7). A retry with
no durable batch re-finalizes fresh.

**Finalization site and exception safety.** For a transaction **with** a log batch, the claim
runs inside `TransactionLogStore::writeBatch` **under the store's write mutex, immediately
before the entry headers are stamped** — so per-store append order equals claim order and the
enabled store's keys are strictly monotonic (§3.4); the batch re-stamp of record values then
runs on the same thread after `writeBatch` returns and before `txn->Commit`. For a transaction
**without** a log batch, finalization runs at the top of `executeLogWork` (a deliberate
pass-through on every async lane — two-lane, single-lane, legacy) and at the equivalent point
in `CommitSync`; only uniqueness matters there. In both shapes the candidate is the snapshot
taken at the commit entry point on the JS thread, and everything precedes `txn->Commit` on the
thread that owns the txn at that moment. **No-unwind is enforced at
the lane boundary, not inside one operation — and the boundary includes the queue handoffs**:
the no-throw trampoline wraps lambda construction, `enqueue` (including the two-lane
log→commit forwarding step and the stopped-worker inline-run fallback), stage execution, and
completion dispatch, with a `catch (...)` whose failure path is non-allocating — it sets a
pre-existing failure marker on the state (an enum + a static status, never a
message-constructing `Status` that could itself throw under allocation failure) — so
reserve-extension I/O errors, allocation failure, queue-growth or thread-creation failure
during forwarding, and batch-rebuild failure all reject (or safely abandon, with the
transaction intact per #668 semantics) instead of unwinding into `CommitWorker::run` (which
has no catch and would `std::terminate`); the sync and direct-put paths translate the same
failures to thrown JS errors. Fault tests inject failures at each boundary — before
scheduling, inside execution, during forwarding, during completion — in all three async modes
plus direct writes, and prove process survival.

Non-transactional `Database::PutSync` claims with `candidate = getMonotonicTimestamp()`
(always the keep path in practice) and stamps the value buffer before `db->Put`.

### 3.3 Record stamping: pre-stamp at put, supported-API re-stamp at commit

On a stamped CF, rocksdb-js **owns the first 8 bytes of every value**. A put with
`value.length < 8` on a stamped CF is **rejected with a clear error** (it cannot carry the
word; silent skip would leave holes in the enabled-CF contract). Harper's records are always
≥ 12 bytes. Two-phase stamping:

1. **Segmented staging at `putSync` — the caller's buffer is never touched.** The value is
   staged through the `SliceParts` overloads (`Transaction::Put(cf, SliceParts, SliceParts)`,
   `transaction.h:503-509`, and the matching `DB::Put` overload for direct puts): an owned
   8-byte stack prefix holding the candidate (`startTimestamp`, BE float64) plus a second
   slice over the caller's bytes 8.. — RocksDB concatenates the parts while serializing into
   the batch. No mutate-and-restore, so shared/`SharedArrayBuffer`-backed buffers can never
   observe a transient stamp and a concurrent writer's bytes are never overwritten (pinned by
   an SAB observer/writer test). Cost: one stack write; the memcpy into the batch happens
   either way. The candidate value used is recorded on the handle (`preStampValue`, plus a
   flag when puts spanned more than one candidate — only possible via `setTimestamp` between
   puts, which state-gating still permits while `Pending`).
2. **Re-stamp at commit, only when needed**: after finalization, if
   `localStamp != preStampValue` (re-stamp path, pinned-retry path, or mixed pre-stamp
   values), the batch is rewritten **through supported RocksDB APIs only** — no `const_cast`
   into the batch representation (the public headers say callers must not modify the returned
   batch; Iterate is const; a future integrity/index change would make an in-place write
   memory-unsafe) — as an **order-preserving full reconstruction**: walk
   `GetWriteBatch()->GetWriteBatch()` with a handler that copies _every_ operation, in order,
   into a fresh `WriteBatch` (patching stamped-CF put values as fixed-width substitutions in
   the copy — deletes, unstamped-CF ops, and repeated keys pass through verbatim in their
   original positions), then `Transaction::RebuildFromWriteBatch(&copy)` (`transaction.h:707`).
   O(batch) one-time copy on the re-stamp path only. A per-entry re-put shortcut was
   considered and **rejected**: WriteBatch applies operations in insertion order, so appending
   a patched `Put(k)` after an original `Put(k); Delete(k)` sequence would resurrect a deleted
   key. **Acceptance gates before this path ships** (GoogleTest + Vitest, both txn modes):
   `Put→Delete` and `Delete→Put` orderings, repeated keys, cross-CF batches, and — critically —
   conflict-detection key tracking preserved at the _original_ read/track sequence numbers
   (`RebuildFromWriteBatch` re-drives `txn->Put`/`Delete`, and tracked keys must merge at the
   earliest tracked sequence, not re-track at rebuild time; a red-first test stages a conflict
   between the original put and the rebuild and asserts the commit still reports `IsBusy`).
   If the gate fails, the pinned-RocksDB escape hatch is a fenced in-place fixed-width
   substitution behind layout/protection assertions — a build-time switch, never a silent
   runtime fallback.
   The keep path walks nothing. The handler stamps only CFs with stamping enabled (cf_id →
   descriptor lookup), so a txn spanning stamped and unstamped CFs stays correct.

Deletes carry no value and need no stamp (their write identity lives in the log entry; a
delete-only transaction still claims a stamp for its batch, and `committedLocalTime` — §3.6 —
reports it). `Merge` is not exposed by this binding.

### 3.4 The batch key IS the local stamp — log-architecture ruling (recorded)

**Ruling (task owner, recorded 2026-09-01): variant (i), refined to the stamp-as-key model.**
The commit-time local stamp is not carried _beside_ the log key — on an enabled store it **is**
the batch key: the first word of every entry header, exactly as it is the first word of every
record. The clock that diverges from it is the record **version** (LWW resolution,
`updatedTime`/`createdTime`, `record.getVersion()`), which is not owed the first word of
anything: harper carries it in the record's distinct-version word (§3.6) and already carries it
in-band in every audit payload (`createAuditEntry` writes it; `readAuditEntry` decodes it).
This is LMDB's existing model — lmdb's audit store is keyed by receiver-local commit time with
the version as a separate field — and it is what the entry format has anticipated all along:
the entry timestamp is deliberately left blank at staging and stamped at write time
(`transaction_log_entry.h:46` "skip timestamp for now, it will be written when the batch is
written").

Mechanically, on an enabled store:

- `TransactionLogStore::writeBatch` assigns `batch.timestamp` from `claimLocalStamp` **under
  the store's write mutex at append time** (reserve pre-checked outside the mutex, §3.2), so
  per-store append order equals claim order and the keys are **strictly monotonic per log
  segment created after activation** — an upgrade over today's mostly-monotonic keys that
  gives `findPositionByTimestamp` a true upper bound on those segments (active segments are
  rotated at the key-domain flip, §3.1, so no file mixes domains; pre-activation segments
  keep the existing tolerances). Stamp-keying is governed by the database-wide key-domain
  marker, not the initiating CF (§3.1) — a shared log never mixes origin-keyed and
  stamp-keyed batches after the flip. (Claims are already strictly monotonic globally; taking
  the claim under the append lock is what pins append order to claim order across sync
  commits interleaving with the commit lanes.) The keep path is byte-for-byte today's write:
  candidate == stamp == key.
- Transactions with no log batch claim at commit entry as before (uniqueness only; no ordering
  constraint exists without an append).
- The record patch (§3.3) uses the same claimed stamp, before `txn->Commit` — the atomicity
  contract (§5) becomes simply: **record first words == batch key**.
- Pinned retries (#668) are unchanged: the durable batch's key is the pinned stamp; later
  batches claim later stamps; per-log monotonicity holds because the pinned batch was appended
  at its claim position.

**There is no log-format change at all.** No entry extension, no flag, no CRC, no v2 header:
enabled and dormant stores write identical bytes for identical inputs — what changes at
activation is only the _value domain_ of the key for divergent commits (a replicated apply's
batch is keyed by the receiver's stamp instead of the origin's timestamp). A pre-stage-1
binary parses enabled logs perfectly; the _semantic_ migration of cursors is exactly what
harper#2412 stages 0b/2 gate atomically with the record decoder (and the wire/peer floor).
Consequently rev 3's log-format artifacts are withdrawn: the v2 downgrade gate (nothing to
gate — bytes are unchanged) and the flagged-entry CRC (no new bytes to protect; the documented
Windows torn-payload gap remains exactly today's, out of scope). Rotation survives in a
different role: not a format boundary but an ordering boundary at the key-domain flip (§3.1),
with a **durable lazy-log protocol** for stores not instantiated during activation — the
key-domain marker carries a monotonic domain generation, each log store persists the
generation it last rotated for **together with the post-rotation sequence number** — the
in-memory rotation is only certified once the on-disk sequence reaches it, so a crash between
the rotation and the first append re-rotates on reload instead of trusting a row for a
rotation that never materialized — and a store whose recorded generation predates the marker's
rotates its active segment before its first stamp-keyed append. Post-flip segments are therefore purely
receiver-domain on every store, whether or not it was open at activation.

**Seek-by-local-stamp comes free.** Because the stamp is the key, the existing per-file
running-maxima binary index _is_ the local-stamp index: measured 60µs cold / 6µs warm to seek,
10.1M entries/s (1.4 GB/s) to scan — strictly better than either original variant, with
neither variant (ii)'s +0.93µs/commit (+10%) second append path and doubled file-lifecycle
invariant surface (rotation, recovery, retirement, purge, backup, validation), nor original
variant (i)'s +12B flagged entries and O(scan)-only local seeks. The decision-support harness
ships in-repo as `benchmark/log-architecture-measure.mjs`.

**Amended acceptance constraint (supersedes the issue's "per-node log key domain stays origin
clock" line, per the owner's ruling).** Dormant behavior is unchanged everywhere. At
activation, an enabled store's keys are receiver-local stamps — the per-hop cursor domain: a
subscriber's resume cursor indexes the log it reads, in the clock of the node that wrote that
log, which is LMDB's model today ("on LMDB the cursor must stay on the sender's audit sequence
id", `replicationConnection.ts:4085-4088`). The cursor-site migration is harper#2412 stage
0b/2 work (0b already moves harper-pro cursor sites to `localTime`). One consumer is flagged
to #2412 explicitly: the origin-version keyed dedup/resequencing lookup
(`Table.ts:2525` `auditStore.get(txnTime, tableId, id, nodeId)`) loses its direct key seek at
activation and must move to payload-carried refs, per-node seq-state, or a bounded scan —
recorded there, not solvable here.

**Consumer survey and measurements retained as the decision record** (this machine, release
build, sync commits, 128B payloads):

| Measure                                         | Result                                                           |
| ----------------------------------------------- | ---------------------------------------------------------------- |
| Full log-append path, marginal per commit       | +0.93µs on a 9.2µs no-log commit (+10%); +0.44µs per extra entry |
| Sequential scan rate, `query()` drain           | 10.1M entries/s (1.4 GB/s, mmap)                                 |
| Seek by timestamp via the per-file binary index | 60µs cold / 6µs warm                                             |

No local-stamp _seek_ consumer existed even before the ruling made seeks free: the broadcaster
holds a live positional iterator; replication resume reads its upstream's key domain; the CRDT
resequencing walk needs origin-version lookups (now #2412's migration item above); table/MQTT
resumes tolerate bounded out-of-order; boot replay is position-based
(`startFromLastFlushed`).

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
`{ value, localTime?: number, version?: number } | undefined`, parsing the value-header
contract the VT already defines, with fail-closed bounds: `localTime` = first word (BE float64
at 0) when the value is ≥ 8 bytes and the word is finite and positive, else `undefined`;
`version` = the 8-byte BE float64 at offset 12 **only when** the value is ≥ 20 bytes, the
4-byte metadata word at offset 8 carries `VERSION_HEADER_TAG`, the new
**`HAS_DISTINCT_VERSION_FLAG = 0x20000`** producer flag is set (exported from the binding's
constants next to `VERSION_NOT_UNIQUE_FLAG = 0x10000`, so harper stage 2 sets the same bit this
reads; 0x20000 is unused in harper's record-metadata bitmap), and the word is finite and
positive; otherwise `version = localTime`. A crafted/corrupt 12-byte flagged record can never
cause an out-of-range read. Meaningful only for stores whose producer writes the header
contract (as with the VT flag read); documented as such. Also new:
`Transaction#committedLocalTime` (the finalized stamp, readable after commit — on the keep path
it equals `getTimestamp()`; defined for delete-only transactions too), so a producer can learn
the stamp without re-reading the record. `setTimestamp`/`getTimestamp` doc units are corrected
to milliseconds in passing (typings currently say seconds).

### 3.7 Durable receiver-clock floor and marker: a hidden metadata column family

The receiver clock must never re-mint a stamp that any durable artifact already carries, even
across a wall-clock regression, and the enable marker must be as durable and as _portable_ as
the data it guards. Origin-keyed state cannot provide the floor: the log store's
`latestTimestamp` tracks **batch keys** — for replicated applies, remotely influenced origin
time — so it is never consulted (a replicated future key must not poison, and a purged segment
may have held the maximum). A sidecar file fails the portability half: backup, backup stream,
and checkpoint copy RocksDB files (plus optional transaction logs) and would silently drop it,
so a restore would come up floorless and markerless — permitting unstamped writes or duplicate
stamps on a store full of stamped records.

Both therefore live in a **hidden internal RocksDB column family** (`__rocksdbjs.meta`),
created lazily on first enable:

- **Exclusive creation + schema magic.** The name is inside the user-controllable CF
  namespace, so it is never silently adopted: first enable _creates_ the CF and writes a
  schema row (magic token + schema version) in the same batch; if a CF of that name already
  exists without a valid schema row, open fails closed with an explicit migration error rather
  than interpreting user/attacker bytes as clock state. Every row read is validated (bounded
  lengths, finite positive doubles below 8.64e15) — a crafted or restored database cannot
  inject an extreme reserve or make open misbehave.
- Rows: the schema row; one reserve-ceiling row (float64); one **clean-close floor** row
  (the exact watermark, written on orderly `close()`); one marker row per stamped CF, **keyed
  by the CF's persistent RocksDB ID** (name recorded as a diagnostic value only). RocksDB's
  WAL owns write atomicity/durability (ceiling writes use `WriteOptions.sync = true`); there
  is no bespoke file format, torn-write protocol, or name-length bound to design — and every
  copy lifecycle (backup, stream, checkpoint, restore) inherits the state automatically
  because it is ordinary CF data.
- **ID-keyed markers make drop/recreate structurally safe.** `DropColumnFamily` cannot be
  atomic with any marker write, and same-name recreation is deliberately legal — so markers
  are never deleted at all: a dropped CF's row is retained (harmless garbage, bytes per drop)
  and simply never matches again, because a recreated CF has a new ID and starts unmarked —
  which is also correct, since the drop destroyed the stamped data the marker guarded. No
  cleanup ordering exists to race.
- **Reserve invariant**: no stamp above the persisted ceiling is ever returned by
  `claimLocalStamp`, and **the ceiling itself can only be raised by values the claim could
  actually produce** — `ensureHeadroom` applies the same provenance/skew rule as the claim, so a
  caller-supplied far-future timestamp (which would re-stamp at receiver time) can never durably
  poison the ceiling. The ceiling is extended ahead of need (`RESERVE_WINDOW_MS`, 5 minutes
  above `max(now, watermark)`) by a **single-flight** off-thread task triggered when the
  watermark crosses `ceiling − margin`; the claim path blocks on extension only if claims
  outrun the asynchronous extension (pathological — bounded, surfaced as a commit failure if
  the sync write itself fails, never silent, never a spin). **Reserve extension never runs
  under a log store's `writeMutex`**: the logged-commit path pre-checks the reserve _before_
  acquiring the append mutex (extending there if needed) and the under-mutex claim only
  proceeds when headroom exists — so a metadata fsync can never stall every later log commit
  behind one holder of the append lock. The extender itself has an explicit lifecycle: it
  holds a `shared_ptr` pin on the descriptor, registers in `operationsInFlight`, is drained
  by `close()` before the metadata CF is destroyed, and every scheduling boundary
  (thread/queue creation, enqueue, inline-run fallback) catches failures and converts them to
  a rejected extension (surfacing as a rejected commit), never an unhandled throw.
- **On open**: the watermark seeds from the clean-close floor when present (orderly shutdown —
  no skew, no reserve consumption), else from the ceiling row (crash — consuming at most one
  `RESERVE_WINDOW_MS` of logical skew per crash, so even a crash loop advances the clock
  boundedly: N crashes ≤ N × 5 minutes, and only relative to a wall clock that failed to catch
  up). **The clean-close row is durably invalidated during open itself — deleted in the same
  synchronous batch that performs the open-time reserve extension, before any handle can
  claim** (deleting it lazily at first claim would let this sequence re-mint a stamp: close
  records floor F, reopen, claim and durably log S, crash before the lazy deletion is durable,
  reopen seeds F again). Crash tests cover both sides of that batch. The open-time extension
  also means the first commit never stalls; cold-open latency and restart-skew bounds are
  tested. A CF marker row present is honored per §3.1 (unspecified ⇒ enabled; explicit
  `false` ⇒ fail closed). A database that never enabled stamping has no metadata CF and no new
  open-time work.
- **Mixed DB+log backup coherence.** Directory backup and the backup stream capture RocksDB
  first and transaction logs afterward (`backup.cpp:253-279`, `backup_stream.cpp:435-499`), so
  a commit landing between the captures can put a stamp in the log snapshot that exceeds the
  ceiling in the DB snapshot. The transaction-log snapshot section therefore carries a **floor
  capture** (the live ceiling, read after the log capture completes — ceiling monotonicity
  makes "after" sufficient), written as a small validated record in the log-snapshot
  directory. **Restore's reconciliation protocol is crash-safe by construction, and its
  ordering is load-bearing**: restore durably publishes the selected backup's pending-floor
  artifact **into the destination before the destructive RocksDB restore begins** (today's
  restore purges and installs the DB first, then copies logs — an artifact copied "with the
  logs" would leave a crash window where an older restored ceiling is live while the
  higher-stamped logs arrive later). The artifact survives the restore-mode purge, is
  idempotent, persists until superseded, and **open reconciles it before any claim** — folding
  `max(ceiling row, artifact)` into the metadata CF in the same synchronous open-time batch as
  the clean-close invalidation above, then leaving the artifact in place (a re-run after a
  crashed restore or a crashed first open re-applies the same maximum). Crash tests cover:
  after pending-floor publication, after the DB restore, during destination-log replacement,
  after reconciliation, and full re-run.
  Checkpoints need nothing: a checkpoint is a single RocksDB point-in-time snapshot, so data
  and ceiling are already mutually consistent. Concurrent-backup tests crash at both capture
  boundaries (plus the stream variant) and assert the restored store never re-mints a stamp
  its logs carry.
- **Trust boundary, stated plainly**: the metadata CF and the backup floor artifact are
  _trusted_ content, in the same trust class as the database bytes themselves — schema magic
  is collision detection, not authentication, and a crafted database or backup can already
  fabricate arbitrary data. Values are validated for shape (finite, positive, < 8.64e15), and
  a recovered ceiling far ahead of wall clock (beyond `MAX_KEPT_SKEW_MS` + one reserve window)
  additionally emits a loud global warning event naming the skew and the recovery option, so
  an operator restoring a clock-poisoned artifact is told rather than left with silently
  future-dated local time. Reserve arithmetic is **checked**: extension and `nextafter` never
  produce a value at or beyond the 8.64e15 domain bound, and a database whose recovered
  ceiling leaves no claimable headroom below the bound **fails open with an explicit
  clock-exhaustion error** naming the operator recovery path (rewrite the ceiling after
  fixing the source) — a warning alone is not a control, and unchecked extension from a
  near-bound ceiling would otherwise persist an invalid value or wedge every write.
- The metadata CF is not user-openable by name (the reserved name is rejected at open) and is
  excluded from the public `db.columns` listing; it does appear in the `registryStatus()`
  diagnostic listing — diagnostics report what exists — and
  `LoadLatestOptions`-based per-CF option preservation treats it like any other CF (its
  options are fixed internally: tiny write buffer, no compression concerns). Read-only opens
  read the floor but never extend it (no writes can claim stamps there).

This removes rev-1's no-log residual and rev-2's copy-lifecycle hole: the floor holds with
logs, without logs, across purges, and across backup/restore/checkpoint, because it lives in
the database itself and derives only from receiver state.

## 4. Approaches considered (four axes, same root cause)

| Axis                        | Option                                                                                                                                                                                                                           | Assessment                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Higher layer                | **A. JS-layer stamping** — harper assigns the stamp and writes it into the first word itself at save time                                                                                                                        | Cannot satisfy uniqueness or keep-if-greater: the JS layer cannot see the committed watermark at commit time (commits finalize on the commit lanes, possibly after other envs' commits), cannot patch the already-copied WriteBatch on the re-stamp path, and every worker/env would need a shared claim protocol rocksdb-js already owns natively. LMDB solves it inside the engine for the same reason (lmdb-js substitutes the placeholder at commit). Rejected.                                                                                                                                                   |
| This layer (chosen)         | **B. Native commit-time stamping behind a dormant per-CF gate**, with the durable floor/marker in an internal metadata CF and supported-API batch re-stamping (rev 3 = round 2's "materially better same-layer option", adopted) | Owns the only place where "at commit, above every prior claim" is knowable and where the batch is still replaceable; zero default-path cost via pre-stamp + keep-if-greater; state rides every copy lifecycle for free; dormant until harper's paired decoder (stage 2).                                                                                                                                                                                                                                                                                                                                              |
| Lower layer                 | **C. RocksDB user-defined timestamps** (comparator-suffixed UDT)                                                                                                                                                                 | Changes the key encoding and comparator of existing CFs (on-disk incompatible, bulk migration), puts the timestamp beside the _key_ rather than in the value word that replication payloads and harper's decoder read, and UDT solves point-in-time reads, not write-identity stamping. Also does nothing for the consumer-side conflation. Rejected.                                                                                                                                                                                                                                                                 |
| Do less                     | **D. No format change: fix the broadcaster's gate with its existing physical iterator position, keep the #766 VT refusal, measure the remaining caching hole**                                                                   | Repairs one consumer (the broadcaster restart gap) but leaves the caching hole permanent — every version-reusing write (the `Math.max` clamp on out-of-order merges, `_recordRelocate`, same-key-in-txn) stays unvouchable forever, a standing tax on exactly the merge-heavy workloads the VT exists for; leaves MQTT/table cursors in an ill-defined clock domain; leaves LMDB/RocksDB semantics divergent. This is the status-quo-plus option the three-round harper plan already rejected in favor of staged adoption. Rejected here, though its broadcaster fix is compatible with and complementary to stage 1. |
| Deeper cause (VT-only)      | **E. Native in-memory mutation generation for the VT** — publish a synthetic per-write generation instead of the value word; migrate local subscriptions to physical log positions                                               | Priced: solves only the process-local VT slice (a generation can be returned alongside reads and verified without any format change), but provides **no durable write identity** — nothing for cache-vs-store comparison across restarts, tombstone-prune/blob-scan write identity (harper#2412 stage 2's cleanup migration), LMDB-parity `localTime` on records, or a well-defined cursor domain for MQTT/table resumes. Those need the stamp in the bytes. Rejected as the primary; its position-migration half overlaps D's broadcaster fix.                                                                       |
| Log architecture (within B) | **(i) in-band stamps (flagged-entry CRC, v2 gate)** vs **(ii) receiver-local journal**, with a sparse sidecar index as (i)'s future extension                                                                                    | §3.4 — measured; recommendation (i); ruling owed by the task owner before format work.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |

## 5. Native atomicity contract and fault-injection matrix

**Contract.** On a stamping-enabled CF, for every transaction whose RocksDB commit becomes
durable: every stamped record's first word, the batch's local stamp (in-band or key), and the
value later exposed by `committedLocalTime` are the **same** double, and that double was
claimed exactly once, at or below the durable reserve ceiling. No crash at any boundary can
yield a committed record whose first word differs from its durable batch's stamp, and no
restart or restore-from-backup/checkpoint can re-mint a stamp any durable artifact carries.
(Cross-crash _replay_ — harper re-applying durable log batches whose RocksDB commit was lost —
constructs new transactions and therefore new stamps; replay fidelity in the version domain is
harper#2411's per-write API, out of scope here and unchanged by this design.)

Structural argument: the stamp is finalized before the batch header is written and before
`txn->Commit`, is immutable once any durable artifact exists (pinning via `committedPosition`),
both consumers (record patch, batch write) read the same field on the same thread, and the
reserve is durable before any stamp under it is issued.

The fault matrix proves it at every boundary via a test-only crash seam:
**`ROCKSDB_JS_CRASH_POINT`, read from the environment at startup only** (spawned-child
pattern; no runtime setter is exported — an externally callable `_exit` switch in a production
binding is an outage primitive). Each armed point calls `_exit` so nothing "cleans up".
Epistemic scope, stated plainly: process-death seams prove _ordering_ of the protocol's durable
steps; they cannot prove the kernel/device honored `fsync` — that is the platform contract this
repo's existing crash tests also stand on.

| #   | Crash point (seam)                                                                      | Durable at crash                        | Recovery assertion (child respawn + reopen)                                                                                                                                          |
| --- | --------------------------------------------------------------------------------------- | --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| F1  | after stamp finalize, before `writeBatch`                                               | reserve only                            | no batch visible, no records; next claims exceed the pre-crash watermark (reserve seeding)                                                                                           |
| F2  | mid `writeBatchToFile` (partial append, after first writev chunk)                       | torn entry prefix                       | `recoverTail`/boundary-marker retirement behaves exactly as today (entry bytes are format-identical); no records committed; `validateTransactionLogStore` clean                      |
| F3  | after `writeBatch`, before `txn->Commit`                                                | full batch keyed at the claimed stamp   | batch readable at that key; records absent; a replay-style re-apply after reopen produces records whose _new_ stamps exceed the recovered reserve (never equal to any durable stamp) |
| F4  | after `txn->Commit`, before `commitFinished`                                            | batch + records                         | record first words == batch stamp; recovered floor ≥ that stamp                                                                                                                      |
| F5  | after `commitFinished`, before completion callback                                      | batch + records + watermark bookkeeping | same as F4 (bookkeeping is in-memory)                                                                                                                                                |
| F6  | ENOSPC / short append (existing injection machinery)                                    | partial batch, segment retired          | retirement/rotation invariants unchanged with stamped entries; commit never ran; no stamp divergence                                                                                 |
| F7  | IsBusy after durable batch, crash before retry commits                                  | batch at stamp S                        | reopened store exposes the batch at S; the no-crash variant of this test proves a same-process retry commits records at exactly S (pinning), never a fresh stamp                     |
| F8  | crash between reserve extension and dependent claims (forced-small `RESERVE_WINDOW_MS`) | old or new ceiling row                  | either ceiling recovers to a value ≥ every issued stamp; clock regressed below ceiling on reopen still cannot duplicate                                                              |

Failure-injection (non-crash) companions, asserting **rejection + process survival** (no
`std::terminate` through the commit lanes): reserve-row write failure (sync-write error),
allocation failure in the rebuild path, batch-rebuild refusal, log-append open/short-write
failures — each surfaces as a rejected commit with the transaction intact per existing #668
semantics. F2/F6 also run on Windows (pre-extended zero-padded tail semantics, invariant 5);
the F2 assertion there is scoped to today's semantics — the entry format is unchanged, so the
documented durable-header/partial-payload ambiguity is neither widened nor fixed by stage 1.

## 6. Performance budgets (numeric pass/fail, not comparative-only)

"Zero default-path cost" means precisely: with no stamped CF touched, no claim, no clock read,
no batch walk, no metadata I/O, and no allocation are added — one flag branch only. That is
**asserted structurally, not statistically**: B0 is a debug/test-instrumented counter check
(claims, clock reads, batch walks, reserve I/O, allocations in the new code paths all read 0
after a dormant-mode workload), because a 2% p50 threshold against a recorded baseline is too
noise-sensitive to gate CI. Benchmarks are additionally _reported_ for context. Enabled-path
budgets are asserted enabled-vs-disabled in the same process, interleaved, release build,
stress-gated:

| #   | Scenario                                                                                                                                   | Budget (fail threshold)                                                                                                                                                                                                                                                                                                                                                                                                                              |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| B0  | Dormant mode (feature compiled in, CF not enabled), full write/read/log workload                                                           | structural by construction: `stampState` is null on a never-enabled database and every stamping path is behind that raw-pointer null gate (no shared_ptr refcount traffic); pinned by the dormant byte-identical tests and the full suite. (The originally sketched counter instrumentation was subsumed by the null-gate design.)                                                                                                                   |
| B1  | Enabled, uncontended single-put commit, keep path                                                                                          | p50 ratio enabled/disabled ≤ **1.05**; p99 ratio ≤ **1.10**                                                                                                                                                                                                                                                                                                                                                                                          |
| B2  | Enabled, forced re-stamp every commit (`setTimestamp(1)`), single put                                                                      | median per-round p50 ratio ≤ **1.4** — the measured inherent cost of the supported-API append rebuild is 1.21–1.27×, so the budget bounds regression above that adjudicated cost, not the cost itself                                                                                                                                                                                                                                                |
| B3  | Enabled, 10k-put batch, keep path                                                                                                          | ratio ≤ **1.05**                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| B4  | Enabled, 10k-put batch, forced re-stamp (order-preserving reconstruction + rebuild)                                                        | median ratio ≤ **1.75×** the keep batch (measured inherent append-rebuild cost: 1.43–1.51×; absolute numbers recorded in the PR)                                                                                                                                                                                                                                                                                                                     |
| B5  | Keep-path claim allocation count                                                                                                           | **0** heap allocations — GoogleTest with an operator-new counter around the Node-free `claimLocalStamp` + pre-stamp helpers                                                                                                                                                                                                                                                                                                                          |
| B6  | Contended: 4 worker envs, mixed keep/re-stamp async commits **plus concurrent direct `PutSync` writers**, duplicate-prone stale candidates | global stamp **uniqueness asserted** across the full interleaving; per-mode commit semantics (single-lane/legacy/two-lane, incl. the #668 pinned retry) asserted per mode in `test/commit-stamping-async.test.ts`; contended p95/p99 latency ratios reported, deferred as gates with B9/B10                                                                                                                                                          |
| B7  | Lock-freedom                                                                                                                               | `static_assert(std::atomic<uint64_t>::is_always_lock_free)` compiled on all platforms; claim function exercised from N threads in GTest for progress + uniqueness                                                                                                                                                                                                                                                                                    |
| B8  | Enabled, logged single-entry commit (claim under the store append lock)                                                                    | p50 ratio enabled/disabled ≤ **1.05**; strict per-log key monotonicity asserted across the B6 interleavings; entry bytes asserted format-identical to dormant output for the same inputs                                                                                                                                                                                                                                                             |
| B9  | Dormant numeric backstop against a genuine pre-change baseline build                                                                       | **deferred** (recorded openly): needs a second built binding in one session; the dormant backstop rides the repo benchmark CI against main                                                                                                                                                                                                                                                                                                           |
| B10 | Overlapping transactions (a window of open transactions committing out of creation order — the worst-case re-stamp shape)                  | implemented: worst-case measured at **87.5% re-stamp rate** (7/8 by construction) with median paired-pass p95 ratios of **1.0–1.3×** for single-put commits, asserted ≤ **2.0** as the median of paired passes (single-run p95s at microsecond scales are scheduling-noisy), with the ≥50% re-stamp-rate assertion proving the path was exercised; large-batch re-stamp head-of-line cost is priced by B4. The forced-rollover lock-hold half is structural — reserve I/O cannot run under a log `writeMutex` — and F8 crash-tests the extension boundary |

## 7. Dormancy / adoption contract

- `commitStamping` absent on an unmarked CF ⇒ zero behavior change; the full existing suite
  must pass unchanged on all platforms (including Windows) with the feature merely compiled in
  (plus B0's structural proof).
- Enabled ⇒ record first words, the metadata-CF floor/marker, and (per the §3.4 ruling)
  receiver-domain log keys — with **no log-format byte change**. harper must not enable before
  its distinct-version decoder + durable data-format floor (harper#2412 stage 2);
  cluster-level activation additionally needs #2412's peer gate (new-format bytes and
  _semantics_ — the key-domain change — must not reach peers below the floor) and its
  old-binary package-floor enforcement — recorded there, not re-owned here. rocksdb-js
  enforces what it can locally: the marker fails a forgetful reopen closed, and the cross-open
  conflict check keeps one process from mixing stamped and unstamped writers on a CF.
- Log key domain, per the §3.4 amended constraint: **dormant databases are untouched**;
  activation flips the database-wide key domain to receiver-local stamps (per-hop cursors),
  rotating active segments at the flip. `findPositionByTimestamp` mechanics are unchanged and
  gain a true upper bound on post-activation segments.

## 8. Verification route

- **Unit (Vitest)**: stamp uniqueness/keep semantics across txn + non-txn writes;
  caller-buffer never mutated (SliceParts staging; SharedArrayBuffer observer/writer test,
  including abort and re-stamp paths); short-value
  rejection on stamped CFs; `setTimestamp` hardening (NaN/Infinity/≥8.64e15 rejected;
  non-Pending rejected; between-puts value still patched); `setTimestamp`-after-`addLogEntry`
  (enabled store: the key is assigned at `writeBatch` from the claim regardless of the staged
  snapshot; dormant store: today's snapshot behavior pinned unchanged); multi-CF txn with mixed
  stamped/unstamped CFs; delete-only txns; repeated-key puts; both txn modes incl. conflict +
  retry with the adopted re-stamp shape; `getEntry` bounds/word parsing incl.
  `HAS_DISTINCT_VERSION_FLAG`, 12-byte flagged corpse, non-finite words;
  `committedLocalTime`; VT: stamped value FRESH round-trip, flagged legacy value still refused
  (#766 tests untouched and green); log: divergent-commit batches keyed at the claimed stamp
  (read back via `query()`, `findPositionByTimestamp` upper bound honored on enabled stores),
  strict per-log key monotonicity under sync+async+two-lane interleaving, entry bytes
  format-identical to dormant output, purge/rotation on enabled stores; metadata CF:
  enable-marker fail-closed
  reopen, explicit-false conflict, hidden from listings, dropped-CF marker rows retained and
  recreated same-name CF starting unmarked, first-activation exclusivity (second live handle
  present fails the enabling open; racing direct writes cannot straddle the marker),
  mixed-gate/shared-log coverage (stamped and unstamped CFs appending to one log after the
  flip are all stamp-keyed), interrupted metadata-CF initialization (empty-CF-no-schema-row
  resumes; non-empty fails closed), read-only
  open; **lifecycle**: backup + restore, backup stream, and checkpoint each carry the metadata
  CF (floor + markers) and stamped data coherently — restore then reopen enforces the marker
  and never re-mints a stamp, including **concurrent** backups with commits landing between
  the DB and log captures (floor-capture reconciliation) and crashes at both capture
  boundaries; drop-then-recreate-same-name (ID-keyed markers: recreated CF starts unmarked);
  crash between marker publication and the first stamped commit; metadata-CF namespace
  collision (pre-existing user CF named `__rocksdbjs.meta` fails closed) and crafted/malformed
  rows; SharedArrayBuffer-backed value buffers observed by a second worker during puts (no
  transient stamp visible, no lost concurrent write); cold-open latency and restart-skew
  bounds (clean close vs crash loop); reserve-extension concurrency (single-flight,
  forced-small window); dormant-mode byte-identical write assertions **plus the two documented
  dormant-visible `setTimestamp` rejections**.
- **GoogleTest (Node-free)**: `claimLocalStamp` keep/re-stamp/skew-cap/reserve semantics with
  an injected clock (rollback > `MAX_KEPT_SKEW_MS` terminates — the rev-2 spin case; no-log;
  post-purge), uniqueness under threads, B5/B7; the order-preserving batch reconstruction
  round-trip against a real `WriteBatchWithIndex` (including the conflict-tracking gate for
  `RebuildFromWriteBatch` and the `Put→Delete`/`Delete→Put`/repeated-key order assertions).
- **Fault injection**: §5 matrix as spawned-child crash tests (fixtures pattern) plus the
  non-crash failure-injection companions (rejection + survival), F2/F6 on Windows CI.
- **Benchmarks**: §6 budgets asserted (B0 structural); existing bench suite for regression
  context.
- **End-to-end route**: full `pnpm test` on Node/Bun/Deno + Windows CI (ThinLTO/gyp caveats per
  repo history) proves dormant compatibility. Activation end-to-end (harper encoder preserving
  origin versions, apply-side two-word behavior, broadcaster resume on stamps, restart,
  backup/restore, mixed legacy/stamped records, mixed-version cluster) is **harper#2412's
  rollout gate on the paired adoption branch — recorded there as a cross-repo gate,
  deliberately not claimed as coverage by this repository**.
