# Transaction timestamp integrity

## Intent

Harper's source-record version and a RocksDB transaction's write identity are separate values.
Harper's `RecordEncoder` and `PrimaryRocksDatabase` already own the record value layout and decode
its metadata; rocksdb-js owns the transaction timestamp and uses it as the transaction-log batch
key. This change only makes overriding that timestamp safe for Harper's two legitimate adopters,
replication apply and crash replay.

## Invariant

Within one transaction attempt, the timestamp cannot change after the transaction leaves `Pending`
or stages a database write or transaction-log entry. A retry may adopt again only when the failed
attempt did not durably write its log batch; once a batch is durable, its timestamp remains frozen
across retry resets. Encoding a transaction timestamp into record bytes remains the producer's
obligation because native code cannot observe when a producer reads and copies the timestamp.

## Requirements traced to Harper

- HarperFast/harper#2065 intentionally allowed a source record's version to differ from the
  transaction timestamp.
- HarperFast/harper#2409 fixed the immediate subscription regression by carrying both values in
  Harper's audit representation.
- HarperFast/harper#2411 and HarperFast/harper-pro#790 require replay and replication respectively
  to adopt the origin transaction timestamp through `setTimestamp()` before applying writes.
- HarperFast/harper#2412 owns the optional second version word in `RecordEncoder`, including its
  encoding, decoding, compatibility gates, and call-site audit.

Checked against Harper's source rather than the issue text: `resources/RecordEncoder.ts` decodes the
value header itself (producing `localTime`, `version` and the metadata flags) and owns every flag bit
in that word, and `resources/PrimaryRocksDatabase.ts#getEntry()` overrides the base read entirely —
it calls `super.getSync()`/`super.get()` and reads the encoder's output. So nothing in Harper would
call a rocksdb-js decoder for the second word. Harper's three `setTimestamp()` call sites
(`resources/DatabaseTransaction.ts` at the read-transaction, save and retry-replay paths) each set it
on a freshly constructed transaction before any write, which is what this change makes enforceable.

One consequence for #2412 that is not in its call-site audit: `PrimaryRocksDatabase#getEntry()` passes
the cached entry's `version` as `expectedVersion` to the verification table, and the table keys on the
value's **first** word. Once `version` becomes the record version, that argument has to become
`localTime`, or cached reads stop hitting the fast path — and a record version that happened to equal
the key's current first word would report a stale copy as fresh.

## Approaches considered

### Different layer

Teach rocksdb-js to define and decode Harper's second version word. Rejected because Harper's
`PrimaryRocksDatabase#getEntry()` already overrides the base read path and receives the structured
entry produced by Harper's `RecordEncoder`; a second decoder in rocksdb-js would duplicate the
layout and cannot supply Harper's other metadata fields.

Reserving Harper's proposed `0x20000` bit in rocksdb-js without decoding it was also considered.
Rejected because rocksdb-js does not allocate the producer's metadata namespace: Harper already
defines the other optional-field flags in that word, while rocksdb-js knows `0x10000` only because
the native verification table gives that specific bit behavior. A bit with no native behavior or
public rocksdb-js contract belongs beside Harper's encoder and downgrade gate.

### Deeper cause

Remove `setTimestamp()` and redesign replay and replication so rocksdb-js always issues a new local
timestamp. Rejected because Harper's record-to-log lookup and per-origin resume cursor require the
origin transaction timestamp to remain the transaction-log key on every receiver.

A construction-time timestamp option was considered as a safer replacement for the setter. It
would add another public API and migrate the replay and replication callers, but could not remove
the existing setter compatibly; therefore it would not eliminate the state it is meant to prevent.

### Do less

Make no rocksdb-js change and rely on Harper always calling `setTimestamp()` before staging writes.
Rejected because the current public API permits a later call, after the producer has encoded the old
timestamp or the log batch has captured it, which can split one transaction across two identities.

For the separate clock-rollback concern, seeding the process clock from the largest durable key
across the database's transaction logs was considered and rejected on Harper's own log layout.
`resources/RocksTransactionLogStore.ts` opens one log per origin node — `useLog('local')` for locally
originated writes and `logById(nodeId)` for each peer — and a receiver deliberately keeps the origin's
clock as the key in the peer's log (HarperFast/harper-pro#790). A "largest key in the database" seed
therefore reads adopted peer keys as if they were locally issued: one peer running ahead ratchets this
node's clock on its next restart, and the node then originates those keys to its own peers. Reading
the headers only to warn has the same ambiguity — normal peer skew would be reported as a local clock
rollback. A correct floor has to be told which log is locally originated (a per-log opt-in, or an
explicit "raise the floor to this value" call Harper makes from its own `local` log), which is a
different API than the one the issue sketched and is not a prerequisite for the dual-version record
work.

### Chosen

Keep `setTimestamp()` as the adoption API, reject invalid timestamp values, and reject changes once
the transaction is no longer pending or has staged a write or log entry. Preserve the existing
retry behavior: a reset with no durable batch clears staged writes and may adopt again, while
`committedPosition` freezes a timestamp whose batch already landed. Remove the second-word read API
and flag from this PR; they belong to Harper's record codec. The clock-floor-at-open implementation
is retained as an explicit, caller-named option: it scans only the locally originated log, and
fails the opted-in open when it cannot prove a complete, plausible floor. The existing
`latestTimestamp` behavior used to populate new transaction-log segment headers remains unchanged.

Document what is true about log keys today, which the issue also asked for: entries are appended in
commit order rather than timestamp order, an adopted key carries another node's clock, keys are
unique only within a process, and `findPositionByTimestamp` is a running-maxima seek rather than a
sorted binary search.

## Verification route

The existing transaction tests cover all transaction modes, invalid inputs, post-write and
post-dispatch rejection, log-entry staging, and a coordinated retry whose log batch is already
durable. README and AGENTS documentation will state the enforceable transaction-layer rule and the
producer-owned record boundary. The repository build, native tests, full Vitest suite, and checks
will run before push.

## Alternatives rejected

Freezing the timestamp on the first `getTimestamp()` call would break existing documented and tested
read-then-override behavior. A getter is observational for callers that do not encode its result,
and making every read a state transition still could not prove that a producer's record bytes match
the transaction timestamp. The enforced boundary therefore remains the first native staging event;
producer ordering is documented explicitly.

The clock-floor scan's far-future check stays in the open path rather than changing appends: an
implausible key refuses the explicitly opted-in open, while normal append and segment-header
behavior remains unchanged. A floor based on unnamed logs remains out of scope because it cannot
distinguish locally issued keys from adopted peer keys.

## Clock-floor fail-closed amendment

The task owner explicitly selected restart-safe key uniqueness over availability when
`timestampFloorLog` is configured. This changes that option's failure behavior only; omitting the
option preserves the existing process-local clock behavior.

### Approaches considered

#### Do less

Warn and raise the floor from the readable prefix. Rejected: an unreadable segment, framing break,
or exhausted budget can hide a durable key, leaving the next process able to issue the same key.

#### Different layer

Make `getMonotonicTimestamp()` persist its own global floor. Rejected: the timestamp source cannot
know which transaction-log keys this process originated, and a global persisted value would add a
separate durability protocol without proving it covers the caller's log.

#### Deeper cause

Require every transaction-log append to use an externally durable counter. Rejected: that changes
the transaction timestamp contract and replication/replay adoption path, far beyond the opt-in
startup safety check requested here.

#### Chosen

After recovery, scan every segment in the caller-named local log under the bounded deadline. Reject
the open before raising the process-wide floor if the scan is incomplete or sees an implausibly
future key. A recoverable read-only torn tail remains valid because its contiguous prefix covers all
durable entries. The named-log-missing warning remains non-fatal so a fresh local log can be named
before its first append.

### Verification route

The forked integration fixture writes a named log in one process and opens it in another. It verifies
successful seeding, then independently corrupts a segment, exhausts the deadline, skips discovery,
creates a mid-file framing break, and writes an implausible key; each unsafe case must reject the
opted-in open. Native tests cover scan classification and the repository build/check gates validate
the binding and TypeScript surface.
