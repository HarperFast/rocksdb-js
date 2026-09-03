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

For the separate clock-rollback concern, reading segment headers only to warn was considered.
Rejected because a generic transaction-log store cannot tell locally issued keys from timestamps
adopted from another origin; warning on the largest key would report normal peer skew as a local
clock rollback. Persisting local-issuer provenance would solve that ambiguity, but requires a new
durable format and backup/downgrade policy and is not a prerequisite for Harper's dual-version
record work.

### Chosen

Keep `setTimestamp()` as the adoption API, reject invalid timestamp values, and reject changes once
the transaction is no longer pending or has staged a write or log entry. Preserve the existing
retry behavior: a reset with no durable batch clears staged writes and may adopt again, while
`committedPosition` freezes a timestamp whose batch already landed. Remove the second-word read API,
flag, and clock-floor-at-open implementation from this PR. The first two belong to Harper's record
codec; the clock-floor policy addresses a pre-existing restart/clock-rollback concern and cannot
distinguish locally issued keys from adopted origin keys in generic transaction logs. The existing
`latestTimestamp` behavior used to populate new transaction-log segment headers remains unchanged.

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

Retaining the removed clock-floor branch's far-future header bound would change the append path using
a ten-year policy introduced only for that floor. This change instead restores `latestTimestamp` and
segment-header behavior exactly to `origin/main`; any provenance-aware clock rollback design belongs
in separate work.
