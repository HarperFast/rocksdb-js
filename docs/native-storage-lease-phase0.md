# Native storage lease: Phase 0 design

- **Issue:** [HarperFast/rocksdb-js#831](https://github.com/HarperFast/rocksdb-js/issues/831)
- **Consumer experiment:** [HarperFast/fulltext#7](https://github.com/HarperFast/fulltext/issues/7)
- **Status:** Phase 0 implementation and qualification plan
- **Base:** `origin/main` at `7ab102ca3e9600343bcefe6f19204b111836ec52`
- **Package:** rocksdb-js 2.8.0, embedding RocksDB 11.8.1

## Objective

Prove the smallest safe native interface through which another Node addon can use a
caller-owned rocksdb-js database and selected column family. The first consumer is
`@harperfast/fulltext/rocks`, whose Tantivy `Directory` executes on Rust-owned threads and cannot
route each storage operation through JavaScript.

rocksdb-js remains the only owner of the RocksDB instance, column-family handles, close sequence,
durability policy, locks, statistics, and allocated read buffers. The consumer receives operations,
not RocksDB C++ objects. It does not link RocksDB or depend on rocksdb-js C++ class layouts.

Phase 0 is an experiment, not a published API. It adds a build-gated lease producer so fulltext can
measure the real addon-to-addon path and crash it at controlled points. The production interface is
promoted only after the experiment identifies the required capabilities and durability strategy.
Set `ROCKSDB_JS_NATIVE_STORAGE_LEASE=1` when building the coordinated test addon; ordinary builds do
not define the hidden `Database.__nativeStorageLease()` method.

## Required invariants

### Ownership

- One `DBDescriptor` and one RocksDB instance continue to serve all handles for an open database.
- The lease never exposes `rocksdb::DB*`, `rocksdb::ColumnFamilyHandle*`, C++ standard-library
  objects, or rocksdb-js class layouts.
- The lease object does not keep an otherwise unused database open. Its provider state is revoked
  through the descriptor's existing `Closable` graph. An admitted operation protects the raw
  descriptor through a separately owned shared operation gate without adding a registry-visible
  `shared_ptr<DBDescriptor>` reference.
- Every buffer returned across the ABI is released by a provider function with its provider-owned
  context. The consumer never frees provider memory.

### Close and drop

- Admission and close form one gate: either an operation is admitted and close waits for it, or
  close wins and the operation receives `closed` without touching RocksDB. Lease operations are
  bounded and observe the gate's cancellation state so foreign work cannot hold synchronous close
  behind an unbounded scan or queue.
- Close revokes future lease operations before resetting `DBDescriptor::db` and waits for every
  admitted operation to leave.
- A dropped and recreated column family is a new incarnation even if RocksDB reuses its name or
  numeric ID. Already-admitted operations may finish against their retained old handle, matching
  RocksDB's documented drop semantics; later lease calls fail as stale.
- A stale lease fails by identity/lifetime validation; it never follows a reused pointer or numeric
  ID into a new database or column family.

### Errors and language boundaries

- Every provider entry point is a C ABI function declared `noexcept`.
- Provider entry points catch C++ exceptions and return a stable status. No exception crosses into
  Rust.
- The fulltext consumer catches Rust panics before a provider callback frame can unwind.
- Status messages and result buffers have explicit lengths. All returned data is bounded and has a
  provider release function.

### Durability

- The caller selects an explicit write policy for every batch: WAL enabled/disabled and sync
  enabled/disabled. `sync=true, WAL=false` is rejected.
- Publication must never make metadata durable ahead of an object it references.
- The bridge does not join a Harper source-record transaction. Harper's derived-index protocol is
  post-commit and watermark-recoverable; the full-text index may lag and replay.
- A target-column-family flush is not promoted merely because it is possible. The fulltext crash
  matrix first tests whether one shared ordered WAL plus a synchronous metadata batch already gives
  the required durability with less write-stall exposure.

## Existing rocksdb-js primitives

The implementation must extend these primitives rather than create parallel ones:

| Concern           | Existing owner                                            | Grounding in current source                                                                                                                             |
| ----------------- | --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Database identity | `DBDescriptor`                                            | `vtEpoch` is process-unique for each open lifecycle.                                                                                                    |
| Database sharing  | `DBRegistry`                                              | Reuses a process-global descriptor for a path/read-only identity and coordinates reopen with close.                                                     |
| Column family     | `ColumnFamilyDescriptor`                                  | Shared by every `DBHandle` that opens the same live family.                                                                                             |
| Close graph       | `Closable` plus `DBDescriptor::attach()`                  | Descriptor close drains attached resources before resetting RocksDB.                                                                                    |
| In-flight work    | `operationsInFlight`                                      | `finishClose()` waits for it before workers, flush, handles, and RocksDB are torn down.                                                                 |
| Writes            | RocksDB `WriteBatch` paths                                | Existing transactions and direct writes already select `WriteOptions`, including `disableWAL`.                                                          |
| Flush             | `DBDescriptor::flush()`                                   | Snapshots every live column family and flushes them as one atomic-flush job; transaction-log durability tracking depends on that database-wide meaning. |
| Locks             | `DBDescriptor::locks`                                     | One per-database registry currently serves JavaScript callbacks and handle-owned release.                                                               |
| Observability     | RocksDB statistics, background errors, write-stall events | Already attached to the shared descriptor.                                                                                                              |

The existing close fence is correct because both atomics use sequentially consistent ordering.
Every current acquire increments `operationsInFlight` before reading `closing`, while close sets
`closing` before reading the count. In the single sequentially consistent order, an acquirer cannot
both observe `closing=false` and be absent from the closer's count observation. An acquire that
lands after close must observe `closing=true`, roll its count back, and touch no RocksDB state.

The problem is enforceability across a native ABI, not a demonstrated race. The protocol currently
lives in a macro and several hand-written counter manipulations. Phase 0 encapsulates the existing
ordering as one Node-free `OperationGate` used by `OperationGuard`, checkpoint, backup, drop, and
the lease. This changes ownership and admission APIs without changing the hot-path synchronization
algorithm.

## Architecture

```text
JavaScript
  rocksdb-js Database for one column family
       │ test build only: request NativeStorageLease External
       ▼
rocksdb-js native addon
  StorageLeaseState
    atomic revocable DBDescriptor reference
    shared OperationGate + weak ColumnFamilyDescriptor
    copied database epoch + column-family epoch
    function table + provider image/build identity
       │ C ABI, no JavaScript callbacks on I/O path
       ▼
fulltext native addon
  validate External/type tag/header/capabilities once
  retain lease context
  call from Tantivy threads
       │
       ▼
atomic DBDescriptor admission gate
  retained old column-family handle for each admitted call
  existing RocksDB instance, WAL, caches, stalls, errors
```

The lease has two lifetime levels:

1. The N-API `External` and the consumer's explicit retain keep only `StorageLeaseState` alive.
   That state contains identities, an atomic raw descriptor reference, a shared operation gate, and
   a weak column-family reference, so it cannot prevent database closure.
2. Each successful operation admission owns a shared gate claim and promotes only the
   column-family weak reference. The descriptor remains alive because close owns it while closing
   and cannot pass the gate's drain point. The operation keeps the column-family handle until the
   call completes, then returns only consumer-owned or copied provider bytes.

`StorageLeaseState` implements `Closable` and is attached to `DBDescriptor` as the same shared
object owned by the lease context. `StorageLeaseState::close()` marks its revocation word and stores
null to its atomic raw descriptor reference. A call acquire-loads the pointer and tries to claim the
state-owned gate before dereferencing the descriptor. If close won, the gate rejects admission and
the pointer is never dereferenced. If admission won, close cannot pass the drain point and the
descriptor remains live. There is no lease mutex or serialized point-read entry path.

Lease creation itself first acquires an operation claim, registers the column-family usage, creates
the state, and completes `DBDescriptor::attach()` before releasing the claim. `attach()` becomes a
fallible operation that refuses a descriptor already marked closing. This prevents close from
passing the closables sweep before a newly returned state has joined it. If close starts after
attachment but before JavaScript receives the External, the state is revoked by the normal close
pass and the returned lease is already closed rather than dangling.

The lease deliberately does not use a temporary `shared_ptr<DBDescriptor>`. `DBRegistry` decides
whether to purge after the last `DBHandle` closes by inspecting the descriptor's reference count.
A native operation that raised that count could make the purge skip, then finish on a non-JavaScript
thread with no safe existing completion point at which to retry `PurgeIfUnreferenced()`. Atomic
admission supplies the required lifetime without changing registry ownership.

An owned read copies bytes into provider-owned memory before releasing operation admission. No
RocksDB-owned memory, pin, iterator, or column-family handle crosses the end of admission. A future
zero-copy design would need a different close contract and is out of Phase 0.

## Atomic operation admission

Add a separately allocated, `alignas(64)`, Node-free `OperationGate` containing the existing
sequentially consistent `closing` flag and operation count. `tryAcquire()` performs one
unconditional `fetch_add`, then reads `closing`. When closing is set it immediately releases the
count and returns false. `beginClose()` exchanges the closing flag to true. `waitForDrain()` waits
for the count to reach zero. Release decrements the count and notifies when it observes closing and
becomes the last admitted operation.

The gate retains the existing 32-bit count. Its counter and closing flag are private, so direct
manipulation no longer compiles. A move-only RAII claim releases on every return path. A foreign
claim owns `shared_ptr<OperationGate>` through the decrement and possible final `notify_all()`, so
the closer may destroy `DBDescriptor` after observing zero without freeing the gate beneath the
releasing thread. Existing `OperationGuard` can borrow the gate because it already retains the
shared descriptor across its destructor; it does not add another shared-pointer refcount pair.
Checkpoint and backup paths migrate to the same API.

Node-free interleaving tests prove both sides of the sequentially consistent invariant and the
absence of a lost wakeup when the final release races `beginClose()`. A native microbenchmark and
the existing JS point-operation benchmark compare the encapsulated gate with the current inline
sequence at 1, 8, and 32 threads/worker environments. The helper stays header-inline. The
release-build sequence is inspected to ensure it remains one count RMW plus one closing load, and
benchmark results must be indistinguishable from the baseline within the harness's measured noise.

The lease call sequence is:

```text
acquire-load raw descriptor
  └─ null -> CLOSED
tryAcquire shared OperationGate
  └─ failure -> CLOSED
lock weak column-family descriptor
  └─ failure -> STALE_COLUMN_FAMILY
compare db epoch and cf epoch
check descriptor->db and column live/revoked state
execute RocksDB operation
RAII claim releases operation admission
```

Every lease operation has fixed input/result limits and can read `isClosing()` from its claim.
Point reads and iterator steps use RocksDB's best-effort `ReadOptions` deadline and I/O timeout.
Batch mutation count and bytes are capped, and writes
set `no_slowdown=true` so RocksDB reports an incomplete/busy status instead of placing an indexer
behind write-stall backpressure. Recovery scans are page-based rather than one long iterator call;
each page stops at its entry/byte limit or cancellation. Close logs a diagnostic if lease claims do
not drain within the configured diagnostic interval, but it still waits rather than freeing live
RocksDB state.

The descriptor admission is acquired before promoting/using the column family so descriptor close
cannot clear the family map. `ColumnFamilyDescriptor` gains an atomic revoked flag. A successful
drop sets that flag on the shared descriptor and unregisters its name. A call admitted just before
drop may retain and use the old handle: RocksDB's
[column-family contract](https://github.com/facebook/rocksdb/wiki/Column-Families) documents that
outstanding handles remain usable after `DropColumnFamily`, and actual removal waits for their
destruction. Writes use `ignore_missing_column_families=false`, so a batch that RocksDB cannot apply
returns an error rather than false success. No shared/exclusive CF lock blocks the Node event loop
behind a native scan.

## Incarnation identities

`DBDescriptor::vtEpoch` already supplies the database-open identity and is copied into the lease.
It is not reinterpreted as an address.

`ColumnFamilyHandle::GetID()` is a RocksDB identifier, not a process-lifetime incarnation token.
Phase 0 adds a monotonically allocated `uint64_t incarnation` and an atomic revoked flag to
`ColumnFamilyDescriptor`. Each descriptor construction, including loading existing families at a
new database open and recreating a dropped name, receives a fresh nonzero incarnation. The lease
copies that value. Numeric exhaustion fails creation rather than wrapping.

The same descriptor gains cold-path usage registration under one mutex. It counts live
verification-table-enabled handles and live storage leases. Lease creation succeeds only when the
VT count is zero; opening a VT-enabled handle succeeds only when the lease count is zero. This
prevents a second handle from bypassing a check made only on the issuing handle. Close/finalization
unregisters exactly once.

These tokens are diagnostic and equality identities only. They are not pointers, authorization
secrets, or persistence identifiers.

## N-API envelope and C ABI

The test build exposes a hidden native method on an open database instance. It returns an N-API
`External` tagged with one fixed 128-bit `napi_type_tag` shared by provider and consumer source.
There is no TypeScript export in Phase 0.

The consumer validates in this order:

1. value type is `napi_external`;
2. `napi_check_object_type_tag()` matches;
3. the fixed prefix's magic, ABI major, and minimum `struct_size` match;
4. the requested capability bits are present;
5. provider build identity, embedded RocksDB version, and process image token are compatible; and
6. `retain(context)` succeeds before the callback returns and the JavaScript value may be collected.

The pointed-to prefix is fixed-width C data. The consumer reads no later member until
`struct_size` proves it exists. Reserved fields are zero. A major version changes incompatible
layout or semantics; a minor version only appends fields/capabilities. Function signatures use
fixed-width integers and `{pointer,length}` spans, never `bool`, `size_t`, C++ strings, or C++
ownership types.

ABI v1 layout (the C header is authoritative):

```c
typedef struct {
  uint64_t magic;
  uint32_t abi_major;
  uint32_t abi_minor;
  uint32_t struct_size;
  uint32_t status_size;
  uint64_t capabilities;
  uint64_t provider_image_token;
  uint64_t database_incarnation;
  uint64_t column_family_incarnation;
  uint32_t rocksdb_major;
  uint32_t rocksdb_minor;
  uint32_t rocksdb_patch;
  uint32_t reserved;
  ByteSpan provider_build_identity;
  void* context;

  uint32_t (*retain)(void* context, StatusBuffer* status);
  void (*release)(void* context);
  uint32_t (*poll_state)(void* context);
  uint32_t (*get_owned)(void* context, ByteSpan key,
                        OwnedBytes* result, StatusBuffer* status);
  uint32_t (*write_batch)(void* context, const StorageMutation* mutations,
                          uint64_t mutation_count, uint64_t mutation_stride,
                          uint32_t policy, StatusBuffer* status);
  uint32_t (*scan_page)(void* context, ByteSpan prefix,
                        ByteSpan start_after, uint64_t entry_limit,
                        uint64_t byte_limit, OwnedBytes* page,
                        StatusBuffer* status);
  uint32_t (*collect_stats)(void* context,
                            StorageStats* result, StatusBuffer* status);
} FulltextStorageLeaseV1;
```

This is the implemented Phase 0 table, not yet a published API. Qualification records which
functions were called and promotes only those that pass the capability gates. Pinned reads, native
locking, target-CF flush, and `multi_get` are omitted rather than implemented speculatively.

The shared header is valid C and does not spell C++ `noexcept`; every C++ implementation function
is nevertheless declared `noexcept`. Functions return only a fixed-width status code. Optional
details go into caller-owned fixed storage described by `StatusBuffer`, whose size the caller
provides. ABI v1 requires the exact v1 status size; the provider fills the supplied buffer and never
replaces its data pointer. Catch-all paths write a constant bounded message without allocation.
Result structs also carry caller-declared sizes so an older provider never writes a field it does
not know exists. A valid owned-result struct is cleared before every operation, including non-OK
returns.
Every function accepts a null status pointer or a zero-capacity message buffer and returns its code
without writing. The provider validates all outer struct sizes before reading later fields.

Phase 0 fixes conservative negotiated limits before any allocation: keys at 64 KiB, owned values
and scan pages at 64 MiB, batches at 65,536 mutations and 64 MiB total input, and scan pages at
4,096 entries. Arithmetic is checked before pointer addition or narrowing to the host/RocksDB
types. Hitting a limit returns a stable `LIMIT` status so fulltext can chunk work.

`release(context)` and result-buffer release each consume exactly one retained reference. Calling
either twice is a consumer contract violation with undefined behavior; Phase 0 tests balanced
ownership, not an impossible post-free double-release diagnostic.

`poll_state(context)` reads a provider-owned atomic and returns active, closing, or revoked without
admitting a RocksDB operation. fulltext checks it between publication stages. A write that returns
`CLOSED` before metadata publication aborts that Tantivy commit, discards the in-memory writer, and
reopens the last durable head; the derived-index watermark causes the source changes to replay.
Revocation therefore cannot be mistaken for successful publication.

Phase 0 tests `napi_type_tag_object`/`napi_check_object_type_tag` on an External under Node, Bun, and
Deno. Failure is an explicit unsupported-runtime result; the bridge does not silently replace type
validation with the magic prefix.

The image token is generated by one provider-addon image and is stable for that image's process
lifetime. fulltext records the first accepted provider token process-wide and rejects a different
one, preventing leases from two independently loaded rocksdb-js binary images from being mixed in
one consumer runtime. The build identity and embedded RocksDB version make the diagnostic
actionable; compatibility still derives from the ABI and capabilities, not package version text.
The type tag, magic, and image token prevent accidental misuse inside one trusted process. Another
native addon can forge them and is already in the same address-space trust domain; this interface
is not a security or authorization boundary.

## Candidate operations

### Owned read

`get_owned` uses `rocksdb::PinnableSlice` internally, copies the value once into provider-owned
storage, then returns an immutable span plus release context. Not-found is a distinct status, not an
empty value. Keys and values are byte strings; no rocksdb-js application encoding is applied.

This is the mandatory correctness baseline because its lifetime ends cleanly at the ABI. The
consumer benchmark records allocation count and copied bytes.

### Atomic batch

`write_batch` accepts an ordered array of puts and deletes and builds one RocksDB `WriteBatch` for
the leased column family. The caller supplies the element stride; ABI v1 requires it to match the v1
mutation layout exactly. The provider reads and validates each element once while constructing the
batch. The batch uses the shared database's normal RocksDB write path and returns only after that
call finishes.

Write policy is explicit:

| Policy   | `disableWAL` | `sync` | Use in experiment          |
| -------- | -----------: | -----: | -------------------------- |
| WAL      |        false |  false | immutable object fragments |
| WAL_SYNC |        false |   true | published metadata/head    |
| NO_WAL   |         true |  false | flush-barrier candidates   |

The ABI has no transaction pointer and does not expose transaction begin/commit. fulltext owns the
logical grouping and writes a complete binding/head in one batch.

Lease creation is rejected when the issuing `DBHandle` has `enableVerificationTable=true`.
Full-text bytes belong in a dedicated column family and are not Harper record values; bypassing the
handle's verification-table write-intent protocol on a record CF could let a reader publish stale
data as fresh. The provider also fixes `ignore_missing_column_families=false` for every lease batch.

Lease creation rejects read-only and pessimistic database descriptors. Pessimistic
`TransactionDB::Write` can wait on key locks independently of `no_slowdown`, which would let a
foreign writer hold synchronous close behind the database lock timeout. On supported optimistic
databases, `no_slowdown` moves write-stall waiting out of RocksDB; it does not make index writes
harmless to other column families. On `Busy`/`Incomplete`, fulltext backs off and throttles its bounded ingest queue rather
than immediately spinning. Phase 0 records per-CF flush-job frequency, L0 file count, pending
compaction bytes, `rocksdb.stall.micros`, write-stop events, and foreground write latency while
sweeping index load. A load level that causes record-path stalls fails even if every lease batch
returned quickly.

### Prefix scan

`scan_page` creates an iterator, copies at most the negotiated entry/byte bounds into one
provider-owned encoded page, and destroys the iterator before releasing admission. A completed
entry-count page does not advance the iterator again, and a non-empty partial page is returned if
the read deadline expires after making progress. The next call
uses the last returned key as `start_after`. There is no callback into Rust and no RocksDB iterator
or slice crossing the admission boundary. Phase 0 uses scans for reopen/recovery and
garbage-collection experiments, not query posting reads.

ABI v1 encodes a page as a little-endian `uint32_t` entry count, followed by each entry's
little-endian `uint64_t` key length, little-endian `uint64_t` value length, key bytes, and value
bytes. The provider caps a page at 4,096 entries and 64 MiB. The consumer validates all lengths,
integer conversions, entry boundaries, and trailing bytes before exposing slices.

### No target-column-family flush

Phase 0 does not expose a target-CF flush. rocksdb-js enables RocksDB atomic flush and its
transaction-log listener treats a completed flush job's largest sequence as a database-wide
durability watermark. A lease-only CF flush could advance that watermark past unflushed record
column families and retire replay data incorrectly. Allowing write stall during such a flush would
also conflict with current transaction-log teardown ordering.

The WAL-disabled durability control invokes the existing database-wide `DBDescriptor::flush()`
through the JavaScript coordinator. If Candidate A fails, a target-CF design is a separate
rocksdb-js change that must first preserve transaction-log watermark semantics; it is not a hidden
function-table capability.

### Locking stays in fulltext

Phase 0 does not adapt `DBDescriptor::locks`. That registry identifies owners as weak `DBHandle`
references; an empty native owner would be treated as expired by `lockReleaseByOwner()` and could be
released when an unrelated handle closes. The fulltext runtime supplies Tantivy's writer exclusion
for the experiment and proves that the runtime is shared across supported Node worker environments.

If production must coordinate independently loaded fulltext addon images, that requirement gets a
separate generic native-owner design in rocksdb-js. It is not hidden inside this storage ABI.

### Statistics

The lease exposes a fixed, bounded experiment struct: get, scan, and batch operation counts;
requested, returned, and copied bytes; live owned buffers; and provider errors. Existing RocksDB statistics
remain authoritative for cache, WAL, compaction, and stall metrics and are read through current
rocksdb-js APIs outside the hot path.

There is no arbitrary statistic-name lookup in the C ABI.

## Durability experiment

The fulltext consumer tests these candidates against the same mapping and crash schedule:

| Candidate | Object writes       | Barrier                            | Metadata publication          |
| --------- | ------------------- | ---------------------------------- | ----------------------------- |
| A         | WAL, unsynced       | none                               | WAL_SYNC batch in the same DB |
| B         | NO_WAL              | current database-wide atomic flush | WAL_SYNC batch                |
| C         | WAL_SYNC each write | none                               | WAL_SYNC batch                |

Candidate A is preferred if RocksDB 11.8.1 recovery proves that a synchronous later batch in the
same database makes earlier ordered WAL writes durable. The test reads and asserts live
`two_write_queues=false`, `unordered_write=false`, `manual_wal_flush=false`, `atomic_flush=true`,
and the selected `wal_recovery_mode`. A small `max_total_wal_size` forces at least one WAL rotation
between object writes and metadata publication; rotation before, during, and after the sync is a
named crash schedule, not incidental load. Candidate B is the current public durability control;
Candidate C is a cost ceiling.

The child process is killed before, during, and after object writes, a writer flush, an object
barrier, metadata publication, reload, deletion, and generation swap. After reopen, each visible
head must resolve every referenced object at its published length. The previous complete head is an
acceptable result; a visible partial or missing object is not.

### Backup and restore

A whole-database backup may capture unreferenced fragments from an in-progress fulltext commit, but
the durable head must still reference only a complete object set. Restore validates the fulltext
head and its derived-index watermark against the restored source state. A valid older head is
opened and replay advances it. A missing object, invalid head, or watermark outside the restored
source history discards the derived index and rebuilds it; it never makes the restored source data
unavailable. Tests restore backups taken before object writes, between object and metadata writes,
and after metadata publication.

## Build and packaging boundary

The Phase 0 producer is guarded by a node-gyp variable populated from a dedicated environment
variable using the existing `ROCKSDB_ASAN` configure-time pattern. Test builds set it, compile the
lease source, and export the hidden N-API method. Release/prebuild/package workflows leave it off
and assert the export is absent from every prebuild. The TypeScript source,
declarations, package exports, README, and public native class omit the experiment.

The shared C header used by rocksdb-js and fulltext is copied at a pinned revision for the
experiment and byte-for-byte compared in their coordinated integration job. Production promotion
must choose one canonical published header source and add compile-time size/offset assertions on
both sides.

## Test plan

### Node-free native tests

- atomic admission versus close at each interleaving;
- acquire/close/release interleavings, including the final-release notification race;
- database and column-family incarnation uniqueness across close/reopen and drop/recreate;
- batch policy validation and ordered put/delete construction;
- owned buffer release after lease revocation;
- prefix bounds, pagination, entry/byte limits, cancellation, and malformed spans;
- provider exception conversion, including allocation failure seams; and
- catch-all status handling with null, zero-capacity, undersized, and full status buffers.

The gate test seam can pause between the final decrement and notification so descriptor destruction
wins deterministically while the shared gate remains valid. Linux CI builds the Node-free gate and
provider-state tests with ThreadSanitizer; ASan/UBSan remain required for the native consumer path.
Malformed-input fuzz cases assert through a test accessor that every return path restores the gate
count to its prior value.

### rocksdb-js N-API tests

- test-only export is absent in a normal build and present in a Phase 0 build;
- External type tag and fixed header are present;
- External finalization balances lease retain/release;
- lease creation is admitted and attached before its operation claim is released; `attach()`
  refuses an already-closing descriptor;
- stale calls after `close()`, `destroy()`, drop, and worker-environment teardown fail safely;
- revocation between object writes and metadata publication aborts and reopens the prior head;
- an admitted delayed call makes close wait, while a later call is rejected;
- the last JavaScript handle can close during a native operation without keeping the database
  registered after that operation completes;
- lease creation refuses a column family with any verification-table-enabled handle, and a later
  VT-enabled open refuses a column family with a live lease;
- dropped-column writes return an error and never report a discarded batch as successful;
- backup/restore at each publication boundary produces a valid older head, a valid new head, or an
  explicit rebuild decision;
- a separately copied/loaded addon image is rejected by the consumer; and
- current JavaScript CRUD, transactions, backup, close, flush, locks, write-stall events, and
  worker-thread behavior remain green after the shared admission-gate change.

The build-gated producer includes a deterministic non-JavaScript-thread fixture. It starts a native
operation, races `close()`, and proves that close waits for the admitted call while rejecting a
later call. The fixture controls both sides with condition variables and a close-drain test seam,
not timing sleeps. The operation observes close cancellation and exits within its bound; the test
also asserts event-loop delay while the synchronous close drains. A second test races a paginated
prefix scan with column-family drop and proves the JS event loop is not waiting on a lease-owned CF
mutex; the current page either completes against its retained old handle or returns a stable error.

A small test consumer addon, compiled only in the Phase 0 test target, consumes the C table from a
genuine non-JavaScript thread. A child-process fixture uses it to prove object write, forced WAL
rotation, WAL_SYNC metadata publication, process termination, reopen, and referenced-object
validation without requiring Tantivy or the fulltext repository. It also asserts that the
database-wide flush control advances transaction-log durability only for the atomic all-CF job.

### Cross-addon fulltext tests

- consume the tagged External in `@harperfast/fulltext` without RocksDB headers or libraries;
- run Tantivy create, commit, reload, search, close, and reopen through the lease;
- run the full Directory contract, crash matrix, and fault injection from fulltext issue #7; and
- run AddressSanitizer/undefined-behavior instrumentation around C++ callbacks and Rust panic seams.

### Performance tests

Measure owned point reads, ordered batches, paginated scans, and the database-wide flush control
against the existing JavaScript path. Run warm/cold, sequential/random, payload-size, concurrency,
one/many index, and foreground-write interference sweeps. Record p50/p95/p99, throughput, CPU,
allocations, copied bytes, event-loop delay, cache metrics, WAL/compaction bytes, stalls, queue
depth, and RSS.

The bridge passes Phase 0 only with:

- zero contract, crash, stale-lease, sanitizer, or unwind failures;
- zero fulltext-attributable RocksDB write-stop events in the steady mixed workload;
- foreground point-write p99 regression no greater than 10% and 1 ms absolute;
- publication barrier p99 no greater than 250 ms;
- no unbounded queue, buffer, or RSS growth in a 30-minute soak; and
- native Directory-operation p99 at least 50% faster than the JavaScript control.

The rocksdb-js end-to-end route is its C-consumer child-process crash fixture. The product route is
the coordinated fulltext child-process test: JavaScript opens the caller's
database, passes a Phase 0 lease to fulltext, fulltext creates and queries a real Tantivy index, the
child is killed at named publication points, and the parent reopens through rocksdb-js to verify the
last complete index head. This behavior is not observable end-to-end inside rocksdb-js alone.

## Deliverables and promotion

Phase 0 in rocksdb-js delivers:

1. encapsulated, cancellable descriptor admission shared by existing and lease operations;
2. column-family incarnation/revocation state;
3. a build-gated N-API External producer;
4. the bounded state, owned-read, batch, scan-page, and statistics C ABI and provider implementation;
5. native and N-API lifecycle/fault tests;
6. cross-repository integration instructions; and
7. raw metrics identifying required, optional, and rejected capabilities.

The change also adds one concise repository invariant documenting the sequentially consistent
admission/close ordering and the rule that no RocksDB-owned memory may outlive an admission claim.

The production change is a separate reviewed promotion. It removes the Phase 0 naming, freezes the
ABI header, documents compatibility, enables the selected capabilities in release builds, and adds
the public rocksdb-js method used by standalone `@harperfast/fulltext/rocks` callers. Harper pins a
qualified rocksdb-js/fulltext pair; npm semver alone is not treated as native ABI compatibility.

## Approaches considered

### Different layer: implement Tantivy storage in rocksdb-js

Rejected. rocksdb-js would own Tantivy file semantics, commit publication, garbage collection, and
search-engine upgrades. The generic storage provider should not depend on Tantivy.

### Deeper cause: let fulltext own a separate RocksDB database

Rejected for the caller-owned Rocks backend. A sibling database would avoid cross-addon lifetime
coordination, but it would add another RocksDB instance, block cache, write-buffer/fd budget, RocksDB
version, and recovery lifecycle. More importantly, rocksdb-js backup, checkpoint, close, and restore
operate on its database as a whole; a sibling index database would silently fall outside those
Harper operations. Native filesystem storage remains a supported standalone fulltext backend, but
Harper releases use the caller-owned Rocks backend.

### Deeper cause: avoid a new durability primitive

Selected as the first experiment. Write immutable objects through the existing ordered WAL, then
publish metadata with a synchronous batch. If recovery proves the ordering, no target-CF flush is
needed. If it fails, the existing database-wide atomic flush is the control and any narrower flush
requires a separate transaction-log watermark design.

### Do less: use existing JavaScript CRUD and database-wide flush

Retained as the control. It is functionally expressive enough for a prototype, but every Tantivy
operation crosses JavaScript and native thread scheduling, and the only flush affects unrelated
column families. Measurements, not intuition, decide whether the native lease is justified.

### Transport alternative: expose RocksDB pointers or C++ wrappers

Rejected. This couples allocator, C++ ABI, RocksDB version, ownership, and close behavior across
addons and cannot reject stale database or column-family incarnations.

RocksDB's own `rocksdb/c.h` is also insufficient. It avoids C++ layout coupling but still exposes
opaque database/column-family pointers with no rocksdb-js descriptor admission, incarnation,
revocation, allocator, or close contract. Wrapping those pointers safely recreates this operation
lease.

### Execution alternative: run all lease operations on a rocksdb-js executor

Rejected for the sustained Directory path. A provider-owned thread or pool would simplify close by
cancelling queued work before it begins, but every point read would add an enqueue, wakeup, context
switch, and completion handoff. A single lane would also serialize Tantivy's concurrent readers;
a pool would duplicate the scheduling and backpressure already owned by fulltext. The chosen
foreign-thread calls instead admit directly, use bounded non-callback operations, and observe close
cancellation.

### Ownership alternative: make the lease a strong rocksdb-js handle

Rejected for Phase 0. A durable JS-owned handle would prevent database revocation during a Tantivy
commit and could call the existing detach/purge tail when explicitly closed. It would also change
`db.close()` semantics, let a leaked fulltext object pin the database indefinitely, and require the
final native release to be marshalled onto a live JavaScript environment. The revocable lease keeps
rocksdb-js as the authoritative owner; explicit state plus failed-publication recovery makes a
mid-commit close visible and replayable.

### Column-family alternative: require exclusive lease ownership

Rejected because ordinary non-VT rocksdb-js handles can safely inspect or administer the same
column family and do not carry a conflicting cache protocol. The cold-path usage registry excludes
the known incompatible pair—VT-enabled handles and native leases—without imposing a broader
single-handle rule. New per-handle protocols must register their compatibility instead of silently
sharing a leased family.

### Chosen: versioned operation lease over rocksdb-js ownership

The lease preserves a single database owner and provides only bounded operations. Its close fence,
column identities, locks, write policies, and observability are extensions of existing rocksdb-js
primitives. Capabilities that do not earn their cost in Phase 0 remain absent.
