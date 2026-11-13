# Batch-read costs and measurements (#1019)

A cached decoded batch reads through the shared value buffer, as `get()` does, so it
no longer allocates per batch. With that, `getManySync()` of two cached keys costs
about the same per key as `getSync()` (within ±2%), and batches of 4–64 cost 8–22%
less. Before the change, two cached keys cost 9–13% more than two `getSync()` calls.

One case is still slower: a cold `getMany()` in a transaction, with 6–8 keys, cost
10–12% more per key than `Promise.all()` over `get()`. Non-transactional cold
batches of that size are at parity. See [cold transaction batches](#cold-transaction-batches).

## Work per batch

For N keys and V bytes of found values (excluding allocations inside RocksDB):

- Both approaches encode N keys. Batching reuses the JS key and offset buffers,
  crosses N-API once for a synchronous read, and allocates native key slices,
  N statuses and N pinnable results.
- A cached decoded batch whose `4 * N + V` bytes fit the 64 KiB shared value buffer
  is written there, like a `getSync()` value. The built-in msgpack decoder reads each
  value in place with `{ start, end }`, so the batch allocates only its result array;
  any other decoder gets one `subarray` view per found key.
- A larger batch, a binary encoding, and the `getManyBinary*()` APIs allocate one
  packed JS buffer of `4 * N + V` bytes and one view per found key. The binary APIs
  return those views, so retaining one retains the whole batch, as the README says.
- A cold non-transactional batch first attempts a cache-only read, then copies the
  keys and queues one worker for the whole batch. The worker packs V bytes into
  a native string; completion copies that string into a JS buffer and decodes it.
  Individual cold reads queue one worker per key, so `Promise.all()` over `get()`
  can use several libuv threads at once. The async baseline includes `Promise.all`.
- A cold transaction batch additionally captures staged writes, deletes and cache
  hits in a second cache-only pass on the owning thread. It retains the transaction's
  shared snapshot and captured values, and allocates key/index/result arrays for the
  misses. Only those misses reach `DB::MultiGet` on the worker. This extra work keeps
  the worker independent of transaction mutation, commit and reset, without blocking
  writes on a pending disk read. It does not add work to fully cached transaction batches.
- A decoder declaring `needsStableBuffer` gets a `Buffer.from` copy of each value
  before decoding, so a retained view cannot retain the batch or the shared buffer.
  Small copies may still share Node's bounded Buffer pool. The default msgpack decoder
  already copies embedded binary values and does not need this extra input copy.
- One-key batches use the existing single-key path; empty batches do no native read.

## Local measurement

Measured on 2026-11-13 at the review fixes on top of `230430fb` merged with `main`,
Node 24.16.0, RocksDB 11.13.2, Apple M4 Max, macOS arm64. No tests or builds ran
concurrently. The shared host was not CPU-pinned or isolated; one-minute load
averages were 2.4–3.1 across the runs. Treat differences of a few percent as noise.

The database contains 1,000,000 records of roughly 100 bytes, flushed and compacted.
Hot runs use a 1 GiB block cache and read all records before timing. Cold runs use
1 MiB, with the OS page cache warm: this measures block-cache misses, not cold-device
latency. Each arm reads about 20,000 keys per sample, with one discarded warmup
repetition and nine measured repetitions. Arms are interleaved in shuffled order
with different deterministic key sequences to limit immediate cache reuse.

Each cell is the median **nanoseconds per key**, with the change against the
single-key arm. "Before" is the batch path without the shared buffer: a packed
buffer per batch, decoded through one view per value. It was measured in the same
run, through `getManyBinary*()` plus `decodeValue()`, which is what the old decoded
path did. A one-key batch takes the single-key path in both versions. The binary
row uses `encoding: 'binary'`, whose batch path did not change. The transaction rows
use one optimistic transaction with snapshots enabled and no staged writes. Hot async
timings include `await` and `Promise.all` overhead.

| Mode                    | Keys | Single key |      Before |       After |
| ----------------------- | ---: | ---------: | ----------: | ----------: |
| Hot, sync, decoded      |    2 |       1680 | 1891 (+13%) |  1694 (+1%) |
|                         |    4 |       1653 |  1645 (−0%) |  1517 (−8%) |
|                         |    8 |       1704 | 1527 (−10%) | 1407 (−17%) |
|                         |   16 |       1660 | 1444 (−13%) | 1391 (−16%) |
|                         |   64 |       1639 | 1410 (−14%) | 1349 (−18%) |
| Hot, sync, binary       |    2 |       1416 |  1386 (−2%) |  1364 (−4%) |
|                         |    4 |       1381 | 1140 (−17%) | 1153 (−16%) |
|                         |    8 |       1361 | 1004 (−26%) | 1013 (−26%) |
|                         |   16 |       1360 | 1025 (−25%) | 1045 (−23%) |
|                         |   64 |       1419 |  959 (−32%) |  960 (−32%) |
| Cold, sync, decoded     |    2 |       2673 | 2949 (+10%) |  2678 (+0%) |
|                         |    4 |       2659 |  2541 (−4%) | 2346 (−12%) |
|                         |    8 |       2660 | 2330 (−12%) | 2229 (−16%) |
|                         |   16 |       2658 | 2215 (−17%) | 2100 (−21%) |
|                         |   64 |       2639 | 2157 (−18%) | 2062 (−22%) |
| Hot, async, decoded     |    2 |       1940 |  2045 (+5%) |  1841 (−5%) |
|                         |    4 |       1926 | 1739 (−10%) | 1622 (−16%) |
|                         |    8 |       1959 | 1585 (−19%) | 1508 (−23%) |
|                         |   16 |       1897 | 1538 (−19%) | 1441 (−24%) |
|                         |   64 |       1900 | 1491 (−22%) | 1391 (−27%) |
| Cold, async, decoded    |    2 |       8785 | 7663 (−13%) | 7600 (−13%) |
|                         |    4 |       6941 | 5364 (−23%) | 5427 (−22%) |
|                         |    8 |       4239 |  4271 (+1%) |  4223 (−0%) |
|                         |   16 |       4080 |  3718 (−9%) | 3532 (−13%) |
|                         |   64 |       3877 | 2912 (−25%) | 2834 (−27%) |
| Hot transaction, sync   |    2 |       1835 |  2005 (+9%) |  1798 (−2%) |
|                         |    4 |       1834 |  1712 (−7%) | 1571 (−14%) |
|                         |    8 |       1771 | 1592 (−10%) | 1480 (−16%) |
|                         |   16 |       1775 | 1533 (−14%) | 1411 (−21%) |
|                         |   64 |       1772 | 1460 (−18%) | 1420 (−20%) |
| Cold transaction, async |    2 |       9004 |  8853 (−2%) |  8678 (−4%) |
|                         |    4 |       6902 | 6099 (−12%) | 6084 (−12%) |
|                         |    8 |       4260 | 4714 (+11%) | 4709 (+11%) |
|                         |   16 |       4181 |  4143 (−1%) |  4104 (−2%) |
|                         |   64 |       3995 | 3415 (−15%) | 3357 (−16%) |

### Cold transaction batches

A second run at 4–32 keys reproduced the cold transaction result: +10% at 6 keys,
+12% at 8, then −1% at 12 and −13% at 32. Non-transactional cold batches were at
parity (−0% and −1%) at 6 and 8 keys. The cause is not isolated: skipping
`Store.getMany()`'s cache-only attempt, which the transaction's owning-thread pass
repeats, only moved 6 and 8 keys to +9% and +7%. Both arms are wall time for one
caller: the `Promise.all()` arm spreads its reads across libuv threads, so it uses
more CPU than the one worker a batch uses.

## Reproducing

Run from the repository root with a freshly built native binding. The first command
creates the database; reuse the same path for the remaining commands:

```sh
node benchmark/multi-get.mts /tmp/multi-get-data --reps 9 --api sync --cache hot
node benchmark/multi-get.mts /tmp/multi-get-data --reps 9 --api sync --cache hot --encoding binary
node benchmark/multi-get.mts /tmp/multi-get-data --reps 9 --api sync --cache cold
node benchmark/multi-get.mts /tmp/multi-get-data --reps 9 --api async --cache hot
node benchmark/multi-get.mts /tmp/multi-get-data --reps 9 --api async --cache cold
node benchmark/multi-get.mts /tmp/multi-get-data --reps 9 --api sync --cache hot --transaction
node benchmark/multi-get.mts /tmp/multi-get-data --reps 9 --api async --cache cold --transaction
```

The "before" column came from the same script with a third arm that decodes
`getManyBinarySync()`/`getManyBinary()` results value by value.
