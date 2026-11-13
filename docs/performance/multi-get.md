# Batch-read costs and measurements (#1019)

`getMany()` saves binding calls and async work, but batching is not always faster.
In this run, two cached decoded keys cost 13% more per key with `getManySync()`
than with `getSync()`. Four were within 2%; batches of 16–256 saved 13–16% on that
path. Cold transaction batches of 16 were 4% slower, while 64–256 saved 10–14%.
Use individual reads for small cached workloads unless a representative measurement
supports batching. These results do not establish a universal crossover size.

## Work per batch

For N keys and V bytes of found values (excluding allocations inside RocksDB):

- Both approaches encode N keys. Batching reuses the JS key and offset buffers,
  crosses N-API once for a synchronous read, and allocates native key slices,
  N statuses and N pinnable results. Individual decoded reads can use the shared
  value buffer; batching allocates a packed JS buffer of `4 * N + V` bytes,
  copies V bytes into it, and creates an array plus one Buffer view per found key.
- A cold non-transactional batch first attempts a cache-only read, then copies the
  keys and queues one worker for the whole batch. The worker packs V bytes into
  a native string; completion copies that string into a JS buffer. Individual cold
  reads can queue one worker per key. The async baseline here includes `Promise.all`.
- A cold transaction batch additionally captures staged writes, deletes and cache
  hits in a second cache-only pass on the owning thread. It retains the transaction's
  shared snapshot and captured values, and allocates key/index/result arrays for the
  misses. Only those misses reach `DB::MultiGet` on the worker. This extra work keeps
  the worker independent of transaction mutation, commit and reset, without blocking
  writes on a pending disk read. It does not add work to fully cached transaction batches.
- A decoder declaring `needsStableBuffer` gets a `Buffer.from` copy of each value
  before decoding, so a retained view cannot retain the packed batch. Small copies
  may still share Node's bounded Buffer pool. The default msgpack decoder already
  copies embedded binary values and does not need this extra input copy. The binary
  APIs intentionally retain their shared packed allocation, as documented in the README.
- One-key batches use the existing single-key path; empty batches do no native read.

## Local measurement

Measured on 2026-11-13 with the review fixes applied to `b583c3a`, Node 24.16.0,
RocksDB 11.13.2, Apple M4 Max, macOS arm64. No tests or builds ran concurrently.
The shared host was not CPU-pinned or isolated; one-minute load averages were
2.67–3.16 across the six runs. Treat small differences as host-sensitive.

The database contains 1,000,000 records of roughly 100 bytes, flushed and compacted.
Hot runs use a 1 GiB block cache and read all records before timing. Cold runs use
1 MiB, with the OS page cache warm: this measures block-cache misses, not cold-device
latency. Each arm reads about 20,000 keys per sample, with one discarded warmup
repetition and nine measured repetitions. Arms are interleaved in shuffled order
with different deterministic key sequences to limit immediate cache reuse.

Each cell below is **single-key / batch nanoseconds per key**, at the median.
The binary row uses `encoding: 'binary'` for both APIs. The transaction row uses
one optimistic transaction with snapshots enabled and no staged writes. Staged
writes, custom decoders, larger values, other runtimes and other machines can change
these results. Hot async timings include `await` and `Promise.all` overhead; they
do not measure callers consuming synchronous `get()` results directly.

| Mode                             |         1 key |      2 keys |      4 keys |     16 keys |     64 keys |    256 keys |
| -------------------------------- | ------------: | ----------: | ----------: | ----------: | ----------: | ----------: |
| Hot, sync, decoded               |   1698 / 1695 | 1676 / 1890 | 1646 / 1620 | 1667 / 1446 | 1660 / 1391 | 1664 / 1402 |
| Hot, sync, binary                |   1381 / 1435 | 1450 / 1401 | 1393 / 1204 |  1366 / 965 |  1342 / 927 |  1353 / 933 |
| Cold, sync, decoded              |   2657 / 2679 | 2665 / 2826 | 2663 / 2476 | 2634 / 2167 | 2652 / 2100 | 2645 / 2076 |
| Cold, async, decoded             | 10868 / 10473 | 8593 / 7427 | 6756 / 5218 | 3896 / 3513 | 3647 / 2791 | 3537 / 2623 |
| Hot, async, decoded              |   1935 / 1822 | 1864 / 1950 | 1838 / 1686 | 1797 / 1464 | 1783 / 1416 | 1739 / 1393 |
| Cold transaction, async, decoded | 10955 / 10762 | 8630 / 8427 | 6778 / 5901 | 3915 / 4062 | 3660 / 3303 | 3596 / 3106 |

Run from the repository root with a freshly built native binding. The first command
creates the database; reuse the same path for the remaining commands:

```sh
node benchmark/multi-get.mts /tmp/multi-get-data --reps 9 --api sync --cache hot
node benchmark/multi-get.mts /tmp/multi-get-data --reps 9 --api sync --cache hot --encoding binary
node benchmark/multi-get.mts /tmp/multi-get-data --reps 9 --api sync --cache cold
node benchmark/multi-get.mts /tmp/multi-get-data --reps 9 --api async --cache cold
node benchmark/multi-get.mts /tmp/multi-get-data --reps 9 --api async --cache hot
node benchmark/multi-get.mts /tmp/multi-get-data --reps 9 --api async --cache cold --transaction
```
