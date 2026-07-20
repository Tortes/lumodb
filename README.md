# LumoDB

LumoDB is an immutable-row key/value store for compiler and build outputs. The
public data API supports automatic and caller-selected row routing:

```cpp
Put(column, key, value);         // Automatic row.
PutStructs(column, entries);     // Automatic-row batch.
Put(column, rowId, key, value);  // Explicit row.
PutRowStructs(column, rowId, entries);  // Explicit-row batch.
Flush();
Get(column, key, value);
Get(column, rowId, key, value);
GetRowStruct(column, rowId, key, value);  // Compatible explicit-row lookup.
```

Automatic columns provide their expected scale and let LumoDB choose a stable
row count. Explicit columns supply row IDs directly. Both modes share the same
sequential write and compact read engine, can be selected independently per
column, and may even coexist within one column without collisions. Callers never
choose a hash bucket count.

The old object and `PutUniqueStructs` APIs are not part of this format.
`StructEntry`/`PutStructs` and `RowStructEntry`/`PutRowStructs` remain available
for efficient automatic and caller-routed batch writes.

## Creating and reading a database

```cpp
#include "lumodb/database.h"

LumoDB::DatabaseOptions options;
options.expectedEntryCountPerColumn = 1'000'000'000ULL;
options.expectedAutomaticColumnCount = 1;
options.expectedExplicitRowCount = 64;
options.expectedExplicitEntryCount = 10'000'000ULL;
options.averageKeyBytes = 16;
options.averageValueBytes = 256;
options.writerThreadCount = 16;       // 0 = hardware concurrency
options.rowShardCount = 16;           // 0 = automatic, capped at 32
options.memoryBudgetBytes = 128ULL * 1024 * 1024 * 1024;
options.oneShotBuild = true;           // Empty DB, unique keys, one Flush.

LumoDB::Database db;
LumoDB::Status status = db.Open("output.lumodb", options);
if (!status) {
  // handle status.Message()
}

db.Put("symbols", "name", "encoded-value");
db.Put("syntax", 42, "node-name", "encoded-node");
db.Flush();

std::string value;
db.Get("symbols", "name", value);
db.Get("syntax", 42, "node-name", value);
std::vector<std::byte> flatBufferBytes;
db.GetRowStruct("syntax", 42, "node-name", flatBufferBytes);
db.Close();

// Routing options are stored in row_index.lumori.
db.OpenReadOnly("output.lumodb");
db.Get("symbols", "name", value);
```

Binary values use `std::span<const std::byte>` and
`std::vector<std::byte>` overloads. Explicit row IDs must be smaller than
`2^63`; `expectedExplicitRowCount` is the expected total number of explicit
rows across all columns and pre-sizes the outer index. The optional
`expectedExplicitEntryCount` helps choose spill parallelism for explicit-heavy
builds. `PutRowStructs` stages all entries under one row/partition lock and
preserves input order, so the last duplicate key in a batch wins at `Flush`.
`PutStructs` hashes the column once, routes records in bounded chunks, and
holds each used spill-partition lock once per chunk instead of once per key.
`GetRowStruct` restores the legacy explicit-row name and directly probes the
specified row; it is equivalent to the explicit-row `Get` overload.

`oneShotBuild` is intended for immutable compiler output. The database must be
empty, the caller must guarantee every `(column, resolved row, key)` is unique,
and no write is accepted after the first successful `Flush`. Duplicate keys are
not checked and violate the mode's contract. The option is build-only and is
not required by `OpenReadOnly`.

`Put` is safe to call concurrently. `Flush`, `Close`, and `Get` must not race
with Put calls. A batch becomes readable and durable at `Flush`; `Get` returns
`kInvalidArgument` while uncommitted Put calls exist instead of silently
returning an old value. `Close` flushes a successful pending batch.

## Automatic row sizing

For a new database, LumoDB chooses:

1. A target entry count from `targetRowBytes`, average key/value sizes, and
   `maxEntriesPerRow`.
2. `routeCountPerColumn` from the expected entries and a four-standard-deviation
   distribution margin, so hash variance does not frequently double a local
   bucket table.
3. The outer row-index bucket count from automatic routes plus
   `expectedExplicitRowCount` and `maxLoadFactor`.
4. Sequential spill partitions and row-value shards from the expected byte
   volume, writer count, and memory budget.

Defaults are biased toward the measured low-latency point of about 12,288 keys
per row for small values. The byte target automatically reduces that number for
large values. Typical results with the default 64 MiB target are approximately:

| Average value | Target keys per row |
| ---: | ---: |
| 16 B - 4 KiB | 12,288 |
| 16 KiB | 3,072 |
| 64 KiB | 768 |

For one billion small keys, the default target produces about 84,400 routes per
column and a 131,072-bucket outer index. Underestimating the entry count does not
make the outer index hang: it can grow during Flush. It can, however, make each
local row larger; one compact row is limited to 4 GiB, so the scale estimate
should be in the right order of magnitude.

Use `Database::Layout()` after Open to inspect the persisted choice.

## Write path

Each Put resolves either an automatic or explicit row ID. Batch writes are
stored as compact `(column, resolved row ID)` chunks: the column and row are
written once, followed by key hash, varint lengths, key, and value for each
record. Partitions remain in RAM up to half of `memoryBudgetBytes`; only a
partition that exceeds its share creates a stage file and spills sequentially.
There is no mmap random write per key and no global one-bucket-per-key table.

At Flush, spill partitions are processed in parallel:

- records are grouped by `(column, resolved row ID)`;
- duplicate keys are resolved with last-write-wins semantics;
- an existing row is merged only when a later batch updates it;
- each new immutable row block is built contiguously;
- a row shard appends the complete block under a short per-shard lock;
- the small outer index is published after the block write succeeds.

In `oneShotBuild` mode, each row uses a compact scratch table with one control
byte and one 32-bit entry index per bucket. The selected bucket is reused when
the entry index becomes a persisted record offset, eliminating the normal
temporary dedup table and second hash-table insertion. Header, metadata, index,
keys, and values retain the same contiguous v4 layout and read path.

This keeps the high-volume I/O sequential and avoids the cache-miss-heavy random
index publication that made the former unique path stall.

Spill files are temporary but require real disk space until Flush completes.
With enough memory no stage file is created. If staging spills, budget peak
space for both compact chunks and final row files. For one billion 16-byte keys
and 16-byte values, a conservative spilled-build allowance is roughly
90-120 GiB, depending on column lengths and whether adaptive key encoding is
profitable.

### Interrupted builds

This database is designed as a compiler output. The first Put creates an empty
`build.incomplete` marker. A successful Flush syncs all row files and the index,
then removes the marker. If the process or machine stops during a build, later
Open calls fail immediately with `kCorruption`; delete the output directory and
rebuild it.

There is deliberately no rollback, transaction replay, or full row-file scan.

## Read path

Read-only Open mmaps the outer index and row-value shards. A random lookup does:

1. calculate the automatic row ID, or encode the caller-provided row ID in a
   separate internal namespace;
2. probe the compact outer row index;
3. probe the row's compact local key table;
4. compare the stored key and copy the value.

For the common row below 16 MiB, each local bucket co-locates one control byte
and a 24-bit record offset in four bytes. Sixteen buckets form one aligned
64-byte group, so ARM NEON or x86 SSE2 checks all fingerprints while loading one
cache line. Rows above 16 MiB automatically use 32-bit offsets. The full key is
always verified, so fingerprint collisions preserve exact lookup semantics.

At Flush, every row independently compares raw keys with lossless adaptive
encodings. Repeated 8/16/32-byte prefixes may be stored in a row-local prefix
dictionary, and suffix alphabets of at most 16 or 64 bytes may use 4-bit or
6-bit symbols. Short-key rows stay raw; otherwise encoding is selected only when
the complete row is at least 10% smaller and saves at least eight bytes per key.
High-entropy binary keys also stay raw. Read-only lookup compares raw or encoded
key bytes directly in mmap without allocating a temporary stored-key string.
Values remain uncompressed and are copied unchanged.

## Files

| File | Purpose |
| --- | --- |
| `row_index.lumori` | mmap outer index and persisted routing layout |
| `row_values-NNN.lumorv` | immutable row blocks, sharded for parallel writes |
| `stage-NNN.lumost` | temporary sequential spill files; removed after Flush |
| `build.incomplete` | incomplete compiler-output marker |

The v4 adaptive-key format is intentionally incompatible with databases created
by the former object/unique, v2 24-byte-bucket, or v3 compact-row formats.
Rebuild those outputs in a fresh directory.

## Build and test

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

The test suite covers automatic/explicit row coexistence, duplicate resolution,
updates across Flush calls, concurrent Put, RAM-first and spilled staging,
one-shot lifecycle and readback, 4-bit/6-bit/raw key encodings,
24/32-bit offsets, packed-record varint boundaries, fingerprint collisions,
malformed records, index growth, column isolation, read-only mmap access, and
rejection of interrupted builds.

## Benchmark

```bash
./build/lumodb_bench \
  --entries 1000000000 \
  --value-bytes 16 \
  --threads 16 \
  --row-shards 16 \
  --explicit-rows 0 \
  --memory-gb 128 \
  --reads 10000 \
  --key-prefix-bytes 0 \
  --one-shot \
  --keep
```

The benchmark reports the Put staging rate, parallel row-build/Flush rate,
resulting disk size, and mmap random-read p50/p99 latency separately. Set
`--explicit-rows` above zero to benchmark caller-selected row routing; zero uses
automatic routing. `--key-prefix-bytes` adds a shared prefix to every generated
key so adaptive prefix/alphabet encoding can be measured independently.
`--one-shot` enables the immutable single-pass row-index builder.
