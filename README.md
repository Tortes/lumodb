# LumoDB

LumoDB is a lightweight C++20 embedded database optimized for fast point reads of
serialized objects:

```text
Column String + Key String -> mmap hash index -> append-only value file offset
Column String + RowId + Key String -> row index -> row block local key index
```

The storage layer treats values as opaque bytes. Struct values can be FlatBuffer
payloads and are returned as one contiguous block without field-level parsing.

## Design

- O(1)-style point lookup through a memory-mapped open-addressing hash index.
- Append-only value file for simple, sequential writes.
- `Column + Key` addressing for independently indexed struct categories.
- `Column + RowId + Key` addressing for row-partitioned struct maps.
- Latest-write-wins semantics for repeated `Column + Key` writes.
- Collision safety by verifying stored column/key bytes before returning data.
- Index recovery by scanning the value file when the index file is missing or
  has an invalid header.

LumoDB intentionally does not implement range scans, deletion, transactions, or
compaction. The current scope is small and direct: write serialized values,
lookup by exact object key, and read the whole object back quickly.

## Row Storage

Row storage is intended for data shaped like:

```text
column -> rowId -> key -> FlatBuffer bytes
```

Each row is serialized as one row block. The block contains a local key index and
all struct payload bytes for that row. Row blocks are appended to a fixed number
of value shard files:

```text
row_index.lumori
row_values-000.lumorv
row_values-001.lumorv
...
row_values-NNN.lumorv
```

The shard count is a physical write parallelism setting, not a business row
count. With the default `rowShardCount = 8`, row storage creates 9 row files:
one row index file plus eight row value files.

## Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build --output-on-failure
```

CMake uses GoogleTest for unit tests. It first searches the local system and
falls back to FetchContent when GTest is not installed.

## Layout

```text
src/lumodb/   public headers and implementation, grouped by feature
schemas/     example FlatBuffers schema
tests/       GoogleTest unit tests by feature
bench/       write/read benchmark CLI
```

## API

```cpp
LumoDB::Database db;
LumoDB::Status status = db.Open("/tmp/example-db");
if (!status) {
  return;
}

db.Put("key", "value");
std::string value;
db.Get("key", value);

std::vector<std::byte> flatBufferBytes = BuildStructA();
db.PutStruct("StructA", "object-key", flatBufferBytes);

std::vector<std::byte> loaded;
db.GetStruct("StructA", "object-key", loaded);

std::vector<std::byte> rowObjectA = BuildStructA();
std::vector<std::byte> rowObjectB = BuildStructA();
std::vector<LumoDB::RowStructEntry> rowEntries = {
    {.key = "object-a", .flatBufferBytes = rowObjectA},
    {.key = "object-b", .flatBufferBytes = rowObjectB},
};
db.PutRowStructs("StructA", 42, rowEntries);
db.GetRowStruct("StructA", 42, "object-a", loaded);

db.Flush();
db.Close();
```

## Files

- `values.lumov`: append-only records containing column, key, and value bytes.
- `index.lumoi`: mmap open-addressing hash table containing latest offsets.
- `row_values-*.lumorv`: append-only row blocks for row-partitioned data.
- `row_index.lumori`: mmap row offset index from `Column + RowId` to a row block.

If `index.lumoi` is missing or has an invalid header, LumoDB rebuilds it by
scanning `values.lumov`. Row storage follows the same model: if `row_index.lumori`
is missing or invalid, LumoDB rebuilds it by scanning `row_values-*.lumorv`.

## FlatBuffers

The example schema is in `schemas/example.fbs`:

```flatbuffers
table MapA {
  key:int;
  value:string;
}

table StructA {
  mapA:[MapA];
  mapB:[MapA];
  used:bool;
}

table StructB {
  sA:[StructA];
  sB:[StructA];
}
```

Generate code with a local FlatBuffers compiler when needed:

```bash
flatc --cpp -o generated schemas/example.fbs
```

LumoDB itself intentionally has no FlatBuffers runtime dependency. This keeps the
storage layer independent: callers own serialization, LumoDB owns indexing and
whole-object readback.

## Tests

```bash
ctest --test-dir build --output-on-failure
```

The test executable covers string KV, struct byte payloads, overwrite semantics,
database reopen, index rebuild from append-only value files, row block lookup,
and parallel row writes.

## Benchmark

```bash
./build/lumodb_bench --dir /tmp/lumodb-bench --sizes 1,5,10 --read-count 100
```

For row-partitioned parallel writes:

```bash
./build/lumodb_bench --mode row-parallel --dir /tmp/lumodb-bench \
  --sizes 1,5,10 --read-count 100 --row-entries 64 --threads 8 --shards 8
```

For a quick smoke run:

```bash
./build/lumodb_bench --sizes 0.001 --read-count 10
./build/lumodb_bench --mode row-parallel --sizes 0.001 --read-count 10
```
