# tdldb

tdldb is a lightweight C++20 embedded database optimized for fast point reads of
serialized objects:

```text
Column String + Key String -> mmap hash index -> append-only value file offset
```

The storage layer treats values as opaque bytes. Struct values can be FlatBuffer
payloads and are returned as one contiguous block without field-level parsing.

## Design

- O(1)-style point lookup through a memory-mapped open-addressing hash index.
- Append-only value file for simple, sequential writes.
- `Column + Key` addressing for independently indexed struct categories.
- Latest-write-wins semantics for repeated `Column + Key` writes.
- Collision safety by verifying stored column/key bytes before returning data.
- Index recovery by scanning the value file when the index file is missing or
  has an invalid header.

tdldb intentionally does not implement range scans, deletion, transactions, or
compaction. The current scope is small and direct: write serialized values,
lookup by exact object key, and read the whole object back quickly.

## Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build --output-on-failure
```

## Layout

```text
include/tdldb/   public API
src/             storage implementation
schemas/         example FlatBuffers schema
tests/           smoke and persistence tests
bench/           write/read benchmark CLI
```

## API

```cpp
tdldb::Database db;
tdldb::Status status = db.Open("/tmp/example-db");
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

db.Flush();
db.Close();
```

## Files

- `values.tdlv`: append-only records containing column, key, and value bytes.
- `index.tdli`: mmap open-addressing hash table containing latest offsets.

If `index.tdli` is missing or has an invalid header, tdldb rebuilds it by
scanning `values.tdlv`.

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

tdldb itself intentionally has no FlatBuffers runtime dependency. This keeps the
storage layer independent: callers own serialization, tdldb owns indexing and
whole-object readback.

## Tests

```bash
ctest --test-dir build --output-on-failure
```

The test executable covers string KV, struct byte payloads, overwrite semantics,
database reopen, and index rebuild from the append-only value file.

## Benchmark

```bash
./build/tdldb_bench --dir /tmp/tdldb-bench --sizes 1,5,10 --read-count 100
```

For a quick smoke run:

```bash
./build/tdldb_bench --sizes 0.001 --read-count 10
```
