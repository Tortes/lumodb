#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "lumodb/status.h"

namespace LumoDB {

enum class WritePhase {
  kStaging,
  kBuildingRows,
  kFlushing,
};

struct WriteProgress {
  WritePhase phase = WritePhase::kStaging;
  uint64_t completed = 0;
  uint64_t total = 0;
};

using WriteProgressCallback = std::function<void(const WriteProgress&)>;

struct RowStructEntry {
  std::string_view key;
  std::span<const std::byte> flatBufferBytes;
};

struct StructEntry {
  std::string_view key;
  std::span<const std::byte> flatBufferBytes;
};

// Options used when a database is first created. Routing parameters are stored
// in the database and are reused on every subsequent read-write or read-only
// open, so readers never need to repeat these values.
struct DatabaseOptions {
  // Expected number of keys in the largest automatically routed column. These
  // values choose the automatic row count and pre-size the outer row index.
  uint64_t expectedEntryCountPerColumn = 0;
  uint32_t expectedAutomaticColumnCount = 1;
  // Expected total explicit rows across all columns. The index still grows if
  // this estimate is exceeded.
  uint64_t expectedExplicitRowCount = 0;
  // Expected total entries written through explicit-row Put. This is used to
  // size temporary spill partitions, not to choose explicit row IDs.
  uint64_t expectedExplicitEntryCount = 0;
  uint32_t averageKeyBytes = 16;
  uint32_t averageValueBytes = 256;

  // Rows use grouped control-byte hash tables with an upper target near 87.5%.
  // The byte target may reduce maxEntriesPerRow for large values.
  uint64_t targetRowBytes = 64ULL * 1024 * 1024;
  uint32_t maxEntriesPerRow = 12'288;

  // Zero means auto. Up to half of memoryBudgetBytes is used for RAM-first
  // staging; a partition spills sequentially only after its share is full.
  // Write concurrency is also bounded by this budget during Flush().
  uint32_t writerThreadCount = 0;
  uint32_t rowShardCount = 0;
  uint32_t spillPartitionCount = 0;
  uint64_t memoryBudgetBytes = 64ULL * 1024 * 1024 * 1024;
  // Buffered write size after a staging partition has spilled to disk.
  uint32_t stageBufferBytes = 256 * 1024;
  double maxLoadFactor = 0.80;

  // Invoked synchronously at coarse phase boundaries. Keep it cheap.
  WriteProgressCallback writeProgress;
};

struct DatabaseLayout {
  uint64_t expectedEntryCountPerColumn = 0;
  uint64_t expectedExplicitRowCount = 0;
  uint64_t routeCountPerColumn = 0;
  uint32_t targetEntriesPerRow = 0;
  uint32_t rowShardCount = 0;
  uint32_t spillPartitionCount = 0;
  uint64_t rowIndexBucketCount = 0;
};

class Database {
 public:
  Database();
  ~Database();

  Database(const Database&) = delete;
  Database& operator=(const Database&) = delete;

  Database(Database&& other) noexcept;
  Database& operator=(Database&& other) noexcept;

  Status Open(const std::filesystem::path& directory,
              const DatabaseOptions& options = DatabaseOptions());
  Status OpenReadOnly(const std::filesystem::path& directory);
  Status Close();

  // Commits all Put() calls since the previous Flush(). Flush is the visibility
  // and durability boundary; Get() intentionally rejects reads while a batch
  // is uncommitted instead of returning stale data.
  Status Flush();

  // Automatic-row writes and reads.
  Status Put(std::string_view column, std::string_view key, std::span<const std::byte> value);
  Status Put(std::string_view column, std::string_view key, std::string_view value);
  // Automatic-row batch. Records are routed and staged in bounded chunks,
  // with one partition lock per used partition instead of one lock per key.
  // Input order is retained for duplicate keys, so the last value wins.
  Status PutStructs(std::string_view column, std::span<const StructEntry> entries);
  Status Get(std::string_view column, std::string_view key, std::vector<std::byte>& value) const;
  Status Get(std::string_view column, std::string_view key, std::string& value) const;

  // Explicit-row writes and reads. Explicit and automatic rows use separate
  // internal namespaces and may coexist even within the same column. Put is
  // safe to call concurrently. Flush, Close, and Get must not race with Put.
  Status Put(std::string_view column, uint64_t rowId, std::string_view key,
             std::span<const std::byte> value);
  Status Put(std::string_view column, uint64_t rowId, std::string_view key, std::string_view value);
  // Batch form of explicit-row Put. Entries are staged in input order, so the
  // last duplicate key in the batch wins at Flush().
  Status PutRowStructs(std::string_view column, uint64_t rowId,
                       std::span<const RowStructEntry> entries);
  Status Get(std::string_view column, uint64_t rowId, std::string_view key,
             std::vector<std::byte>& value) const;
  Status Get(std::string_view column, uint64_t rowId, std::string_view key,
             std::string& value) const;
  // Compatibility name for direct explicit-row lookup. This probes only the
  // caller-selected row and does not perform automatic row routing.
  Status GetRowStruct(std::string_view column, uint64_t rowId, std::string_view key,
                      std::vector<std::byte>& flatBufferBytes) const;

  [[nodiscard]] bool IsOpen() const;
  [[nodiscard]] bool HasUncommittedWrites() const;
  [[nodiscard]] uint64_t EntryCount() const;
  [[nodiscard]] uint64_t RowCount() const;
  [[nodiscard]] DatabaseLayout Layout() const;

 private:
  class Impl;
  Impl* impl_;
};

}  // namespace LumoDB
