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

// Options used when a database is first created. Routing parameters are stored
// in the database and are reused on every subsequent read-write or read-only
// open, so readers never need to repeat these values.
struct DatabaseOptions {
  // Expected number of keys in the largest column. This is required when a new
  // database is created and is used to choose a stable row count.
  uint64_t expectedEntryCountPerColumn = 0;
  uint32_t expectedColumnCount = 1;
  uint32_t averageKeyBytes = 16;
  uint32_t averageValueBytes = 256;

  // Rows are selected from 75%-full power-of-two local hash tables. The byte
  // target may reduce maxEntriesPerRow for large values.
  uint64_t targetRowBytes = 64ULL * 1024 * 1024;
  uint32_t maxEntriesPerRow = 12'288;

  // Zero means auto. Write concurrency is bounded by both writerThreadCount
  // and memoryBudgetBytes during Flush().
  uint32_t writerThreadCount = 0;
  uint32_t rowShardCount = 0;
  uint32_t spillPartitionCount = 0;
  uint64_t memoryBudgetBytes = 64ULL * 1024 * 1024 * 1024;
  uint32_t stageBufferBytes = 256 * 1024;
  double maxLoadFactor = 0.80;

  // Invoked synchronously at coarse phase boundaries. Keep it cheap.
  WriteProgressCallback writeProgress;
};

struct DatabaseLayout {
  uint64_t expectedEntryCountPerColumn = 0;
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

  // The only write/read data path. Put is safe to call concurrently. Flush,
  // Close, and Get must not race with Put calls from other threads.
  Status Put(std::string_view column, std::string_view key, std::span<const std::byte> value);
  Status Put(std::string_view column, std::string_view key, std::string_view value);
  Status Get(std::string_view column, std::string_view key, std::vector<std::byte>& value) const;
  Status Get(std::string_view column, std::string_view key, std::string& value) const;

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
