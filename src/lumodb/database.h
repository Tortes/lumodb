#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <iosfwd>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "lumodb/status.h"

namespace LumoDB {

enum class WritePhase {
  kValidating,
  kResizingIndex,
  kWritingValues,
  kPublishingIndex,
  kFlushing,
};

struct WriteProgress {
  WritePhase phase = WritePhase::kValidating;
  uint64_t completed = 0;
  uint64_t total = 0;
};

using WriteProgressCallback = std::function<void(const WriteProgress&)>;

struct DatabaseOptions {
  uint64_t initialBucketCount = 1ULL << 16;
  uint64_t initialRowBucketCount = 1ULL << 16;
  uint32_t rowShardCount = 8;
  double maxLoadFactor = 0.80;
  // Invoked synchronously at coarse-grained write boundaries. Keep it cheap.
  WriteProgressCallback writeProgress;
};

struct RowStructEntry {
  std::string_view key;
  std::span<const std::byte> flatBufferBytes;
};

struct StructEntry {
  std::string_view key;
  std::span<const std::byte> flatBufferBytes;
};

struct ColumnStats {
  std::string column;
  uint64_t objectCount = 0;
  uint64_t rowCount = 0;
};

struct DumpValue {
  std::string_view column;
  std::optional<uint64_t> rowId;
  std::string_view key;
  std::span<const std::byte> flatBufferBytes;
};

using DumpDeserializer = std::function<Status(const DumpValue&, std::string& text)>;

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
  Status OpenReadOnly(const std::filesystem::path& directory,
                      const DatabaseOptions& options = DatabaseOptions());
  Status Close();
  Status Flush();

  Status Put(std::string_view key, std::string_view value);
  Status Get(std::string_view key, std::string& value) const;

  Status PutStruct(std::string_view column, std::string_view key,
                   std::span<const std::byte> flatBufferBytes);
  Status PutStructs(std::string_view column, std::span<const StructEntry> entries);
  // Every (column, key) must be new for the lifetime of the database.
  Status PutUniqueStructs(std::string_view column,
                          std::span<const StructEntry> entries);
  Status GetStruct(std::string_view column, std::string_view key,
                   std::vector<std::byte>& flatBufferBytes) const;
  Status PutRowStruct(std::string_view column, uint64_t rowId, std::string_view key,
                      std::span<const std::byte> flatBufferBytes);
  Status PutRowStructs(std::string_view column, uint64_t rowId,
                       std::span<const RowStructEntry> entries);
  Status GetRowStruct(std::string_view column, uint64_t rowId, std::string_view key,
                      std::vector<std::byte>& flatBufferBytes) const;
  Status GetMany(std::string_view column, const std::vector<std::string>& keys,
                 std::vector<std::vector<std::byte>>& values) const;
  Status GetColumnStats(std::vector<ColumnStats>& stats) const;
  Status DumpColumnStats(std::ostream& output) const;
  Status Dump(std::ostream& output) const;
  Status DumpColumn(std::ostream& output, std::string_view column,
                    const DumpDeserializer& deserialize = {}) const;

  [[nodiscard]] bool IsOpen() const;
  [[nodiscard]] uint64_t EntryCount() const;
  [[nodiscard]] uint64_t BucketCount() const;
  [[nodiscard]] uint64_t RowCount() const;
  [[nodiscard]] uint64_t RowBucketCount() const;

 private:
  class Impl;
  Impl* impl_;
};

}  // namespace LumoDB
