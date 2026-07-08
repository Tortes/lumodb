#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "lumodb/status.h"

namespace LumoDB {

struct DatabaseOptions {
  uint64_t initialBucketCount = 1ULL << 16;
  uint64_t initialRowBucketCount = 1ULL << 16;
  uint32_t rowShardCount = 8;
  double maxLoadFactor = 0.70;
};

struct RowStructEntry {
  std::string_view key;
  std::span<const std::byte> flatBufferBytes;
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
  Status Close();
  Status Flush();

  Status Put(std::string_view key, std::string_view value);
  Status Get(std::string_view key, std::string& value) const;

  Status PutStruct(std::string_view column, std::string_view key,
                   std::span<const std::byte> flatBufferBytes);
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
