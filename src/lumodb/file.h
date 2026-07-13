#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <span>

#include "lumodb/status.h"

namespace LumoDB::detail {

struct WriteSlice {
  const void* data = nullptr;
  size_t size = 0;
};

class FileDescriptor {
 public:
  FileDescriptor() = default;
  explicit FileDescriptor(int fd) : fd_(fd) {}
  ~FileDescriptor();

  FileDescriptor(const FileDescriptor&) = delete;
  FileDescriptor& operator=(const FileDescriptor&) = delete;

  FileDescriptor(FileDescriptor&& other) noexcept;
  FileDescriptor& operator=(FileDescriptor&& other) noexcept;

  [[nodiscard]] bool IsValid() const { return fd_ >= 0; }
  [[nodiscard]] int Get() const { return fd_; }

  int Release();
  void Reset(int fd = -1);

 private:
  int fd_ = -1;
};

class MappedFile {
 public:
  MappedFile() = default;
  ~MappedFile();

  MappedFile(const MappedFile&) = delete;
  MappedFile& operator=(const MappedFile&) = delete;

  MappedFile(MappedFile&& other) noexcept;
  MappedFile& operator=(MappedFile&& other) noexcept;

  Status Map(int fd, uint64_t size);
  Status MapReadOnly(int fd, uint64_t size);
  Status Sync();
  void Unmap();

  [[nodiscard]] void* Data() const { return data_; }
  [[nodiscard]] uint64_t Size() const { return size_; }
  [[nodiscard]] bool IsMapped() const { return data_ != nullptr; }

 private:
  Status MapWithProtection(int fd, uint64_t size, int protection);

  void* data_ = nullptr;
  uint64_t size_ = 0;
  bool writable_ = false;
};

Status EnsureDirectory(const std::filesystem::path& directory);
Status OpenReadWriteCreate(const std::filesystem::path& path, FileDescriptor& fd);
Status OpenReadOnly(const std::filesystem::path& path, FileDescriptor& fd);
Status GetFileSize(int fd, uint64_t& size);
Status TruncateFile(int fd, uint64_t size);
Status SyncFile(int fd);
Status ReadAllAt(int fd, void* data, size_t size, uint64_t offset);
Status WriteAllAt(int fd, const void* data, size_t size, uint64_t offset);
Status WriteVAllAt(int fd, std::span<const WriteSlice> slices, uint64_t offset);

}  // namespace LumoDB::detail
