#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>

#include "tdldb/Status.h"

namespace tdldb::detail {

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
  Status Sync();
  void Unmap();

  [[nodiscard]] void* Data() const { return data_; }
  [[nodiscard]] uint64_t Size() const { return size_; }
  [[nodiscard]] bool IsMapped() const { return data_ != nullptr; }

 private:
  void* data_ = nullptr;
  uint64_t size_ = 0;
};

Status EnsureDirectory(const std::filesystem::path& directory);
Status OpenReadWriteCreate(const std::filesystem::path& path, FileDescriptor& fd);
Status GetFileSize(int fd, uint64_t& size);
Status TruncateFile(int fd, uint64_t size);
Status SyncFile(int fd);
Status ReadAllAt(int fd, void* data, size_t size, uint64_t offset);
Status WriteAllAt(int fd, const void* data, size_t size, uint64_t offset);

}  // namespace tdldb::detail
