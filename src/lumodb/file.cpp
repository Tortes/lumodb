#include "lumodb/file.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

namespace LumoDB::detail {
namespace {

std::string ErrnoMessage(std::string_view operation) {
  return std::string(operation) + ": " + std::strerror(errno);
}

}  // namespace

FileDescriptor::~FileDescriptor() {
  Reset();
}

FileDescriptor::FileDescriptor(FileDescriptor&& other) noexcept : fd_(other.Release()) {}

FileDescriptor& FileDescriptor::operator=(FileDescriptor&& other) noexcept {
  if (this != &other) {
    Reset(other.Release());
  }
  return *this;
}

int FileDescriptor::Release() {
  int fd = fd_;
  fd_ = -1;
  return fd;
}

void FileDescriptor::Reset(int fd) {
  if (fd_ >= 0) {
    close(fd_);
  }
  fd_ = fd;
}

MappedFile::~MappedFile() {
  Unmap();
}

MappedFile::MappedFile(MappedFile&& other) noexcept
    : data_(other.data_), size_(other.size_) {
  other.data_ = nullptr;
  other.size_ = 0;
}

MappedFile& MappedFile::operator=(MappedFile&& other) noexcept {
  if (this != &other) {
    Unmap();
    data_ = other.data_;
    size_ = other.size_;
    other.data_ = nullptr;
    other.size_ = 0;
  }
  return *this;
}

Status MappedFile::Map(int fd, uint64_t size) {
  Unmap();
  if (size == 0) {
    return Status::InvalidArgument("cannot mmap an empty file");
  }
  void* mapped =
      mmap(nullptr, static_cast<size_t>(size), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (mapped == MAP_FAILED) {
    return Status::IoError(ErrnoMessage("mmap"));
  }
  data_ = mapped;
  size_ = size;
  return Status::Ok();
}

Status MappedFile::Sync() {
  if (data_ == nullptr) {
    return Status::Ok();
  }
  if (msync(data_, static_cast<size_t>(size_), MS_SYNC) != 0) {
    return Status::IoError(ErrnoMessage("msync"));
  }
  return Status::Ok();
}

void MappedFile::Unmap() {
  if (data_ != nullptr) {
    munmap(data_, static_cast<size_t>(size_));
  }
  data_ = nullptr;
  size_ = 0;
}

Status EnsureDirectory(const std::filesystem::path& directory) {
  std::error_code error;
  if (std::filesystem::exists(directory, error)) {
    if (!std::filesystem::is_directory(directory, error)) {
      return Status::InvalidArgument(directory.string() + " is not a directory");
    }
    return Status::Ok();
  }
  if (!std::filesystem::create_directories(directory, error)) {
    return Status::IoError("create_directories " + directory.string() + ": " +
                           error.message());
  }
  return Status::Ok();
}

Status OpenReadWriteCreate(const std::filesystem::path& path, FileDescriptor& fd) {
  int rawFd = open(path.c_str(), O_RDWR | O_CREAT, 0644);
  if (rawFd < 0) {
    return Status::IoError(ErrnoMessage("open " + path.string()));
  }
  fd.Reset(rawFd);
  return Status::Ok();
}

Status GetFileSize(int fd, uint64_t& size) {
  struct stat fileStat {};
  if (fstat(fd, &fileStat) != 0) {
    return Status::IoError(ErrnoMessage("fstat"));
  }
  size = static_cast<uint64_t>(fileStat.st_size);
  return Status::Ok();
}

Status TruncateFile(int fd, uint64_t size) {
  if (ftruncate(fd, static_cast<off_t>(size)) != 0) {
    return Status::IoError(ErrnoMessage("ftruncate"));
  }
  return Status::Ok();
}

Status SyncFile(int fd) {
  if (fsync(fd) != 0) {
    return Status::IoError(ErrnoMessage("fsync"));
  }
  return Status::Ok();
}

Status ReadAllAt(int fd, void* data, size_t size, uint64_t offset) {
  auto* cursor = static_cast<std::byte*>(data);
  size_t remaining = size;
  uint64_t currentOffset = offset;
  while (remaining > 0) {
    ssize_t readSize =
        pread(fd, cursor, remaining, static_cast<off_t>(currentOffset));
    if (readSize < 0) {
      if (errno == EINTR) {
        continue;
      }
      return Status::IoError(ErrnoMessage("pread"));
    }
    if (readSize == 0) {
      return Status::Corruption("unexpected end of file");
    }
    cursor += readSize;
    currentOffset += static_cast<uint64_t>(readSize);
    remaining -= static_cast<size_t>(readSize);
  }
  return Status::Ok();
}

Status WriteAllAt(int fd, const void* data, size_t size, uint64_t offset) {
  const auto* cursor = static_cast<const std::byte*>(data);
  size_t remaining = size;
  uint64_t currentOffset = offset;
  while (remaining > 0) {
    ssize_t writeSize =
        pwrite(fd, cursor, remaining, static_cast<off_t>(currentOffset));
    if (writeSize < 0) {
      if (errno == EINTR) {
        continue;
      }
      return Status::IoError(ErrnoMessage("pwrite"));
    }
    cursor += writeSize;
    currentOffset += static_cast<uint64_t>(writeSize);
    remaining -= static_cast<size_t>(writeSize);
  }
  return Status::Ok();
}

Status WriteVAllAt(int fd, std::span<const WriteSlice> slices, uint64_t offset) {
  std::vector<iovec> vectors;
  vectors.reserve(slices.size());
  for (const WriteSlice& slice : slices) {
    if (slice.size == 0) {
      continue;
    }
    vectors.push_back(
        {.iov_base = const_cast<void*>(slice.data), .iov_len = slice.size});
  }
  if (vectors.empty()) {
    return Status::Ok();
  }
  const long configuredMaxVectors = sysconf(_SC_IOV_MAX);
  const size_t maxVectors =
      configuredMaxVectors > 0 ? static_cast<size_t>(configuredMaxVectors) : 16;
  size_t vectorIndex = 0;
  uint64_t currentOffset = offset;
  while (vectorIndex < vectors.size()) {
    const size_t remainingVectors = vectors.size() - vectorIndex;
    const int vectorCount = static_cast<int>(std::min(remainingVectors, maxVectors));
    const ssize_t writeSize = pwritev(fd, vectors.data() + vectorIndex, vectorCount,
                                      static_cast<off_t>(currentOffset));
    if (writeSize < 0) {
      if (errno == EINTR) {
        continue;
      }
      return Status::IoError(ErrnoMessage("pwritev"));
    }
    if (writeSize == 0) {
      return Status::IoError("pwritev wrote zero bytes");
    }

    size_t consumed = static_cast<size_t>(writeSize);
    currentOffset += static_cast<uint64_t>(consumed);
    while (consumed > 0) {
      iovec& vector = vectors[vectorIndex];
      if (consumed < vector.iov_len) {
        vector.iov_base = static_cast<std::byte*>(vector.iov_base) + consumed;
        vector.iov_len -= consumed;
        consumed = 0;
        break;
      }
      consumed -= vector.iov_len;
      ++vectorIndex;
    }
  }
  return Status::Ok();
}

}  // namespace LumoDB::detail
