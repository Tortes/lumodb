#include "tdldb/Database.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "File.h"
#include "Hash.h"
#include "StorageFormat.h"

namespace tdldb {
namespace {

constexpr std::string_view kStringColumn = "\x1Ftdldb:string";
constexpr uint64_t kMinimumBucketCount = 16;

uint64_t RoundUpPowerOfTwo(uint64_t value) {
  uint64_t result = kMinimumBucketCount;
  while (result < value) {
    result <<= 1;
  }
  return result;
}

bool MagicEquals(const std::array<char, 8>& lhs, const std::array<char, 8>& rhs) {
  return std::equal(lhs.begin(), lhs.end(), rhs.begin());
}

bool FitsUint32(size_t value) {
  return value <= static_cast<size_t>(std::numeric_limits<uint32_t>::max());
}

std::span<const std::byte> AsBytes(std::string_view value) {
  return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

}  // namespace

class Database::Impl {
 public:
  ~Impl() { static_cast<void>(Close()); }

  Status Open(const std::filesystem::path& directory, const DatabaseOptions& options) {
    if (open_) {
      Status status = Close();
      if (!status) {
        return status;
      }
    }
    if (options.maxLoadFactor <= 0.0 || options.maxLoadFactor >= 0.95) {
      return Status::InvalidArgument("maxLoadFactor must be in (0.0, 0.95)");
    }

    directory_ = directory;
    options_ = options;
    options_.initialBucketCount = RoundUpPowerOfTwo(options_.initialBucketCount);

    Status status = detail::EnsureDirectory(directory_);
    if (!status) {
      return status;
    }

    valuePath_ = directory_ / "values.tdlv";
    indexPath_ = directory_ / "index.tdli";

    status = OpenValueFile();
    if (!status) {
      CloseFiles();
      return status;
    }

    status = OpenIndexFile();
    if (!status) {
      CloseFiles();
      return status;
    }

    open_ = true;
    return Status::Ok();
  }

  Status Close() {
    if (!open_ && !valueFile_.IsValid() && !indexFile_.IsValid()) {
      return Status::Ok();
    }
    Status status = Flush();
    CloseFiles();
    open_ = false;
    return status;
  }

  Status Flush() {
    if (!indexMap_.IsMapped()) {
      return Status::Ok();
    }
    Status status = indexMap_.Sync();
    if (!status) {
      return status;
    }
    status = detail::SyncFile(indexFile_.Get());
    if (!status) {
      return status;
    }
    return detail::SyncFile(valueFile_.Get());
  }

  Status Put(std::string_view key, std::string_view value) {
    return PutStruct(kStringColumn, key, AsBytes(value));
  }

  Status Get(std::string_view key, std::string& value) const {
    std::vector<std::byte> bytes;
    Status status = GetStruct(kStringColumn, key, bytes);
    if (!status) {
      return status;
    }
    value.assign(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    return Status::Ok();
  }

  Status PutStruct(std::string_view column, std::string_view key,
                   std::span<const std::byte> flatBufferBytes) {
    if (!open_) {
      return Status::NotOpen("database is not open");
    }
    if (!FitsUint32(column.size()) || !FitsUint32(key.size())) {
      return Status::InvalidArgument("column and key must fit in uint32 length");
    }

    Status status = EnsureCapacityForInsert();
    if (!status) {
      return status;
    }

    const uint64_t columnHash = detail::HashString(column);
    const uint64_t keyHash = detail::HashString(key);

    BucketLookup lookup;
    status = FindBucket(column, key, columnHash, keyHash, lookup);
    if (!status) {
      return status;
    }

    const uint64_t sequence = IndexHeader()->nextSequence;
    const uint64_t recordOffset = appendOffset_;
    const uint64_t valueOffset =
        recordOffset + sizeof(detail::ValueRecordHeader) + column.size() + key.size();
    const uint64_t recordSize =
        sizeof(detail::ValueRecordHeader) + column.size() + key.size() +
        flatBufferBytes.size();

    detail::ValueRecordHeader recordHeader;
    recordHeader.sequence = sequence;
    recordHeader.columnHash = columnHash;
    recordHeader.keyHash = keyHash;
    recordHeader.columnSize = static_cast<uint32_t>(column.size());
    recordHeader.keySize = static_cast<uint32_t>(key.size());
    recordHeader.valueSize = flatBufferBytes.size();

    status = detail::WriteAllAt(valueFile_.Get(), &recordHeader, sizeof(recordHeader),
                                recordOffset);
    if (!status) {
      return status;
    }
    status = detail::WriteAllAt(valueFile_.Get(), column.data(), column.size(),
                                recordOffset + sizeof(recordHeader));
    if (!status) {
      return status;
    }
    status = detail::WriteAllAt(valueFile_.Get(), key.data(), key.size(),
                                recordOffset + sizeof(recordHeader) + column.size());
    if (!status) {
      return status;
    }
    status = detail::WriteAllAt(valueFile_.Get(), flatBufferBytes.data(),
                                flatBufferBytes.size(), valueOffset);
    if (!status) {
      return status;
    }

    detail::IndexBucket bucket;
    bucket.state = detail::kBucketFilled;
    bucket.columnHash = columnHash;
    bucket.keyHash = keyHash;
    bucket.recordOffset = recordOffset;
    bucket.valueOffset = valueOffset;
    bucket.valueSize = flatBufferBytes.size();
    bucket.recordSize = recordSize;
    bucket.sequence = sequence;

    Buckets()[lookup.index] = bucket;
    if (!lookup.found) {
      ++IndexHeader()->itemCount;
    }
    IndexHeader()->nextSequence = sequence + 1;
    appendOffset_ += recordSize;
    return Status::Ok();
  }

  Status GetStruct(std::string_view column, std::string_view key,
                   std::vector<std::byte>& flatBufferBytes) const {
    if (!open_) {
      return Status::NotOpen("database is not open");
    }

    const uint64_t columnHash = detail::HashString(column);
    const uint64_t keyHash = detail::HashString(key);

    BucketLookup lookup;
    Status status = FindBucket(column, key, columnHash, keyHash, lookup);
    if (!status) {
      return status;
    }
    if (!lookup.found) {
      return Status::NotFound("key not found");
    }

    const detail::IndexBucket& bucket = Buckets()[lookup.index];
    flatBufferBytes.resize(static_cast<size_t>(bucket.valueSize));
    return detail::ReadAllAt(valueFile_.Get(), flatBufferBytes.data(),
                             flatBufferBytes.size(), bucket.valueOffset);
  }

  Status GetMany(std::string_view column, const std::vector<std::string>& keys,
                 std::vector<std::vector<std::byte>>& values) const {
    if (!open_) {
      return Status::NotOpen("database is not open");
    }

    std::vector<std::vector<std::byte>> result;
    result.reserve(keys.size());
    for (const std::string& key : keys) {
      std::vector<std::byte> value;
      Status status = GetStruct(column, key, value);
      if (!status) {
        return status;
      }
      result.push_back(std::move(value));
    }

    values = std::move(result);
    return Status::Ok();
  }

  [[nodiscard]] bool IsOpen() const { return open_; }

  [[nodiscard]] uint64_t EntryCount() const {
    if (!indexMap_.IsMapped()) {
      return 0;
    }
    return IndexHeader()->itemCount;
  }

  [[nodiscard]] uint64_t BucketCount() const {
    if (!indexMap_.IsMapped()) {
      return 0;
    }
    return IndexHeader()->bucketCount;
  }

 private:
  struct BucketLookup {
    uint64_t index = 0;
    bool found = false;
  };

  detail::IndexFileHeader* IndexHeader() {
    return static_cast<detail::IndexFileHeader*>(indexMap_.Data());
  }

  const detail::IndexFileHeader* IndexHeader() const {
    return static_cast<const detail::IndexFileHeader*>(indexMap_.Data());
  }

  detail::IndexBucket* Buckets() {
    auto* base = static_cast<std::byte*>(indexMap_.Data());
    return reinterpret_cast<detail::IndexBucket*>(base + sizeof(detail::IndexFileHeader));
  }

  const detail::IndexBucket* Buckets() const {
    const auto* base = static_cast<const std::byte*>(indexMap_.Data());
    return reinterpret_cast<const detail::IndexBucket*>(
        base + sizeof(detail::IndexFileHeader));
  }

  void CloseFiles() {
    indexMap_.Unmap();
    indexFile_.Reset();
    valueFile_.Reset();
    appendOffset_ = 0;
  }

  Status OpenValueFile() {
    Status status = detail::OpenReadWriteCreate(valuePath_, valueFile_);
    if (!status) {
      return status;
    }

    uint64_t fileSize = 0;
    status = detail::GetFileSize(valueFile_.Get(), fileSize);
    if (!status) {
      return status;
    }

    if (fileSize == 0) {
      detail::ValueFileHeader header;
      status =
          detail::WriteAllAt(valueFile_.Get(), &header, sizeof(header), 0);
      if (!status) {
        return status;
      }
      appendOffset_ = sizeof(header);
      return Status::Ok();
    }

    if (fileSize < sizeof(detail::ValueFileHeader)) {
      return Status::Corruption("value file header is truncated");
    }

    detail::ValueFileHeader header;
    status = detail::ReadAllAt(valueFile_.Get(), &header, sizeof(header), 0);
    if (!status) {
      return status;
    }
    if (!MagicEquals(header.magic, detail::kValueFileMagic) ||
        header.version != detail::kStorageVersion ||
        header.headerSize != sizeof(detail::ValueFileHeader)) {
      return Status::Corruption("value file header is invalid");
    }

    appendOffset_ = fileSize;
    return Status::Ok();
  }

  Status OpenIndexFile() {
    Status status = detail::OpenReadWriteCreate(indexPath_, indexFile_);
    if (!status) {
      return status;
    }

    uint64_t fileSize = 0;
    status = detail::GetFileSize(indexFile_.Get(), fileSize);
    if (!status) {
      return status;
    }

    bool shouldRebuild = fileSize == 0;
    if (!shouldRebuild) {
      status = indexMap_.Map(indexFile_.Get(), fileSize);
      if (!status) {
        return status;
      }
      shouldRebuild = !HasValidIndexLayout(fileSize);
      if (shouldRebuild) {
        indexMap_.Unmap();
      }
    }

    if (shouldRebuild) {
      status = CreateEmptyIndex(options_.initialBucketCount);
      if (!status) {
        return status;
      }
      return RebuildIndexFromValues();
    }

    return Status::Ok();
  }

  bool HasValidIndexLayout(uint64_t fileSize) const {
    if (fileSize < sizeof(detail::IndexFileHeader)) {
      return false;
    }
    const detail::IndexFileHeader* header = IndexHeader();
    if (!MagicEquals(header->magic, detail::kIndexFileMagic) ||
        header->version != detail::kStorageVersion ||
        header->headerSize != sizeof(detail::IndexFileHeader)) {
      return false;
    }
    if (header->bucketCount == 0 ||
        (header->bucketCount & (header->bucketCount - 1)) != 0) {
      return false;
    }
    const uint64_t expectedSize =
        sizeof(detail::IndexFileHeader) +
        header->bucketCount * sizeof(detail::IndexBucket);
    return expectedSize == fileSize && header->itemCount <= header->bucketCount;
  }

  Status CreateEmptyIndex(uint64_t bucketCount) {
    bucketCount = RoundUpPowerOfTwo(bucketCount);
    const uint64_t fileSize =
        sizeof(detail::IndexFileHeader) + bucketCount * sizeof(detail::IndexBucket);

    indexMap_.Unmap();
    Status status = detail::TruncateFile(indexFile_.Get(), fileSize);
    if (!status) {
      return status;
    }
    status = indexMap_.Map(indexFile_.Get(), fileSize);
    if (!status) {
      return status;
    }

    std::memset(indexMap_.Data(), 0, static_cast<size_t>(indexMap_.Size()));
    detail::IndexFileHeader header;
    header.bucketCount = bucketCount;
    *IndexHeader() = header;
    return Status::Ok();
  }

  Status RebuildIndexFromValues() {
    uint64_t fileSize = 0;
    Status status = detail::GetFileSize(valueFile_.Get(), fileSize);
    if (!status) {
      return status;
    }

    uint64_t offset = sizeof(detail::ValueFileHeader);
    uint64_t nextSequence = 1;
    while (offset < fileSize) {
      detail::ValueRecordHeader recordHeader;
      status =
          detail::ReadAllAt(valueFile_.Get(), &recordHeader, sizeof(recordHeader), offset);
      if (!status) {
        return status;
      }
      if (!IsValidRecordHeader(recordHeader)) {
        return Status::Corruption("value record header is invalid");
      }

      const uint64_t recordSize =
          sizeof(detail::ValueRecordHeader) + recordHeader.columnSize +
          recordHeader.keySize + recordHeader.valueSize;
      if (recordSize > fileSize - offset) {
        return Status::Corruption("value record extends beyond file size");
      }

      std::string column;
      std::string key;
      status = ReadRecordColumnKey(offset, recordHeader, column, key);
      if (!status) {
        return status;
      }

      status = EnsureCapacityForInsert();
      if (!status) {
        return status;
      }

      BucketLookup lookup;
      status = FindBucket(column, key, recordHeader.columnHash, recordHeader.keyHash,
                          lookup);
      if (!status) {
        return status;
      }

      detail::IndexBucket bucket;
      bucket.state = detail::kBucketFilled;
      bucket.columnHash = recordHeader.columnHash;
      bucket.keyHash = recordHeader.keyHash;
      bucket.recordOffset = offset;
      bucket.valueOffset =
          offset + sizeof(detail::ValueRecordHeader) + recordHeader.columnSize +
          recordHeader.keySize;
      bucket.valueSize = recordHeader.valueSize;
      bucket.recordSize = recordSize;
      bucket.sequence = recordHeader.sequence;

      Buckets()[lookup.index] = bucket;
      if (!lookup.found) {
        ++IndexHeader()->itemCount;
      }

      nextSequence = std::max(nextSequence, recordHeader.sequence + 1);
      offset += recordSize;
    }

    IndexHeader()->nextSequence = nextSequence;
    appendOffset_ = fileSize;
    return Status::Ok();
  }

  bool IsValidRecordHeader(const detail::ValueRecordHeader& recordHeader) const {
    return recordHeader.magic == detail::kRecordMagic &&
           recordHeader.version == detail::kStorageVersion &&
           recordHeader.headerSize == sizeof(detail::ValueRecordHeader);
  }

  Status ReadRecordColumnKey(uint64_t recordOffset,
                             const detail::ValueRecordHeader& recordHeader,
                             std::string& column, std::string& key) const {
    column.resize(recordHeader.columnSize);
    key.resize(recordHeader.keySize);

    uint64_t offset = recordOffset + sizeof(detail::ValueRecordHeader);
    Status status =
        detail::ReadAllAt(valueFile_.Get(), column.data(), column.size(), offset);
    if (!status) {
      return status;
    }
    offset += column.size();
    return detail::ReadAllAt(valueFile_.Get(), key.data(), key.size(), offset);
  }

  Status EnsureCapacityForInsert() {
    const detail::IndexFileHeader* header = IndexHeader();
    const double loadAfterInsert =
        static_cast<double>(header->itemCount + 1) /
        static_cast<double>(header->bucketCount);
    if (loadAfterInsert <= options_.maxLoadFactor) {
      return Status::Ok();
    }
    return ResizeIndex(header->bucketCount * 2);
  }

  Status ResizeIndex(uint64_t newBucketCount) {
    std::vector<detail::IndexBucket> oldBuckets;
    oldBuckets.reserve(static_cast<size_t>(IndexHeader()->itemCount));
    for (uint64_t index = 0; index < IndexHeader()->bucketCount; ++index) {
      const detail::IndexBucket& bucket = Buckets()[index];
      if (bucket.state == detail::kBucketFilled) {
        oldBuckets.push_back(bucket);
      }
    }

    const uint64_t nextSequence = IndexHeader()->nextSequence;
    Status status = CreateEmptyIndex(newBucketCount);
    if (!status) {
      return status;
    }
    IndexHeader()->nextSequence = nextSequence;

    for (const detail::IndexBucket& bucket : oldBuckets) {
      status = PlaceExistingBucket(bucket);
      if (!status) {
        return status;
      }
      ++IndexHeader()->itemCount;
    }
    return Status::Ok();
  }

  Status PlaceExistingBucket(const detail::IndexBucket& bucket) {
    const uint64_t bucketCount = IndexHeader()->bucketCount;
    const uint64_t mask = bucketCount - 1;
    const uint64_t start = detail::MixHashes(bucket.columnHash, bucket.keyHash) & mask;

    for (uint64_t probe = 0; probe < bucketCount; ++probe) {
      const uint64_t index = (start + probe) & mask;
      if (Buckets()[index].state == detail::kBucketEmpty) {
        Buckets()[index] = bucket;
        return Status::Ok();
      }
    }
    return Status::Corruption("index table is full during resize");
  }

  Status FindBucket(std::string_view column, std::string_view key, uint64_t columnHash,
                    uint64_t keyHash, BucketLookup& lookup) const {
    const uint64_t bucketCount = IndexHeader()->bucketCount;
    const uint64_t mask = bucketCount - 1;
    const uint64_t start = detail::MixHashes(columnHash, keyHash) & mask;

    for (uint64_t probe = 0; probe < bucketCount; ++probe) {
      const uint64_t index = (start + probe) & mask;
      const detail::IndexBucket& bucket = Buckets()[index];
      if (bucket.state == detail::kBucketEmpty) {
        lookup.index = index;
        lookup.found = false;
        return Status::Ok();
      }
      if (bucket.columnHash != columnHash || bucket.keyHash != keyHash) {
        continue;
      }

      bool matches = false;
      Status status = RecordMatches(bucket, column, key, matches);
      if (!status) {
        return status;
      }
      if (matches) {
        lookup.index = index;
        lookup.found = true;
        return Status::Ok();
      }
    }

    return Status::Corruption("index table is full");
  }

  Status RecordMatches(const detail::IndexBucket& bucket, std::string_view column,
                       std::string_view key, bool& matches) const {
    matches = false;

    detail::ValueRecordHeader recordHeader;
    Status status = detail::ReadAllAt(valueFile_.Get(), &recordHeader,
                                      sizeof(recordHeader), bucket.recordOffset);
    if (!status) {
      return status;
    }
    if (!IsValidRecordHeader(recordHeader)) {
      return Status::Corruption("index points to an invalid value record");
    }
    if (recordHeader.columnSize != column.size() || recordHeader.keySize != key.size()) {
      return Status::Ok();
    }

    std::string storedColumn;
    std::string storedKey;
    status = ReadRecordColumnKey(bucket.recordOffset, recordHeader, storedColumn,
                                 storedKey);
    if (!status) {
      return status;
    }

    matches = storedColumn == column && storedKey == key;
    return Status::Ok();
  }

  std::filesystem::path directory_;
  std::filesystem::path valuePath_;
  std::filesystem::path indexPath_;
  DatabaseOptions options_;
  detail::FileDescriptor valueFile_;
  detail::FileDescriptor indexFile_;
  detail::MappedFile indexMap_;
  uint64_t appendOffset_ = 0;
  bool open_ = false;
};

Database::Database() : impl_(new Impl()) {}

Database::~Database() {
  delete impl_;
}

Database::Database(Database&& other) noexcept : impl_(std::exchange(other.impl_, nullptr)) {}

Database& Database::operator=(Database&& other) noexcept {
  if (this != &other) {
    delete impl_;
    impl_ = std::exchange(other.impl_, nullptr);
  }
  return *this;
}

Status Database::Open(const std::filesystem::path& directory,
                      const DatabaseOptions& options) {
  if (impl_ == nullptr) {
    impl_ = new Impl();
  }
  return impl_->Open(directory, options);
}

Status Database::Close() {
  return impl_ == nullptr ? Status::Ok() : impl_->Close();
}

Status Database::Flush() {
  return impl_ == nullptr ? Status::Ok() : impl_->Flush();
}

Status Database::Put(std::string_view key, std::string_view value) {
  return impl_ == nullptr ? Status::NotOpen("database is not open") : impl_->Put(key, value);
}

Status Database::Get(std::string_view key, std::string& value) const {
  return impl_ == nullptr ? Status::NotOpen("database is not open") : impl_->Get(key, value);
}

Status Database::PutStruct(std::string_view column, std::string_view key,
                           std::span<const std::byte> flatBufferBytes) {
  return impl_ == nullptr ? Status::NotOpen("database is not open")
                          : impl_->PutStruct(column, key, flatBufferBytes);
}

Status Database::GetStruct(std::string_view column, std::string_view key,
                           std::vector<std::byte>& flatBufferBytes) const {
  return impl_ == nullptr ? Status::NotOpen("database is not open")
                          : impl_->GetStruct(column, key, flatBufferBytes);
}

Status Database::GetMany(std::string_view column, const std::vector<std::string>& keys,
                         std::vector<std::vector<std::byte>>& values) const {
  return impl_ == nullptr ? Status::NotOpen("database is not open")
                          : impl_->GetMany(column, keys, values);
}

bool Database::IsOpen() const {
  return impl_ != nullptr && impl_->IsOpen();
}

uint64_t Database::EntryCount() const {
  return impl_ == nullptr ? 0 : impl_->EntryCount();
}

uint64_t Database::BucketCount() const {
  return impl_ == nullptr ? 0 : impl_->BucketCount();
}

}  // namespace tdldb
