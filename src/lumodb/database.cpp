#include "lumodb/database.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cstring>
#include <iomanip>
#include <limits>
#include <map>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

#include "lumodb/file.h"
#include "lumodb/hash.h"
#include "lumodb/storage_format.h"

namespace LumoDB {
namespace {

constexpr std::string_view kStringColumn = "\x1FLumoDB:string";
constexpr uint64_t kMinimumBucketCount = 16;
// Small records are cheaper to copy once than to submit as four iovecs each.
constexpr uint64_t kPackedObjectRecordThreshold = 4 * 1024;
constexpr size_t kPackedObjectWriteBufferSize = 4 * 1024 * 1024;
// Bound per-call metadata even when one API call contains millions of entries.
constexpr size_t kVectoredObjectWriteEntryCount = 256;
constexpr size_t kObjectIndexPublishChunkSize = 64 * 1024;
constexpr size_t kObjectProgressInterval = 1024 * 1024;
constexpr std::string_view kObjectWriteMarkerFile = "object_write.lumotxn";
constexpr std::string_view kObjectWriteMarkerTempFile =
    "object_write.lumotxn.tmp";

uint64_t RoundUpPowerOfTwo(uint64_t value) {
  if (value <= kMinimumBucketCount) {
    return kMinimumBucketCount;
  }
  if (value > (1ULL << 63)) {
    return 0;
  }
  return std::bit_ceil(value);
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

uint64_t ObjectIndexHash(uint64_t columnHash, uint64_t keyHash) {
  const uint64_t hash = detail::MixHashes(columnHash, keyHash);
  return hash == 0 ? 1 : hash;
}

std::string EscapeText(std::string_view value) {
  constexpr char kHexDigits[] = "0123456789abcdef";

  std::string escaped;
  escaped.reserve(value.size() + 2);
  escaped.push_back('"');
  for (const unsigned char character : value) {
    switch (character) {
      case '\\':
        escaped += "\\\\";
        break;
      case '"':
        escaped += "\\\"";
        break;
      case '\n':
        escaped += "\\n";
        break;
      case '\r':
        escaped += "\\r";
        break;
      case '\t':
        escaped += "\\t";
        break;
      default:
        if (character >= 0x20 && character <= 0x7e) {
          escaped.push_back(static_cast<char>(character));
        } else {
          escaped += "\\x";
          escaped.push_back(kHexDigits[character >> 4]);
          escaped.push_back(kHexDigits[character & 0x0f]);
        }
        break;
    }
  }
  escaped.push_back('"');
  return escaped;
}

std::string HexBytes(std::span<const std::byte> bytes) {
  constexpr char kHexDigits[] = "0123456789abcdef";

  std::string text;
  text.reserve(2 + bytes.size() * 2);
  text += "0x";
  for (const std::byte byte : bytes) {
    const unsigned char value = static_cast<unsigned char>(byte);
    text.push_back(kHexDigits[value >> 4]);
    text.push_back(kHexDigits[value & 0x0f]);
  }
  return text;
}

void AppendBytes(std::vector<std::byte>& output, const void* data, size_t size) {
  if (size == 0) {
    return;
  }
  const auto* bytes = static_cast<const std::byte*>(data);
  output.insert(output.end(), bytes, bytes + size);
}

std::filesystem::path RowValuePath(const std::filesystem::path& directory,
                                   uint32_t shardId) {
  std::ostringstream name;
  name << "row_values-" << std::setw(3) << std::setfill('0') << shardId << ".lumorv";
  return directory / name.str();
}

}  // namespace

class Database::Impl {
 public:
  ~Impl() { static_cast<void>(Close()); }

  Status Open(const std::filesystem::path& directory, const DatabaseOptions& options) {
    return OpenInternal(directory, options, false);
  }

  Status OpenReadOnly(const std::filesystem::path& directory,
                      const DatabaseOptions& options) {
    return OpenInternal(directory, options, true);
  }

  Status OpenInternal(const std::filesystem::path& directory,
                      const DatabaseOptions& options, bool readOnly) {
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
    readOnly_ = readOnly;
    objectIndexConsistent_ = true;
    objectWriteMarkerActive_ = false;
    recoveredObjectBucketCount_ = 0;
    rowPendingInsertCount_.store(0, std::memory_order_relaxed);
    options_.initialBucketCount = RoundUpPowerOfTwo(options_.initialBucketCount);
    options_.initialRowBucketCount = RoundUpPowerOfTwo(options_.initialRowBucketCount);
    if (options_.initialBucketCount == 0 ||
        options_.initialRowBucketCount == 0) {
      return Status::InvalidArgument(
          "initial bucket count exceeds the largest supported power of two");
    }
    if (options_.rowShardCount == 0) {
      return Status::InvalidArgument("rowShardCount must be greater than zero");
    }

    Status status = Status::Ok();
    if (readOnly_) {
      std::error_code error;
      if (!std::filesystem::is_directory(directory_, error)) {
        return Status::InvalidArgument("database directory does not exist: " +
                                       directory_.string());
      }
    } else {
      status = detail::EnsureDirectory(directory_);
      if (!status) {
        return status;
      }
    }

    valuePath_ = directory_ / "values.lumov";
    indexPath_ = directory_ / "index.lumoi";
    rowIndexPath_ = directory_ / "row_index.lumori";
    objectWriteMarkerPath_ = directory_ / kObjectWriteMarkerFile;
    objectWriteMarkerTempPath_ = directory_ / kObjectWriteMarkerTempFile;

    status = OpenValueFile();
    if (!status) {
      CloseFiles();
      return status;
    }

    status = RecoverInterruptedObjectWrite();
    if (!status) {
      CloseFiles();
      return status;
    }

    status = OpenIndexFile();
    if (!status) {
      CloseFiles();
      return status;
    }

    status = OpenRowValueFiles();
    if (!status) {
      CloseFiles();
      return status;
    }

    status = OpenRowIndexFile();
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
    readOnly_ = false;
    return status;
  }

  Status Flush() {
    if (readOnly_) {
      return Status::Ok();
    }
    if (!objectIndexConsistent_) {
      return Status::Corruption(
          "object index is incomplete; close and reopen read-write to recover");
    }
    if (!indexMap_.IsMapped()) {
      return objectWriteMarkerActive_
                 ? Status::Corruption(
                       "object write marker exists without a mapped index")
                 : Status::Ok();
    }
    const uint64_t totalSteps =
        static_cast<uint64_t>(rowValueFiles_.size()) + 5;
    uint64_t completedSteps = 0;
    ReportWriteProgress(WritePhase::kFlushing, completedSteps, totalSteps);
    Status status = Status::Ok();
    status = detail::SyncFile(valueFile_.Get());
    if (!status) {
      return status;
    }
    ReportWriteProgress(WritePhase::kFlushing, ++completedSteps, totalSteps);
    for (detail::FileDescriptor& rowValueFile : rowValueFiles_) {
      status = detail::SyncFile(rowValueFile.Get());
      if (!status) {
        return status;
      }
      ReportWriteProgress(WritePhase::kFlushing, ++completedSteps, totalSteps);
    }

    status = rowIndexMap_.Sync();
    if (!status) {
      return status;
    }
    ReportWriteProgress(WritePhase::kFlushing, ++completedSteps, totalSteps);
    if (rowIndexFile_.IsValid()) {
      status = detail::SyncFile(rowIndexFile_.Get());
      if (!status) {
        return status;
      }
    }
    ReportWriteProgress(WritePhase::kFlushing, ++completedSteps, totalSteps);

    status = indexMap_.Sync();
    if (!status) {
      return status;
    }
    ReportWriteProgress(WritePhase::kFlushing, ++completedSteps, totalSteps);
    status = detail::SyncFile(indexFile_.Get());
    if (!status) {
      return status;
    }
    status = FinishObjectWriteTransaction();
    if (!status) {
      return status;
    }
    ReportWriteProgress(WritePhase::kFlushing, totalSteps, totalSteps);
    return Status::Ok();
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
    const std::array<StructEntry, 1> entries = {
        StructEntry{.key = key, .flatBufferBytes = flatBufferBytes},
    };
    return PutStructs(column, entries);
  }

  Status PutStructs(std::string_view column, std::span<const StructEntry> entries) {
    return WriteStructs(column, entries);
  }

  Status PutUniqueStructs(std::string_view column,
                          std::span<const StructEntry> entries) {
    Status status = ValidateStructWrite(column, entries, false);
    if (!status || entries.empty()) {
      return status;
    }

    status = BeginObjectWriteTransaction();
    if (!status) {
      return status;
    }
    status = EnsureCapacityForInsert(entries.size());
    if (!status) {
      objectIndexConsistent_ = false;
      return status;
    }

    const uint64_t columnHash = detail::HashString(column);
    const uint64_t firstRecordOffset = appendOffset_;
    uint64_t nextAppendOffset = appendOffset_;
    status = WriteRecords(column, entries, nextAppendOffset);
    if (!status) {
      const Status rollbackStatus =
          detail::TruncateFile(valueFile_.Get(), firstRecordOffset);
      if (!rollbackStatus) {
        objectIndexConsistent_ = false;
        return rollbackStatus;
      }
      appendOffset_ = firstRecordOffset;
      return status;
    }

    // The transaction marker keeps this new tail uncommitted until Flush has
    // persisted both values and index contents.
    appendOffset_ = nextAppendOffset;

    uint64_t placedCount = 0;
    uint64_t recordOffset = firstRecordOffset;
    pendingUniqueBuckets_.clear();
    pendingUniqueBuckets_.reserve(
        std::min(entries.size(), kObjectIndexPublishChunkSize));
    size_t firstEntry = 0;
    ReportWriteProgress(WritePhase::kPublishingIndex, 0, entries.size());
    while (firstEntry < entries.size()) {
      const size_t endEntry =
          std::min(entries.size(), firstEntry + kObjectIndexPublishChunkSize);
      pendingUniqueBuckets_.clear();
      for (size_t index = firstEntry; index < endEntry; ++index) {
        const StructEntry& entry = entries[index];
        pendingUniqueBuckets_.push_back(
            {.hash = ObjectIndexHash(columnHash, detail::HashString(entry.key)),
             .recordOffset = recordOffset});
        recordOffset += sizeof(detail::ValueRecordHeader) + column.size() +
                        entry.key.size() + entry.flatBufferBytes.size();
      }

      const detail::IndexBucket* pendingBuckets = pendingUniqueBuckets_.data();
#if defined(__GNUC__) || defined(__clang__)
      detail::IndexBucket* indexBuckets = Buckets();
      const uint64_t bucketMask = IndexHeader()->bucketCount - 1;
#endif
      for (size_t index = 0; index < pendingUniqueBuckets_.size(); ++index) {
#if defined(__GNUC__) || defined(__clang__)
        constexpr size_t kPrefetchDistance = 64;
        if (index + kPrefetchDistance < pendingUniqueBuckets_.size()) {
          const uint64_t futureIndex =
              pendingBuckets[index + kPrefetchDistance].hash & bucketMask;
          __builtin_prefetch(indexBuckets + futureIndex, 1, 0);
        }
#endif
        status = PlaceNewBucket(pendingBuckets[index]);
        if (!status) {
          objectIndexConsistent_ = false;
          return status;
        }
        ++placedCount;
      }
      firstEntry = endEntry;
      ReportWriteProgress(WritePhase::kPublishingIndex, firstEntry,
                          entries.size());
    }
    IndexHeader()->itemCount += placedCount;
    objectIndexConsistent_ = true;
    pendingUniqueBuckets_.clear();
    return Status::Ok();
  }

  Status WriteRecords(std::string_view column,
                      std::span<const StructEntry> entries,
                      uint64_t& nextAppendOffset) {
    uint64_t writeOffset = appendOffset_;
    std::array<detail::ValueRecordHeader,
               kVectoredObjectWriteEntryCount> headers;
    std::array<detail::WriteSlice,
               kVectoredObjectWriteEntryCount * 4> recordSlices;
    size_t firstEntry = 0;
    ReportWriteProgress(WritePhase::kWritingValues, 0, entries.size());
    while (firstEntry < entries.size()) {
      const StructEntry& first = entries[firstEntry];
      const uint64_t firstRecordSize =
          sizeof(detail::ValueRecordHeader) + column.size() + first.key.size() +
          first.flatBufferBytes.size();

      if (firstRecordSize <= kPackedObjectRecordThreshold) {
        size_t endEntry = firstEntry;
        size_t chunkSize = 0;
        while (endEntry < entries.size()) {
          const StructEntry& entry = entries[endEntry];
          const size_t recordSize = sizeof(detail::ValueRecordHeader) +
                                    column.size() + entry.key.size() +
                                    entry.flatBufferBytes.size();
          if (recordSize > kPackedObjectRecordThreshold ||
              recordSize > kPackedObjectWriteBufferSize - chunkSize) {
            break;
          }
          chunkSize += recordSize;
          ++endEntry;
        }

        objectWriteBuffer_.resize(chunkSize);
        std::byte* cursor = objectWriteBuffer_.data();
        for (size_t index = firstEntry; index < endEntry; ++index) {
          const StructEntry& entry = entries[index];
          detail::ValueRecordHeader header;
          header.columnSize = static_cast<uint32_t>(column.size());
          header.keySize = static_cast<uint32_t>(entry.key.size());
          header.valueSize = entry.flatBufferBytes.size();

          std::memcpy(cursor, &header, sizeof(header));
          cursor += sizeof(header);
          if (!column.empty()) {
            std::memcpy(cursor, column.data(), column.size());
            cursor += column.size();
          }
          if (!entry.key.empty()) {
            std::memcpy(cursor, entry.key.data(), entry.key.size());
            cursor += entry.key.size();
          }
          if (!entry.flatBufferBytes.empty()) {
            std::memcpy(cursor, entry.flatBufferBytes.data(),
                        entry.flatBufferBytes.size());
            cursor += entry.flatBufferBytes.size();
          }
        }

        Status status = detail::WriteAllAt(
            valueFile_.Get(), objectWriteBuffer_.data(),
            objectWriteBuffer_.size(), writeOffset);
        if (!status) {
          return status;
        }
        writeOffset += chunkSize;
        firstEntry = endEntry;
        ReportWriteProgress(WritePhase::kWritingValues, firstEntry,
                            entries.size());
        continue;
      }

      size_t endEntry = firstEntry;
      size_t headerCount = 0;
      size_t sliceCount = 0;
      uint64_t chunkSize = 0;
      while (endEntry < entries.size() &&
             headerCount < kVectoredObjectWriteEntryCount) {
        const StructEntry& entry = entries[endEntry];
        const uint64_t recordSize = sizeof(detail::ValueRecordHeader) +
                                    column.size() + entry.key.size() +
                                    entry.flatBufferBytes.size();
        if (recordSize <= kPackedObjectRecordThreshold) {
          break;
        }

        detail::ValueRecordHeader& header = headers[headerCount++];
        header.columnSize = static_cast<uint32_t>(column.size());
        header.keySize = static_cast<uint32_t>(entry.key.size());
        header.valueSize = entry.flatBufferBytes.size();
        recordSlices[sliceCount++] =
            {.data = &header, .size = sizeof(header)};
        recordSlices[sliceCount++] =
            {.data = column.data(), .size = column.size()};
        recordSlices[sliceCount++] =
            {.data = entry.key.data(), .size = entry.key.size()};
        recordSlices[sliceCount++] = {.data = entry.flatBufferBytes.data(),
                                      .size = entry.flatBufferBytes.size()};
        chunkSize += recordSize;
        ++endEntry;
      }

      Status status = detail::WriteVAllAt(
          valueFile_.Get(),
          std::span<const detail::WriteSlice>(recordSlices.data(), sliceCount),
          writeOffset);
      if (!status) {
        return status;
      }
      writeOffset += chunkSize;
      firstEntry = endEntry;
      ReportWriteProgress(WritePhase::kWritingValues, firstEntry,
                          entries.size());
    }
    nextAppendOffset = writeOffset;
    return Status::Ok();
  }

  Status WriteStructs(std::string_view column,
                      std::span<const StructEntry> entries) {
    Status status = ValidateStructWrite(column, entries, true);
    if (!status || entries.empty()) {
      return status;
    }

    struct PendingRecord {
      std::string_view key;
      uint64_t keyHash = 0;
      uint64_t recordOffset = 0;
    };

    const uint64_t columnHash = detail::HashString(column);
    uint64_t nextAppendOffset = appendOffset_;
    std::vector<PendingRecord> pending;
    pending.reserve(entries.size());

    for (const StructEntry& entry : entries) {
      PendingRecord& record = pending.emplace_back();
      record.key = entry.key;
      record.recordOffset = nextAppendOffset;
      record.keyHash = detail::HashString(entry.key);
      const uint64_t recordSize = sizeof(detail::ValueRecordHeader) + column.size() +
                                  entry.key.size() + entry.flatBufferBytes.size();
      nextAppendOffset += recordSize;
    }

    status = WriteRecords(column, entries, nextAppendOffset);
    if (!status) {
      return status;
    }

    for (const PendingRecord& record : pending) {
      BucketLookup lookup;
      status = FindBucket(column, record.key, columnHash, record.keyHash, lookup);
      if (!status) {
        return status;
      }

      detail::IndexBucket bucket;
      bucket.hash = ObjectIndexHash(columnHash, record.keyHash);
      bucket.recordOffset = record.recordOffset;
      Buckets()[lookup.index] = bucket;
      if (!lookup.found) {
        MarkObjectBucketOccupied(lookup.index);
        ++IndexHeader()->itemCount;
      }
    }
    appendOffset_ = nextAppendOffset;
    return Status::Ok();
  }

  Status ValidateStructWrite(std::string_view column,
                             std::span<const StructEntry> entries,
                             bool ensureCapacity) {
    if (!open_) {
      return Status::NotOpen("database is not open");
    }
    if (readOnly_) {
      return Status::InvalidArgument("database is read-only");
    }
    if (!objectIndexConsistent_) {
      return Status::Corruption(
          "object index is incomplete; close and reopen read-write to recover");
    }
    if (!FitsUint32(column.size())) {
      return Status::InvalidArgument("column must fit in uint32 length");
    }
    if (entries.empty()) {
      return Status::Ok();
    }
    ReportWriteProgress(WritePhase::kValidating, 0, entries.size());
    for (size_t index = 0; index < entries.size(); ++index) {
      const StructEntry& entry = entries[index];
      if (!FitsUint32(entry.key.size())) {
        return Status::InvalidArgument("key must fit in uint32 length");
      }
      const size_t completed = index + 1;
      if (completed % kObjectProgressInterval == 0) {
        ReportWriteProgress(WritePhase::kValidating, completed,
                            entries.size());
      }
    }
    ReportWriteProgress(WritePhase::kValidating, entries.size(), entries.size());
    return ensureCapacity ? EnsureCapacityForInsert(entries.size())
                          : Status::Ok();
  }

  Status GetStruct(std::string_view column, std::string_view key,
                   std::vector<std::byte>& flatBufferBytes) const {
    if (!open_) {
      return Status::NotOpen("database is not open");
    }
    if (!objectIndexConsistent_) {
      return Status::Corruption(
          "object index is incomplete; close and reopen read-write to recover");
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
    detail::ValueRecordHeader recordHeader;
    status = ReadValueAt(&recordHeader, sizeof(recordHeader), bucket.recordOffset);
    if (!status) {
      return status;
    }
    if (!IsValidRecordHeader(recordHeader)) {
      return Status::Corruption("index points to an invalid value record");
    }

    const uint64_t valueOffset =
        bucket.recordOffset + sizeof(detail::ValueRecordHeader) +
        recordHeader.columnSize + recordHeader.keySize;
    flatBufferBytes.resize(static_cast<size_t>(recordHeader.valueSize));
    return ReadValueAt(flatBufferBytes.data(), flatBufferBytes.size(), valueOffset);
  }

  Status PutRowStruct(std::string_view column, uint64_t rowId, std::string_view key,
                      std::span<const std::byte> flatBufferBytes) {
    if (!open_) {
      return Status::NotOpen("database is not open");
    }
    if (readOnly_) {
      return Status::InvalidArgument("database is read-only");
    }
    if (!FitsUint32(column.size()) || !FitsUint32(key.size())) {
      return Status::InvalidArgument("column and key must fit in uint32 length");
    }

    const uint64_t columnHash = detail::HashString(column);
    std::vector<std::string> keys;
    std::vector<std::vector<std::byte>> values;
    bool found = false;
    Status status = LoadRowEntries(column, rowId, columnHash, found, keys, values);
    if (!status) {
      return status;
    }

    bool replaced = false;
    for (size_t index = 0; index < keys.size(); ++index) {
      if (keys[index] == key) {
        values[index].assign(flatBufferBytes.begin(), flatBufferBytes.end());
        replaced = true;
        break;
      }
    }
    if (!replaced) {
      keys.emplace_back(key);
      values.emplace_back(flatBufferBytes.begin(), flatBufferBytes.end());
    }

    std::vector<RowStructEntry> entries;
    entries.reserve(keys.size());
    for (size_t index = 0; index < keys.size(); ++index) {
      entries.push_back({.key = keys[index], .flatBufferBytes = values[index]});
    }

    return WriteRowStructs(column, rowId, entries, !found);
  }

  Status PutRowStructs(std::string_view column, uint64_t rowId,
                       std::span<const RowStructEntry> entries) {
    return WriteRowStructs(column, rowId, entries, true);
  }

  Status WriteRowStructs(std::string_view column, uint64_t rowId,
                         std::span<const RowStructEntry> entries,
                         bool reserveInsertSlot) {
    if (!open_) {
      return Status::NotOpen("database is not open");
    }
    if (readOnly_) {
      return Status::InvalidArgument("database is read-only");
    }
    if (!FitsUint32(column.size())) {
      return Status::InvalidArgument("column must fit in uint32 length");
    }
    if (entries.empty()) {
      return Status::InvalidArgument("row entries must not be empty");
    }

    const uint64_t columnHash = detail::HashString(column);
    bool reserved = false;
    Status status = Status::Ok();
    if (reserveInsertSlot) {
      status = ReserveRowIndexInsertSlot();
      if (!status) {
        return status;
      }
      reserved = true;
    }

    const uint64_t sequence = NextRowSequence();
    RowBlockLayout layout;
    status = BuildRowBlockLayout(column, rowId, entries, sequence, columnHash, layout);
    if (!status) {
      if (reserved) {
        ReleaseRowIndexInsertSlot();
      }
      return status;
    }

    const uint32_t shardId = ShardForRow(columnHash, rowId);
    const uint64_t blockOffset = rowAppendOffsets_[shardId]->fetch_add(
        layout.header.blockSize, std::memory_order_relaxed);
    std::vector<detail::WriteSlice> blockSlices;
    blockSlices.reserve(4 + layout.valueEntries.size());
    blockSlices.push_back({.data = &layout.header, .size = sizeof(layout.header)});
    blockSlices.push_back({.data = layout.keyBuckets.data(),
                           .size = layout.keyBuckets.size() * sizeof(detail::RowKeyBucket)});
    blockSlices.push_back({.data = column.data(), .size = column.size()});
    blockSlices.push_back({.data = layout.keyBytes.data(), .size = layout.keyBytes.size()});
    for (const RowStructEntry* entry : layout.valueEntries) {
      blockSlices.push_back(
          {.data = entry->flatBufferBytes.data(), .size = entry->flatBufferBytes.size()});
    }
    status = detail::WriteVAllAt(rowValueFiles_[shardId].Get(), blockSlices, blockOffset);
    if (!status) {
      if (reserved) {
        ReleaseRowIndexInsertSlot();
      }
      return status;
    }

    RowBucketData bucket;
    bucket.columnHash = columnHash;
    bucket.rowId = rowId;
    bucket.shardId = shardId;
    bucket.blockOffset = blockOffset;
    bucket.blockSize = layout.header.blockSize;
    bucket.sequence = sequence;

    bool inserted = false;
    status = InsertOrUpdateRowBucket(column, bucket, inserted);
    if (!status) {
      if (reserved) {
        ReleaseRowIndexInsertSlot();
      }
      return status;
    }
    if (inserted) {
      IncrementRowItemCount();
    }
    if (reserved) {
      ReleaseRowIndexInsertSlot();
    }
    return Status::Ok();
  }

  Status GetRowStruct(std::string_view column, uint64_t rowId, std::string_view key,
                      std::vector<std::byte>& flatBufferBytes) const {
    if (!open_) {
      return Status::NotOpen("database is not open");
    }

    const uint64_t columnHash = detail::HashString(column);
    RowBucketLookup lookup;
    Status status = FindRowBucket(column, rowId, columnHash, lookup);
    if (!status) {
      return status;
    }
    if (!lookup.found) {
      return Status::NotFound("row not found");
    }
    return FindValueInRowBlockAt(lookup.bucket, lookup.blockHeader, key,
                                 flatBufferBytes);
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

  Status GetColumnStats(std::vector<ColumnStats>& stats) const {
    if (!open_) {
      return Status::NotOpen("database is not open");
    }

    std::map<std::string, ColumnStats> columns;
    for (uint64_t index = 0; index < IndexHeader()->bucketCount; ++index) {
      const detail::IndexBucket& bucket = Buckets()[index];
      if (bucket.hash == 0) {
        continue;
      }

      detail::ValueRecordHeader recordHeader;
      Status status = ReadValueAt(&recordHeader, sizeof(recordHeader),
                                  bucket.recordOffset);
      if (!status) {
        return status;
      }
      if (!IsValidRecordHeader(recordHeader)) {
        return Status::Corruption("object index points to an invalid value record");
      }

      std::string column;
      std::string key;
      status = ReadRecordColumnKey(bucket.recordOffset, recordHeader, column, key);
      if (!status) {
        return status;
      }
      ColumnStats& columnStats = columns[column];
      columnStats.column = column;
      ++columnStats.objectCount;
    }

    for (uint64_t index = 0; index < RowIndexHeader()->bucketCount; ++index) {
      detail::RowIndexBucket bucket;
      Status status = LoadFilledRowBucketSnapshotIfPresent(index, bucket);
      if (!status) {
        return status;
      }
      if (bucket.state != detail::kRowBucketFilled) {
        continue;
      }
      if (bucket.shardId >= rowValueFiles_.size()) {
        return Status::Corruption("row index points to an invalid shard");
      }

      detail::RowBlockHeader blockHeader;
      status = ReadRowValueAt(bucket.shardId, &blockHeader, sizeof(blockHeader),
                              bucket.blockOffset);
      if (!status) {
        return status;
      }
      if (!IsValidRowBlockHeader(blockHeader) || blockHeader.blockSize != bucket.blockSize ||
          blockHeader.columnHash != bucket.columnHash || blockHeader.rowId != bucket.rowId) {
        return Status::Corruption("row index points to an invalid row block");
      }

      std::string column;
      status = ReadRowBlockColumn(bucket.shardId, bucket.blockOffset, blockHeader, column);
      if (!status) {
        return status;
      }
      ColumnStats& columnStats = columns[column];
      columnStats.column = column;
      columnStats.objectCount += blockHeader.itemCount;
      ++columnStats.rowCount;
    }

    stats.clear();
    stats.reserve(columns.size());
    for (auto& [column, columnStats] : columns) {
      stats.push_back(std::move(columnStats));
    }
    return Status::Ok();
  }

  Status DumpColumnStats(std::ostream& output) const {
    std::vector<ColumnStats> stats;
    Status status = GetColumnStats(stats);
    if (!status) {
      return status;
    }

    output << "LumoDB column statistics\n";
    for (const ColumnStats& columnStats : stats) {
      output << "column=" << EscapeText(columnStats.column)
             << " objects=" << columnStats.objectCount
             << " rows=" << columnStats.rowCount << '\n';
    }
    return Status::Ok();
  }

  Status Dump(std::ostream& output) const {
    return DumpInternal(output, std::nullopt, {});
  }

  Status DumpColumn(std::ostream& output, std::string_view column,
                    const DumpDeserializer& deserialize) const {
    return DumpInternal(output, std::optional<std::string_view>(column), deserialize);
  }

  Status DumpInternal(std::ostream& output,
                      std::optional<std::string_view> columnFilter,
                      const DumpDeserializer& deserialize) const {
    if (!open_) {
      return Status::NotOpen("database is not open");
    }

    if (columnFilter.has_value()) {
      output << "LumoDB column dump column=" << EscapeText(*columnFilter) << '\n';
    } else {
      output << "LumoDB dump\n";
    }
    if (columnFilter.has_value()) {
      output << "object_index: buckets=" << BucketCount() << '\n';
    } else {
      output << "object_index: entries=" << EntryCount()
             << " buckets=" << BucketCount() << '\n';
    }
    output << "objects:\n";
    for (uint64_t index = 0; index < IndexHeader()->bucketCount; ++index) {
      const detail::IndexBucket& bucket = Buckets()[index];
      if (bucket.hash == 0) {
        continue;
      }

      detail::ValueRecordHeader recordHeader;
      Status status = ReadValueAt(&recordHeader, sizeof(recordHeader),
                                  bucket.recordOffset);
      if (!status) {
        return status;
      }
      if (!IsValidRecordHeader(recordHeader)) {
        return Status::Corruption("object index points to an invalid value record");
      }

      std::string column;
      std::string key;
      status = ReadRecordColumnKey(bucket.recordOffset, recordHeader, column, key);
      if (!status) {
        return status;
      }
      if (columnFilter.has_value() && column != *columnFilter) {
        continue;
      }
      const uint64_t valueOffset =
          bucket.recordOffset + sizeof(detail::ValueRecordHeader) +
          recordHeader.columnSize + recordHeader.keySize;
      const uint64_t recordSize = sizeof(detail::ValueRecordHeader) +
                                  recordHeader.columnSize + recordHeader.keySize +
                                  recordHeader.valueSize;
      std::vector<std::byte> value(static_cast<size_t>(recordHeader.valueSize));
      status = ReadValueAt(value.data(), value.size(), valueOffset);
      if (!status) {
        return status;
      }

      std::string deserialized;
      if (deserialize) {
        status = deserialize(
            {.column = column,
             .rowId = std::nullopt,
             .key = key,
             .flatBufferBytes = std::span<const std::byte>(value)},
            deserialized);
        if (!status) {
          return status;
        }
      }

      output << "  object bucket=" << index
             << " column=" << EscapeText(column)
             << " key=" << EscapeText(key)
             << " column_hash=" << detail::HashString(column)
             << " key_hash=" << detail::HashString(key)
             << " index_hash=" << bucket.hash
             << " record_offset=" << bucket.recordOffset
             << " value_offset=" << valueOffset
             << " value_size=" << recordHeader.valueSize
             << " record_size=" << recordSize
             << " value_hex=" << HexBytes(value);
      if (deserialize) {
        output << " deserialized=" << EscapeText(deserialized);
      }
      output << '\n';
    }

    if (columnFilter.has_value()) {
      output << "row_index: buckets=" << RowBucketCount()
             << " shards=" << rowValueFiles_.size() << '\n';
    } else {
      output << "row_index: rows=" << RowCount()
             << " buckets=" << RowBucketCount()
             << " shards=" << rowValueFiles_.size()
             << " next_sequence="
             << AtomicLoad(RowIndexHeader()->nextSequence, std::memory_order_acquire)
             << '\n';
    }
    output << "rows:\n";
    for (uint64_t index = 0; index < RowIndexHeader()->bucketCount; ++index) {
      detail::RowIndexBucket bucket;
      Status status = LoadFilledRowBucketSnapshotIfPresent(index, bucket);
      if (!status) {
        return status;
      }
      if (bucket.state != detail::kRowBucketFilled) {
        continue;
      }

      std::vector<std::byte> block;
      status = LoadRowBlock(bucket, block);
      if (!status) {
        return status;
      }
      if (block.size() < sizeof(detail::RowBlockHeader)) {
        return Status::Corruption("row block is truncated");
      }
      const auto* blockHeader =
          reinterpret_cast<const detail::RowBlockHeader*>(block.data());
      if (!IsValidRowBlockHeader(*blockHeader) || blockHeader->blockSize != block.size() ||
          blockHeader->columnHash != bucket.columnHash || blockHeader->rowId != bucket.rowId) {
        return Status::Corruption("row index points to an invalid row block");
      }

      const uint64_t columnOffset =
          sizeof(detail::RowBlockHeader) +
          static_cast<uint64_t>(blockHeader->bucketCount) * sizeof(detail::RowKeyBucket);
      const auto* columnData = reinterpret_cast<const char*>(block.data() + columnOffset);
      const std::string column(columnData, blockHeader->columnSize);
      if (columnFilter.has_value() && column != *columnFilter) {
        continue;
      }
      std::vector<std::string> keys;
      std::vector<std::vector<std::byte>> values;
      status = DecodeRowBlockEntries(block, column, bucket.rowId, keys, values);
      if (!status) {
        return status;
      }

      output << "  row bucket=" << index
             << " column=" << EscapeText(column)
             << " row_id=" << bucket.rowId
             << " sequence=" << bucket.sequence
             << " column_hash=" << bucket.columnHash
             << " shard=" << bucket.shardId
             << " block_offset=" << bucket.blockOffset
             << " block_size=" << bucket.blockSize
             << " object_count=" << blockHeader->itemCount << '\n';
      for (size_t entry = 0; entry < keys.size(); ++entry) {
        std::string deserialized;
        if (deserialize) {
          status = deserialize(
              {.column = column,
               .rowId = bucket.rowId,
               .key = keys[entry],
               .flatBufferBytes = std::span<const std::byte>(values[entry])},
              deserialized);
          if (!status) {
            return status;
          }
        }
        output << "    key=" << EscapeText(keys[entry])
               << " value_size=" << values[entry].size()
               << " value_hex=" << HexBytes(values[entry]);
        if (deserialize) {
          output << " deserialized=" << EscapeText(deserialized);
        }
        output << '\n';
      }
    }

    output << "column_statistics:\n";
    std::vector<ColumnStats> stats;
    Status status = GetColumnStats(stats);
    if (!status) {
      return status;
    }
    for (const ColumnStats& columnStats : stats) {
      if (columnFilter.has_value() && columnStats.column != *columnFilter) {
        continue;
      }
      output << "  column=" << EscapeText(columnStats.column)
             << " objects=" << columnStats.objectCount
             << " rows=" << columnStats.rowCount << '\n';
    }
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

  [[nodiscard]] uint64_t RowCount() const {
    if (!rowIndexMap_.IsMapped()) {
      return 0;
    }
    return LoadRowItemCount();
  }

  [[nodiscard]] uint64_t RowBucketCount() const {
    if (!rowIndexMap_.IsMapped()) {
      return 0;
    }
    return RowIndexHeader()->bucketCount;
  }

 private:
  struct BucketLookup {
    uint64_t index = 0;
    bool found = false;
  };

  struct RowBucketLookup {
    uint64_t index = 0;
    bool found = false;
    detail::RowIndexBucket bucket = {};
    detail::RowBlockHeader blockHeader = {};
  };

  struct RowBucketData {
    uint64_t columnHash = 0;
    uint64_t rowId = 0;
    uint32_t shardId = 0;
    uint64_t blockOffset = 0;
    uint64_t blockSize = 0;
    uint64_t sequence = 0;
  };

  struct RowBlockLayout {
    detail::RowBlockHeader header;
    std::vector<detail::RowKeyBucket> keyBuckets;
    std::vector<std::byte> keyBytes;
    std::vector<const RowStructEntry*> valueEntries;
  };

  enum class RowBucketSnapshotState {
    kEmpty,
    kFilled,
    kWriting,
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

  detail::RowIndexFileHeader* RowIndexHeader() {
    return static_cast<detail::RowIndexFileHeader*>(rowIndexMap_.Data());
  }

  const detail::RowIndexFileHeader* RowIndexHeader() const {
    return static_cast<const detail::RowIndexFileHeader*>(rowIndexMap_.Data());
  }

  detail::RowIndexBucket* RowBuckets() {
    auto* base = static_cast<std::byte*>(rowIndexMap_.Data());
    return reinterpret_cast<detail::RowIndexBucket*>(
        base + sizeof(detail::RowIndexFileHeader));
  }

  const detail::RowIndexBucket* RowBuckets() const {
    const auto* base = static_cast<const std::byte*>(rowIndexMap_.Data());
    return reinterpret_cast<const detail::RowIndexBucket*>(
        base + sizeof(detail::RowIndexFileHeader));
  }

  Status ReadValueAt(void* data, size_t size, uint64_t offset) const {
    if (!valueMap_.IsMapped()) {
      return detail::ReadAllAt(valueFile_.Get(), data, size, offset);
    }
    if (offset > valueMap_.Size() || size > valueMap_.Size() - offset) {
      return Status::Corruption("value read extends beyond file size");
    }
    if (size != 0) {
      const auto* source = static_cast<const std::byte*>(valueMap_.Data()) + offset;
      std::memcpy(data, source, size);
    }
    return Status::Ok();
  }

  Status ReadRowValueAt(uint32_t shardId, void* data, size_t size,
                        uint64_t offset) const {
    if (shardId >= rowValueFiles_.size()) {
      return Status::Corruption("row index points to an invalid shard");
    }
    if (shardId >= rowValueMaps_.size() || !rowValueMaps_[shardId].IsMapped()) {
      return detail::ReadAllAt(rowValueFiles_[shardId].Get(), data, size, offset);
    }
    const detail::MappedFile& valueMap = rowValueMaps_[shardId];
    if (offset > valueMap.Size() || size > valueMap.Size() - offset) {
      return Status::Corruption("row value read extends beyond file size");
    }
    if (size != 0) {
      const auto* source = static_cast<const std::byte*>(valueMap.Data()) + offset;
      std::memcpy(data, source, size);
    }
    return Status::Ok();
  }

  void ReportWriteProgress(WritePhase phase, uint64_t completed,
                           uint64_t total) const noexcept {
    if (!options_.writeProgress) {
      return;
    }
    try {
      options_.writeProgress(
          {.phase = phase, .completed = completed, .total = total});
    } catch (...) {
      // Observability must never make an otherwise valid database write fail.
    }
  }

  Status BeginObjectWriteTransaction() {
    if (objectWriteMarkerActive_) {
      return Status::Ok();
    }

    std::error_code error;
    if (std::filesystem::exists(objectWriteMarkerPath_, error)) {
      return Status::Corruption(
          "unfinished object write marker requires a read-write reopen");
    }
    if (error) {
      return Status::IoError("inspect object write marker: " + error.message());
    }

    std::filesystem::remove(objectWriteMarkerTempPath_, error);
    if (error) {
      return Status::IoError("remove stale object write marker: " +
                             error.message());
    }

    detail::FileDescriptor markerFile;
    Status status =
        detail::OpenReadWriteCreate(objectWriteMarkerTempPath_, markerFile);
    if (!status) {
      return status;
    }
    status = detail::TruncateFile(markerFile.Get(), 0);
    if (!status) {
      return status;
    }
    detail::ObjectWriteMarker marker;
    marker.rollbackOffset = appendOffset_;
    if (indexMap_.IsMapped()) {
      marker.indexBucketCount = IndexHeader()->bucketCount;
    }
    status = detail::WriteAllAt(markerFile.Get(), &marker, sizeof(marker), 0);
    if (!status) {
      return status;
    }
    status = detail::SyncFile(markerFile.Get());
    if (!status) {
      return status;
    }
    markerFile.Reset();

    std::filesystem::rename(objectWriteMarkerTempPath_, objectWriteMarkerPath_,
                            error);
    if (error) {
      return Status::IoError("publish object write marker: " + error.message());
    }
    objectWriteMarkerActive_ = true;
    return detail::SyncDirectory(directory_);
  }

  Status FinishObjectWriteTransaction() {
    if (!objectWriteMarkerActive_) {
      return Status::Ok();
    }

    std::error_code error;
    std::filesystem::remove(objectWriteMarkerPath_, error);
    if (error) {
      return Status::IoError("remove object write marker: " + error.message());
    }
    Status status = detail::SyncDirectory(directory_);
    if (!status) {
      return status;
    }
    objectWriteMarkerActive_ = false;
    return Status::Ok();
  }

  Status RecoverInterruptedObjectWrite() {
    std::error_code error;
    const bool markerExists =
        std::filesystem::exists(objectWriteMarkerPath_, error);
    if (error) {
      return Status::IoError("inspect object write marker: " + error.message());
    }
    if (!markerExists) {
      if (!readOnly_) {
        std::filesystem::remove(objectWriteMarkerTempPath_, error);
        if (error) {
          return Status::IoError("remove stale object write marker: " +
                                 error.message());
        }
      }
      return Status::Ok();
    }
    if (readOnly_) {
      return Status::Corruption(
          "unfinished object write requires a read-write open for recovery");
    }

    detail::FileDescriptor markerFile;
    Status status = detail::OpenReadOnly(objectWriteMarkerPath_, markerFile);
    if (!status) {
      return status;
    }
    uint64_t markerSize = 0;
    status = detail::GetFileSize(markerFile.Get(), markerSize);
    if (!status) {
      return status;
    }
    if (markerSize != sizeof(detail::ObjectWriteMarker)) {
      return Status::Corruption("object write marker has an invalid size");
    }
    detail::ObjectWriteMarker marker;
    status = detail::ReadAllAt(markerFile.Get(), &marker, sizeof(marker), 0);
    if (!status) {
      return status;
    }
    if (!MagicEquals(marker.magic, detail::kObjectWriteMarkerMagic) ||
        marker.version != detail::kStorageVersion ||
        marker.headerSize != sizeof(detail::ObjectWriteMarker)) {
      return Status::Corruption("object write marker is invalid");
    }

    uint64_t valueFileSize = 0;
    status = detail::GetFileSize(valueFile_.Get(), valueFileSize);
    if (!status) {
      return status;
    }
    if (marker.rollbackOffset < sizeof(detail::ValueFileHeader) ||
        marker.rollbackOffset > valueFileSize) {
      return Status::Corruption("object write marker rollback offset is invalid");
    }

    status = detail::TruncateFile(valueFile_.Get(), marker.rollbackOffset);
    if (!status) {
      return status;
    }
    status = detail::SyncFile(valueFile_.Get());
    if (!status) {
      return status;
    }
    appendOffset_ = marker.rollbackOffset;
    recoveredObjectBucketCount_ = marker.indexBucketCount;

    std::filesystem::remove(indexPath_, error);
    if (error) {
      return Status::IoError("remove incomplete object index: " +
                             error.message());
    }
    status = detail::SyncDirectory(directory_);
    if (!status) {
      return status;
    }
    // Keep the marker until the rebuilt index has been synced. If recovery is
    // interrupted, the same rollback and rebuild can be repeated safely.
    objectWriteMarkerActive_ = true;
    objectIndexConsistent_ = true;
    return Status::Ok();
  }

  void CloseFiles() {
    rowValueMaps_.clear();
    rowIndexMap_.Unmap();
    rowIndexFile_.Reset();
    rowValueFiles_.clear();
    rowAppendOffsets_.clear();
    rowPendingInsertCount_.store(0, std::memory_order_relaxed);
    indexMap_.Unmap();
    indexFile_.Reset();
    std::vector<detail::IndexBucket>().swap(pendingUniqueBuckets_);
    std::vector<std::byte>().swap(objectWriteBuffer_);
    std::vector<uint64_t>().swap(objectBucketOccupancy_);
    objectBucketOccupancyValid_ = false;
    objectWriteMarkerActive_ = false;
    objectIndexConsistent_ = true;
    recoveredObjectBucketCount_ = 0;
    valueMap_.Unmap();
    valueFile_.Reset();
    appendOffset_ = 0;
  }

  Status OpenValueFile() {
    Status status = readOnly_ ? detail::OpenReadOnly(valuePath_, valueFile_)
                              : detail::OpenReadWriteCreate(valuePath_, valueFile_);
    if (!status) {
      return status;
    }

    uint64_t fileSize = 0;
    status = detail::GetFileSize(valueFile_.Get(), fileSize);
    if (!status) {
      return status;
    }

    if (fileSize == 0) {
      if (readOnly_) {
        return Status::Corruption("value file is empty");
      }
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
    if (MagicEquals(header.magic, detail::kLegacyValueFileMagic) &&
        header.version == detail::kStorageVersion &&
        header.headerSize == sizeof(detail::ValueFileHeader)) {
      if (readOnly_) {
        return Status::InvalidArgument(
            "legacy value file requires a read-write open for migration");
      }
      return MigrateLegacyValueFile(fileSize);
    }
    if (!MagicEquals(header.magic, detail::kValueFileMagic) ||
        header.version != detail::kStorageVersion ||
        header.headerSize != sizeof(detail::ValueFileHeader)) {
      return Status::Corruption("value file header is invalid");
    }

    appendOffset_ = fileSize;
    if (readOnly_) {
      return valueMap_.MapReadOnly(valueFile_.Get(), fileSize);
    }
    return Status::Ok();
  }

  Status MigrateLegacyValueFile(uint64_t legacyFileSize) {
    std::filesystem::path migrationPath = valuePath_;
    migrationPath += ".migrating";

    detail::FileDescriptor migrationFile;
    Status status = detail::OpenReadWriteCreate(migrationPath, migrationFile);
    if (!status) {
      return status;
    }
    status = detail::TruncateFile(migrationFile.Get(), 0);
    if (!status) {
      return status;
    }

    detail::ValueFileHeader newFileHeader;
    status = detail::WriteAllAt(migrationFile.Get(), &newFileHeader,
                                sizeof(newFileHeader), 0);
    if (!status) {
      return status;
    }

    std::vector<std::byte> copyBuffer(1024 * 1024);
    uint64_t sourceOffset = sizeof(detail::ValueFileHeader);
    uint64_t destinationOffset = sizeof(detail::ValueFileHeader);
    while (sourceOffset < legacyFileSize) {
      detail::LegacyValueRecordHeader legacyHeader;
      status = detail::ReadAllAt(valueFile_.Get(), &legacyHeader,
                                 sizeof(legacyHeader), sourceOffset);
      if (!status) {
        return status;
      }
      if (!IsValidLegacyRecordHeader(legacyHeader)) {
        return Status::Corruption("legacy value record header is invalid");
      }

      const uint64_t recordDataSize = legacyHeader.columnSize + legacyHeader.keySize +
                                      legacyHeader.valueSize;
      const uint64_t legacyRecordSize = sizeof(legacyHeader) + recordDataSize;
      if (legacyRecordSize > legacyFileSize - sourceOffset) {
        return Status::Corruption("legacy value record extends beyond file size");
      }

      detail::ValueRecordHeader recordHeader;
      recordHeader.columnSize = legacyHeader.columnSize;
      recordHeader.keySize = legacyHeader.keySize;
      recordHeader.valueSize = legacyHeader.valueSize;
      status = detail::WriteAllAt(migrationFile.Get(), &recordHeader,
                                  sizeof(recordHeader), destinationOffset);
      if (!status) {
        return status;
      }

      uint64_t remaining = recordDataSize;
      uint64_t recordSourceOffset = sourceOffset + sizeof(legacyHeader);
      uint64_t recordDestinationOffset = destinationOffset + sizeof(recordHeader);
      while (remaining > 0) {
        const size_t chunkSize = static_cast<size_t>(
            std::min<uint64_t>(remaining, copyBuffer.size()));
        status = detail::ReadAllAt(valueFile_.Get(), copyBuffer.data(), chunkSize,
                                   recordSourceOffset);
        if (!status) {
          return status;
        }
        status = detail::WriteAllAt(migrationFile.Get(), copyBuffer.data(), chunkSize,
                                    recordDestinationOffset);
        if (!status) {
          return status;
        }
        remaining -= chunkSize;
        recordSourceOffset += chunkSize;
        recordDestinationOffset += chunkSize;
      }

      sourceOffset += legacyRecordSize;
      destinationOffset += sizeof(recordHeader) + recordDataSize;
    }

    status = detail::SyncFile(migrationFile.Get());
    if (!status) {
      return status;
    }
    migrationFile.Reset();
    valueFile_.Reset();

    std::error_code error;
    std::filesystem::rename(migrationPath, valuePath_, error);
    if (error) {
      return Status::IoError("rename migrated value file: " + error.message());
    }
    std::filesystem::remove(indexPath_, error);
    if (error) {
      return Status::IoError("remove stale object index: " + error.message());
    }

    status = detail::OpenReadWriteCreate(valuePath_, valueFile_);
    if (!status) {
      return status;
    }
    appendOffset_ = destinationOffset;
    return Status::Ok();
  }

  Status OpenIndexFile() {
    Status status = readOnly_ ? detail::OpenReadOnly(indexPath_, indexFile_)
                              : detail::OpenReadWriteCreate(indexPath_, indexFile_);
    if (!status) {
      return status;
    }

    uint64_t fileSize = 0;
    status = detail::GetFileSize(indexFile_.Get(), fileSize);
    if (!status) {
      return status;
    }

    if (readOnly_ && fileSize == 0) {
      return Status::Corruption("index file is empty");
    }

    bool shouldRebuild = fileSize == 0;
    if (!shouldRebuild) {
      status = readOnly_ ? indexMap_.MapReadOnly(indexFile_.Get(), fileSize)
                         : indexMap_.Map(indexFile_.Get(), fileSize);
      if (!status) {
        return status;
      }
      shouldRebuild = !HasValidIndexLayout(fileSize);
      if (shouldRebuild) {
        indexMap_.Unmap();
      }
    }

    if (shouldRebuild) {
      if (readOnly_) {
        return Status::Corruption("index file must be valid in read-only mode");
      }
      status = BeginObjectWriteTransaction();
      if (!status) {
        return status;
      }
      objectIndexConsistent_ = false;
      status = CreateEmptyIndex(
          std::max(options_.initialBucketCount, recoveredObjectBucketCount_));
      if (!status) {
        return status;
      }
      recoveredObjectBucketCount_ = 0;
      uint64_t valueFileSize = 0;
      status = detail::GetFileSize(valueFile_.Get(), valueFileSize);
      if (!status) {
        return status;
      }
      if (valueFileSize != 0) {
        status = valueMap_.MapReadOnly(valueFile_.Get(), valueFileSize);
        if (!status) {
          return status;
        }
      }
      status = RebuildIndexFromValues();
      valueMap_.Unmap();
      if (!status) {
        return status;
      }
      objectIndexConsistent_ = true;
      return Status::Ok();
    }

    return Status::Ok();
  }

  Status OpenRowValueFiles() {
    rowValueFiles_.clear();
    rowValueMaps_.clear();
    rowAppendOffsets_.clear();
    rowValueFiles_.resize(options_.rowShardCount);
    rowValueMaps_.resize(options_.rowShardCount);
    rowAppendOffsets_.reserve(options_.rowShardCount);
    for (uint32_t shardId = 0; shardId < options_.rowShardCount; ++shardId) {
      rowAppendOffsets_.push_back(std::make_unique<std::atomic<uint64_t>>(0));
    }

    for (uint32_t shardId = 0; shardId < options_.rowShardCount; ++shardId) {
      Status status = readOnly_
                          ? detail::OpenReadOnly(RowValuePath(directory_, shardId),
                                                 rowValueFiles_[shardId])
                          : detail::OpenReadWriteCreate(RowValuePath(directory_, shardId),
                                                        rowValueFiles_[shardId]);
      if (!status) {
        return status;
      }

      uint64_t fileSize = 0;
      status = detail::GetFileSize(rowValueFiles_[shardId].Get(), fileSize);
      if (!status) {
        return status;
      }

      if (fileSize == 0) {
        if (readOnly_) {
          return Status::Corruption("row value file is empty");
        }
        detail::RowValueFileHeader header;
        status = detail::WriteAllAt(rowValueFiles_[shardId].Get(), &header,
                                    sizeof(header), 0);
        if (!status) {
          return status;
        }
        rowAppendOffsets_[shardId]->store(sizeof(header), std::memory_order_relaxed);
        continue;
      }

      if (fileSize < sizeof(detail::RowValueFileHeader)) {
        return Status::Corruption("row value file header is truncated");
      }

      detail::RowValueFileHeader header;
      status = detail::ReadAllAt(rowValueFiles_[shardId].Get(), &header,
                                 sizeof(header), 0);
      if (!status) {
        return status;
      }
      if (!MagicEquals(header.magic, detail::kRowValueFileMagic) ||
          header.version != detail::kStorageVersion ||
          header.headerSize != sizeof(detail::RowValueFileHeader)) {
        return Status::Corruption("row value file header is invalid");
      }
      rowAppendOffsets_[shardId]->store(fileSize, std::memory_order_relaxed);
      if (readOnly_) {
        status = rowValueMaps_[shardId].MapReadOnly(rowValueFiles_[shardId].Get(),
                                                    fileSize);
        if (!status) {
          return status;
        }
      }
    }

    return Status::Ok();
  }

  Status OpenRowIndexFile() {
    Status status = readOnly_ ? detail::OpenReadOnly(rowIndexPath_, rowIndexFile_)
                              : detail::OpenReadWriteCreate(rowIndexPath_, rowIndexFile_);
    if (!status) {
      return status;
    }

    uint64_t fileSize = 0;
    status = detail::GetFileSize(rowIndexFile_.Get(), fileSize);
    if (!status) {
      return status;
    }

    if (readOnly_ && fileSize == 0) {
      return Status::Corruption("row index file is empty");
    }

    bool shouldRebuild = fileSize == 0;
    if (!shouldRebuild) {
      status = readOnly_ ? rowIndexMap_.MapReadOnly(rowIndexFile_.Get(), fileSize)
                         : rowIndexMap_.Map(rowIndexFile_.Get(), fileSize);
      if (!status) {
        return status;
      }
      shouldRebuild = !HasValidRowIndexLayout(fileSize);
      if (shouldRebuild) {
        rowIndexMap_.Unmap();
      }
    }

    if (shouldRebuild) {
      if (readOnly_) {
        return Status::Corruption("row index file must be valid in read-only mode");
      }
      status = CreateEmptyRowIndex(options_.initialRowBucketCount);
      if (!status) {
        return status;
      }
      return RebuildRowIndexFromValues();
    }

    return Status::Ok();
  }

  bool HasValidRowIndexLayout(uint64_t fileSize) const {
    if (fileSize < sizeof(detail::RowIndexFileHeader)) {
      return false;
    }
    const detail::RowIndexFileHeader* header = RowIndexHeader();
    if (!MagicEquals(header->magic, detail::kRowIndexFileMagic) ||
        header->version != detail::kStorageVersion ||
        header->headerSize != sizeof(detail::RowIndexFileHeader)) {
      return false;
    }
    if (header->shardCount != options_.rowShardCount || header->bucketCount == 0 ||
        (header->bucketCount & (header->bucketCount - 1)) != 0) {
      return false;
    }
    if (header->bucketCount >
        (std::numeric_limits<uint64_t>::max() -
         sizeof(detail::RowIndexFileHeader)) /
            sizeof(detail::RowIndexBucket)) {
      return false;
    }
    const uint64_t expectedSize =
        sizeof(detail::RowIndexFileHeader) +
        header->bucketCount * sizeof(detail::RowIndexBucket);
    if (expectedSize != fileSize || header->itemCount > header->bucketCount) {
      return false;
    }

    uint64_t filledCount = 0;
    for (uint64_t index = 0; index < header->bucketCount; ++index) {
      const uint32_t state = RowBuckets()[index].state;
      if (state == detail::kRowBucketFilled) {
        ++filledCount;
        continue;
      }
      if (state != detail::kRowBucketEmpty) {
        return false;
      }
    }
    return filledCount == header->itemCount;
  }

  Status CreateEmptyRowIndex(uint64_t bucketCount) {
    bucketCount = RoundUpPowerOfTwo(bucketCount);
    if (bucketCount == 0 ||
        bucketCount >
            (std::numeric_limits<uint64_t>::max() -
             sizeof(detail::RowIndexFileHeader)) /
                sizeof(detail::RowIndexBucket)) {
      return Status::InvalidArgument("row index size exceeds uint64");
    }
    const uint64_t fileSize =
        sizeof(detail::RowIndexFileHeader) + bucketCount * sizeof(detail::RowIndexBucket);

    rowIndexMap_.Unmap();
    Status status = detail::TruncateFile(rowIndexFile_.Get(), fileSize);
    if (!status) {
      return status;
    }
    status = rowIndexMap_.Map(rowIndexFile_.Get(), fileSize);
    if (!status) {
      return status;
    }

    std::memset(rowIndexMap_.Data(), 0, static_cast<size_t>(rowIndexMap_.Size()));
    detail::RowIndexFileHeader header;
    header.bucketCount = bucketCount;
    header.shardCount = options_.rowShardCount;
    *RowIndexHeader() = header;
    return Status::Ok();
  }

  Status RebuildRowIndexFromValues() {
    uint64_t nextSequence = 1;
    for (uint32_t shardId = 0; shardId < rowValueFiles_.size(); ++shardId) {
      uint64_t fileSize = 0;
      Status status = detail::GetFileSize(rowValueFiles_[shardId].Get(), fileSize);
      if (!status) {
        return status;
      }

      uint64_t offset = sizeof(detail::RowValueFileHeader);
      while (offset < fileSize) {
        detail::RowBlockHeader blockHeader;
        status = detail::ReadAllAt(rowValueFiles_[shardId].Get(), &blockHeader,
                                   sizeof(blockHeader), offset);
        if (!status) {
          return status;
        }
        if (!IsValidRowBlockHeader(blockHeader)) {
          return Status::Corruption("row block header is invalid");
        }
        if (blockHeader.blockSize > fileSize - offset) {
          return Status::Corruption("row block extends beyond file size");
        }

        std::string column;
        status = ReadRowBlockColumn(shardId, offset, blockHeader, column);
        if (!status) {
          return status;
        }

        status = EnsureRowCapacityForInsert();
        if (!status) {
          return status;
        }

        RowBucketLookup lookup;
        status = FindRowBucket(column, blockHeader.rowId, blockHeader.columnHash, lookup);
        if (!status) {
          return status;
        }

        detail::RowIndexBucket bucket;
        bucket.state = detail::kRowBucketFilled;
        bucket.columnHash = blockHeader.columnHash;
        bucket.rowId = blockHeader.rowId;
        bucket.shardId = shardId;
        bucket.blockOffset = offset;
        bucket.blockSize = blockHeader.blockSize;
        bucket.sequence = blockHeader.sequence;

        RowBuckets()[lookup.index] = bucket;
        if (!lookup.found) {
          ++RowIndexHeader()->itemCount;
        }
        nextSequence = std::max(nextSequence, blockHeader.sequence + 1);
        offset += blockHeader.blockSize;
      }
      rowAppendOffsets_[shardId]->store(fileSize, std::memory_order_relaxed);
    }

    RowIndexHeader()->nextSequence = nextSequence;
    return Status::Ok();
  }

  bool HasValidIndexLayout(uint64_t fileSize) {
    objectBucketOccupancy_.clear();
    objectBucketOccupancyValid_ = false;
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
    if (header->bucketCount >
        (std::numeric_limits<uint64_t>::max() -
         sizeof(detail::IndexFileHeader)) /
            sizeof(detail::IndexBucket)) {
      return false;
    }
    const uint64_t expectedSize =
        sizeof(detail::IndexFileHeader) +
        header->bucketCount * sizeof(detail::IndexBucket);
    if (expectedSize != fileSize || header->itemCount > header->bucketCount) {
      return false;
    }

    // The layout validation already scans every bucket. Reuse that scan to build
    // the write-only side table without adding work to read-only opens.
    if (!readOnly_) {
      objectBucketOccupancy_.assign(
          static_cast<size_t>((header->bucketCount + 63) / 64), 0);
    }
    uint64_t filledCount = 0;
    for (uint64_t index = 0; index < header->bucketCount; ++index) {
      if (Buckets()[index].hash != 0) {
        if (!readOnly_) {
          objectBucketOccupancy_[index / 64] |= 1ULL << (index % 64);
        }
        ++filledCount;
      }
    }
    if (filledCount != header->itemCount) {
      objectBucketOccupancy_.clear();
      return false;
    }
    objectBucketOccupancyValid_ = !readOnly_;
    return true;
  }

  Status CreateEmptyIndex(uint64_t bucketCount) {
    bucketCount = RoundUpPowerOfTwo(bucketCount);
    if (bucketCount == 0 ||
        bucketCount >
            (std::numeric_limits<uint64_t>::max() -
             sizeof(detail::IndexFileHeader)) /
                sizeof(detail::IndexBucket)) {
      return Status::InvalidArgument("object index size exceeds uint64");
    }
    const uint64_t fileSize =
        sizeof(detail::IndexFileHeader) + bucketCount * sizeof(detail::IndexBucket);

    indexMap_.Unmap();
    // Reserve physical storage before mmap stores begin. A sparse ftruncate can
    // otherwise succeed and later deliver SIGBUS when the filesystem is full.
    Status status = detail::TruncateFile(indexFile_.Get(), 0);
    if (!status) {
      return status;
    }
    status = detail::PreallocateFile(indexFile_.Get(), fileSize);
    if (!status) {
      return status;
    }
    status = indexMap_.Map(indexFile_.Get(), fileSize);
    if (!status) {
      return status;
    }

    detail::IndexFileHeader header;
    header.bucketCount = bucketCount;
    *IndexHeader() = header;
    objectBucketOccupancy_.assign(
        static_cast<size_t>((bucketCount + 63) / 64), 0);
    objectBucketOccupancyValid_ = true;
    return Status::Ok();
  }

  Status RebuildIndexFromValues() {
    uint64_t fileSize = 0;
    Status status = detail::GetFileSize(valueFile_.Get(), fileSize);
    if (!status) {
      return status;
    }

    uint64_t offset = sizeof(detail::ValueFileHeader);
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

      const uint64_t columnHash = detail::HashString(column);
      const uint64_t keyHash = detail::HashString(key);
      BucketLookup lookup;
      status = FindBucket(column, key, columnHash, keyHash, lookup);
      if (!status) {
        return status;
      }

      detail::IndexBucket bucket;
      bucket.hash = ObjectIndexHash(columnHash, keyHash);
      bucket.recordOffset = offset;

      Buckets()[lookup.index] = bucket;
      if (!lookup.found) {
        MarkObjectBucketOccupied(lookup.index);
        ++IndexHeader()->itemCount;
      }

      offset += recordSize;
    }

    appendOffset_ = fileSize;
    return Status::Ok();
  }

  bool IsValidRecordHeader(const detail::ValueRecordHeader& recordHeader) const {
    return recordHeader.magic == detail::kRecordMagic &&
           recordHeader.version == detail::kStorageVersion &&
           recordHeader.headerSize == sizeof(detail::ValueRecordHeader);
  }

  bool IsValidLegacyRecordHeader(
      const detail::LegacyValueRecordHeader& recordHeader) const {
    return recordHeader.magic == detail::kRecordMagic &&
           recordHeader.version == detail::kStorageVersion &&
           recordHeader.headerSize == sizeof(detail::LegacyValueRecordHeader);
  }

  Status ReadRecordColumnKey(uint64_t recordOffset,
                             const detail::ValueRecordHeader& recordHeader,
                             std::string& column, std::string& key) const {
    column.resize(recordHeader.columnSize);
    key.resize(recordHeader.keySize);

    uint64_t offset = recordOffset + sizeof(detail::ValueRecordHeader);
    Status status =
        ReadValueAt(column.data(), column.size(), offset);
    if (!status) {
      return status;
    }
    offset += column.size();
    return ReadValueAt(key.data(), key.size(), offset);
  }

  bool IsValidRowBlockHeader(const detail::RowBlockHeader& blockHeader) const {
    if (blockHeader.magic != detail::kRowBlockMagic ||
        blockHeader.version != detail::kStorageVersion ||
        blockHeader.headerSize != sizeof(detail::RowBlockHeader)) {
      return false;
    }
    if (blockHeader.bucketCount == 0 ||
        (blockHeader.bucketCount & (blockHeader.bucketCount - 1)) != 0) {
      return false;
    }
    const uint64_t minimumSize =
        sizeof(detail::RowBlockHeader) +
        static_cast<uint64_t>(blockHeader.bucketCount) * sizeof(detail::RowKeyBucket) +
        blockHeader.columnSize + blockHeader.keyBytesSize + blockHeader.valueBytesSize;
    return blockHeader.blockSize == minimumSize;
  }

  Status ReadRowBlockColumn(uint32_t shardId, uint64_t blockOffset,
                            const detail::RowBlockHeader& blockHeader,
                            std::string& column) const {
    column.resize(blockHeader.columnSize);
    const uint64_t columnOffset =
        blockOffset + sizeof(detail::RowBlockHeader) +
        static_cast<uint64_t>(blockHeader.bucketCount) * sizeof(detail::RowKeyBucket);
    return ReadRowValueAt(shardId, column.data(), column.size(), columnOffset);
  }

  Status LoadRowBlock(const detail::RowIndexBucket& bucket,
                      std::vector<std::byte>& block) const {
    if (bucket.shardId >= rowValueFiles_.size()) {
      return Status::Corruption("row index points to an invalid shard");
    }
    block.resize(static_cast<size_t>(bucket.blockSize));
    return ReadRowValueAt(bucket.shardId, block.data(), block.size(), bucket.blockOffset);
  }

  Status DecodeRowBlockEntries(const std::vector<std::byte>& block,
                               std::string_view column, uint64_t rowId,
                               std::vector<std::string>& keys,
                               std::vector<std::vector<std::byte>>& values) const {
    if (block.size() < sizeof(detail::RowBlockHeader)) {
      return Status::Corruption("row block is truncated");
    }

    const auto* header =
        reinterpret_cast<const detail::RowBlockHeader*>(block.data());
    if (!IsValidRowBlockHeader(*header) || header->blockSize != block.size() ||
        header->rowId != rowId || header->columnHash != detail::HashString(column)) {
      return Status::Corruption("row block header does not match requested row");
    }

    const uint64_t bucketsOffset = sizeof(detail::RowBlockHeader);
    const uint64_t columnOffset =
        bucketsOffset + static_cast<uint64_t>(header->bucketCount) *
                            sizeof(detail::RowKeyBucket);
    const uint64_t keyBytesOffset = columnOffset + header->columnSize;
    const uint64_t valueBytesOffset = keyBytesOffset + header->keyBytesSize;

    const auto* storedColumn =
        reinterpret_cast<const char*>(block.data() + columnOffset);
    if (std::string_view(storedColumn, header->columnSize) != column) {
      return Status::Corruption("row block column does not match requested column");
    }

    const auto* keyBuckets =
        reinterpret_cast<const detail::RowKeyBucket*>(block.data() + bucketsOffset);
    keys.clear();
    values.clear();
    keys.reserve(header->itemCount);
    values.reserve(header->itemCount);

    for (uint64_t index = 0; index < header->bucketCount; ++index) {
      const detail::RowKeyBucket& bucket = keyBuckets[index];
      if (bucket.state == detail::kBucketEmpty) {
        continue;
      }
      if (bucket.state != detail::kBucketFilled) {
        return Status::Corruption("row key bucket state is invalid");
      }
      if (bucket.keyOffset + bucket.keySize > header->keyBytesSize ||
          bucket.valueOffset + bucket.valueSize > header->valueBytesSize) {
        return Status::Corruption("row key bucket points outside block");
      }

      const auto* storedKey = reinterpret_cast<const char*>(
          block.data() + keyBytesOffset + bucket.keyOffset);
      const std::byte* valueBegin =
          block.data() + valueBytesOffset + bucket.valueOffset;
      keys.emplace_back(storedKey, static_cast<size_t>(bucket.keySize));
      values.emplace_back(valueBegin, valueBegin + bucket.valueSize);
    }

    if (keys.size() != header->itemCount) {
      return Status::Corruption("row block item count does not match key buckets");
    }
    return Status::Ok();
  }

  Status LoadRowEntries(std::string_view column, uint64_t rowId, uint64_t columnHash,
                        bool& found, std::vector<std::string>& keys,
                        std::vector<std::vector<std::byte>>& values) const {
    found = false;
    keys.clear();
    values.clear();

    RowBucketLookup lookup;
    Status status = FindRowBucket(column, rowId, columnHash, lookup);
    if (!status) {
      return status;
    }
    if (!lookup.found) {
      return Status::Ok();
    }

    detail::RowIndexBucket bucket;
    status = LoadFilledRowBucketSnapshot(lookup.index, bucket);
    if (!status) {
      return status;
    }

    std::vector<std::byte> block;
    status = LoadRowBlock(bucket, block);
    if (!status) {
      return status;
    }
    status = DecodeRowBlockEntries(block, column, rowId, keys, values);
    if (!status) {
      return status;
    }

    found = true;
    return Status::Ok();
  }

  Status BuildRowBlockLayout(std::string_view column, uint64_t rowId,
                             std::span<const RowStructEntry> entries,
                             uint64_t sequence, uint64_t columnHash,
                             RowBlockLayout& layout) const {
    if (entries.size() > std::numeric_limits<uint32_t>::max()) {
      return Status::InvalidArgument("row entry count must fit in uint32");
    }

    const uint64_t bucketCount = RoundUpPowerOfTwo(
        static_cast<uint64_t>(static_cast<double>(entries.size()) / options_.maxLoadFactor) +
        1);
    if (bucketCount == 0 ||
        bucketCount > std::numeric_limits<uint32_t>::max()) {
      return Status::InvalidArgument("row key bucket count must fit in uint32");
    }

    layout.keyBuckets.assign(static_cast<size_t>(bucketCount), {});
    layout.keyBytes.clear();
    layout.valueEntries.clear();
    layout.keyBytes.reserve(entries.size() * 16);
    std::vector<size_t> selectedEntries(static_cast<size_t>(bucketCount), entries.size());
    uint32_t itemCount = 0;

    for (size_t entryIndex = 0; entryIndex < entries.size(); ++entryIndex) {
      const RowStructEntry& entry = entries[entryIndex];
      const uint64_t keyHash = detail::HashString(entry.key);
      const uint64_t mask = bucketCount - 1;
      const uint64_t start = detail::MixHashes(keyHash, rowId) & mask;
      bool placed = false;

      for (uint64_t probe = 0; probe < bucketCount; ++probe) {
        const uint64_t bucketIndex = (start + probe) & mask;
        detail::RowKeyBucket& bucket = layout.keyBuckets[bucketIndex];
        if (bucket.state == detail::kBucketEmpty) {
          const uint64_t keyOffset = layout.keyBytes.size();
          AppendBytes(layout.keyBytes, entry.key.data(), entry.key.size());

          bucket.state = detail::kBucketFilled;
          bucket.keyHash = keyHash;
          bucket.keyOffset = keyOffset;
          bucket.keySize = entry.key.size();
          selectedEntries[bucketIndex] = entryIndex;
          ++itemCount;
          placed = true;
          break;
        }

        if (bucket.keyHash != keyHash || bucket.keySize != entry.key.size()) {
          continue;
        }
        const auto* storedKey =
            reinterpret_cast<const char*>(layout.keyBytes.data() + bucket.keyOffset);
        if (std::string_view(storedKey, static_cast<size_t>(bucket.keySize)) !=
            entry.key) {
          continue;
        }

        selectedEntries[bucketIndex] = entryIndex;
        placed = true;
        break;
      }

      if (!placed) {
        return Status::Corruption("row key table is full");
      }
    }

    uint64_t valueBytesSize = 0;
    layout.valueEntries.reserve(itemCount);
    for (uint64_t bucketIndex = 0; bucketIndex < bucketCount; ++bucketIndex) {
      detail::RowKeyBucket& bucket = layout.keyBuckets[bucketIndex];
      if (bucket.state == detail::kBucketEmpty) {
        continue;
      }

      const RowStructEntry& entry = entries[selectedEntries[bucketIndex]];
      bucket.valueOffset = valueBytesSize;
      bucket.valueSize = entry.flatBufferBytes.size();
      valueBytesSize += entry.flatBufferBytes.size();
      layout.valueEntries.push_back(&entry);
    }

    layout.header = {};
    layout.header.sequence = sequence;
    layout.header.columnHash = columnHash;
    layout.header.rowId = rowId;
    layout.header.columnSize = static_cast<uint32_t>(column.size());
    layout.header.itemCount = itemCount;
    layout.header.bucketCount = static_cast<uint32_t>(bucketCount);
    layout.header.keyBytesSize = layout.keyBytes.size();
    layout.header.valueBytesSize = valueBytesSize;
    layout.header.blockSize =
        sizeof(detail::RowBlockHeader) + bucketCount * sizeof(detail::RowKeyBucket) +
        column.size() + layout.keyBytes.size() + valueBytesSize;
    return Status::Ok();
  }

  Status FindValueInRowBlockAt(const detail::RowIndexBucket& rowBucket,
                               const detail::RowBlockHeader& blockHeader,
                               std::string_view key,
                               std::vector<std::byte>& flatBufferBytes) const {
    if (rowBucket.shardId >= rowValueFiles_.size()) {
      return Status::Corruption("row index points to an invalid shard");
    }
    if (!IsValidRowBlockHeader(blockHeader) ||
        blockHeader.blockSize != rowBucket.blockSize) {
      return Status::Corruption("row index points to an invalid row block");
    }

    const uint64_t bucketsOffset = sizeof(detail::RowBlockHeader);
    const uint64_t columnOffset =
        bucketsOffset + static_cast<uint64_t>(blockHeader.bucketCount) *
                            sizeof(detail::RowKeyBucket);
    const uint64_t keyBytesOffset = columnOffset + blockHeader.columnSize;
    const uint64_t valueBytesOffset = keyBytesOffset + blockHeader.keyBytesSize;
    const uint64_t keyHash = detail::HashString(key);
    const uint64_t mask = blockHeader.bucketCount - 1;
    const uint64_t start = detail::MixHashes(keyHash, blockHeader.rowId) & mask;

    for (uint64_t probe = 0; probe < blockHeader.bucketCount; ++probe) {
      const uint64_t bucketIndex = (start + probe) & mask;
      detail::RowKeyBucket bucket;
      Status status = ReadRowValueAt(
          rowBucket.shardId, &bucket, sizeof(bucket),
          rowBucket.blockOffset + bucketsOffset +
              bucketIndex * sizeof(detail::RowKeyBucket));
      if (!status) {
        return status;
      }
      if (bucket.state == detail::kBucketEmpty) {
        return Status::NotFound("key not found in row");
      }
      if (bucket.state != detail::kBucketFilled) {
        return Status::Corruption("row key bucket state is invalid");
      }
      if (bucket.keyHash != keyHash || bucket.keySize != key.size()) {
        continue;
      }
      if (bucket.keyOffset > blockHeader.keyBytesSize ||
          bucket.keySize > blockHeader.keyBytesSize - bucket.keyOffset ||
          bucket.valueOffset > blockHeader.valueBytesSize ||
          bucket.valueSize > blockHeader.valueBytesSize - bucket.valueOffset) {
        return Status::Corruption("row key bucket points outside block");
      }

      std::string storedKey(key.size(), '\0');
      status = ReadRowValueAt(rowBucket.shardId, storedKey.data(), storedKey.size(),
                              rowBucket.blockOffset + keyBytesOffset +
                                  bucket.keyOffset);
      if (!status) {
        return status;
      }
      if (storedKey != key) {
        continue;
      }

      flatBufferBytes.resize(static_cast<size_t>(bucket.valueSize));
      return ReadRowValueAt(rowBucket.shardId, flatBufferBytes.data(),
                            flatBufferBytes.size(),
                            rowBucket.blockOffset + valueBytesOffset +
                                bucket.valueOffset);
    }

    return Status::NotFound("key not found in row");
  }

  Status EnsureCapacityForInsert(uint64_t insertCount = 1) {
    const detail::IndexFileHeader* header = IndexHeader();
    if (insertCount >
        std::numeric_limits<uint64_t>::max() - header->itemCount) {
      return Status::InvalidArgument("object count exceeds uint64");
    }
    const uint64_t targetItemCount = header->itemCount + insertCount;
    uint64_t bucketCount = header->bucketCount;
    while (static_cast<double>(targetItemCount) /
               static_cast<double>(bucketCount) >
           options_.maxLoadFactor) {
      if (bucketCount > (1ULL << 62)) {
        return Status::InvalidArgument(
            "object index exceeds the largest supported size");
      }
      bucketCount *= 2;
    }
    if (bucketCount == header->bucketCount) {
      return Status::Ok();
    }
    return ResizeIndex(bucketCount);
  }

  Status ResizeIndex(uint64_t newBucketCount) {
    const uint64_t oldBucketCount = IndexHeader()->bucketCount;
    const uint64_t oldItemCount = IndexHeader()->itemCount;
    const uint64_t progressTotal = oldBucketCount + oldItemCount + 1;
    ReportWriteProgress(WritePhase::kResizingIndex, 0, progressTotal);
    std::vector<detail::IndexBucket> oldBuckets;
    oldBuckets.reserve(static_cast<size_t>(oldItemCount));
    for (uint64_t index = 0; index < oldBucketCount; ++index) {
      const detail::IndexBucket& bucket = Buckets()[index];
      if (bucket.hash != 0) {
        oldBuckets.push_back(bucket);
      }
      if ((index + 1) % kObjectProgressInterval == 0) {
        ReportWriteProgress(WritePhase::kResizingIndex, index + 1,
                            progressTotal);
      }
    }
    ReportWriteProgress(WritePhase::kResizingIndex, oldBucketCount,
                        progressTotal);

    const uint64_t nextSequence = IndexHeader()->nextSequence;
    Status status = CreateEmptyIndex(newBucketCount);
    if (!status) {
      return status;
    }
    ReportWriteProgress(WritePhase::kResizingIndex, oldBucketCount + 1,
                        progressTotal);
    IndexHeader()->nextSequence = nextSequence;

    for (size_t index = 0; index < oldBuckets.size(); ++index) {
      status = PlaceExistingBucket(oldBuckets[index]);
      if (!status) {
        return status;
      }
      ++IndexHeader()->itemCount;
      if ((index + 1) % kObjectProgressInterval == 0) {
        ReportWriteProgress(WritePhase::kResizingIndex,
                            oldBucketCount + index + 2, progressTotal);
      }
    }
    ReportWriteProgress(WritePhase::kResizingIndex, progressTotal,
                        progressTotal);
    return Status::Ok();
  }

  Status PlaceExistingBucket(const detail::IndexBucket& bucket) {
    if (objectBucketOccupancyValid_) {
      return PlaceNewBucket(bucket);
    }

    const uint64_t bucketCount = IndexHeader()->bucketCount;
    const uint64_t mask = bucketCount - 1;
    const uint64_t start = bucket.hash & mask;

    for (uint64_t probe = 0; probe < bucketCount; ++probe) {
      const uint64_t index = (start + probe) & mask;
      if (Buckets()[index].hash == 0) {
        Buckets()[index] = bucket;
        MarkObjectBucketOccupied(index);
        return Status::Ok();
      }
    }
    return Status::Corruption("index table is full during resize");
  }

  Status PlaceNewBucket(const detail::IndexBucket& bucket) {
    const uint64_t bucketCount = IndexHeader()->bucketCount;
    const uint64_t mask = bucketCount - 1;
    const uint64_t start = bucket.hash & mask;

    if (objectBucketOccupancyValid_) {
      // Probe the compact, cache-friendly bitmap instead of loading random
      // 16-byte mmap buckets merely to discover whether they are empty.
      uint64_t index = 0;
      if (!FindEmptyObjectBucket(start, bucketCount, index) &&
          !FindEmptyObjectBucket(0, start, index)) {
        return Status::Corruption("index table is full during unique insert");
      }
      MarkObjectBucketOccupied(index);
      Buckets()[index] = bucket;
      return Status::Ok();
    }

    for (uint64_t probe = 0; probe < bucketCount; ++probe) {
      const uint64_t index = (start + probe) & mask;
      if (Buckets()[index].hash == 0) {
        Buckets()[index] = bucket;
        return Status::Ok();
      }
    }
    return Status::Corruption("index table is full during unique insert");
  }

  bool FindEmptyObjectBucket(uint64_t begin, uint64_t end,
                             uint64_t& index) const {
    if (begin >= end) {
      return false;
    }

    uint64_t wordIndex = begin / 64;
    uint32_t firstBit = static_cast<uint32_t>(begin % 64);
    const uint64_t lastWord = (end - 1) / 64;
    while (wordIndex <= lastWord) {
      uint64_t available = ~objectBucketOccupancy_[wordIndex];
      available &= std::numeric_limits<uint64_t>::max() << firstBit;
      const uint64_t wordEnd = (wordIndex + 1) * 64;
      if (wordEnd > end) {
        const uint32_t endBit = static_cast<uint32_t>(end - wordIndex * 64);
        available &= (1ULL << endBit) - 1;
      }
      if (available != 0) {
        index = wordIndex * 64 + std::countr_zero(available);
        return true;
      }
      ++wordIndex;
      firstBit = 0;
    }
    return false;
  }

  void MarkObjectBucketOccupied(uint64_t index) {
    if (objectBucketOccupancyValid_) {
      objectBucketOccupancy_[index / 64] |= 1ULL << (index % 64);
    }
  }

  Status EnsureRowCapacityForInsert() {
    const detail::RowIndexFileHeader* header = RowIndexHeader();
    const double loadAfterInsert =
        static_cast<double>(header->itemCount + 1) /
        static_cast<double>(header->bucketCount);
    if (loadAfterInsert <= options_.maxLoadFactor) {
      return Status::Ok();
    }
    return ResizeRowIndex(header->bucketCount * 2);
  }

  Status ResizeRowIndex(uint64_t newBucketCount) {
    std::vector<detail::RowIndexBucket> oldBuckets;
    oldBuckets.reserve(static_cast<size_t>(RowIndexHeader()->itemCount));
    for (uint64_t index = 0; index < RowIndexHeader()->bucketCount; ++index) {
      const detail::RowIndexBucket& bucket = RowBuckets()[index];
      if (bucket.state == detail::kRowBucketFilled) {
        oldBuckets.push_back(bucket);
      }
    }

    const uint64_t nextSequence = RowIndexHeader()->nextSequence;
    Status status = CreateEmptyRowIndex(newBucketCount);
    if (!status) {
      return status;
    }
    RowIndexHeader()->nextSequence = nextSequence;

    for (const detail::RowIndexBucket& bucket : oldBuckets) {
      status = PlaceExistingRowBucket(bucket);
      if (!status) {
        return status;
      }
      ++RowIndexHeader()->itemCount;
    }
    return Status::Ok();
  }

  Status PlaceExistingRowBucket(const detail::RowIndexBucket& bucket) {
    const uint64_t bucketCount = RowIndexHeader()->bucketCount;
    const uint64_t mask = bucketCount - 1;
    const uint64_t start = detail::MixHashes(bucket.columnHash, bucket.rowId) & mask;

    for (uint64_t probe = 0; probe < bucketCount; ++probe) {
      const uint64_t index = (start + probe) & mask;
      if (RowBuckets()[index].state == detail::kRowBucketEmpty) {
        RowBuckets()[index] = bucket;
        return Status::Ok();
      }
    }
    return Status::Corruption("row index table is full during resize");
  }

  template <typename T>
  T AtomicLoad(const T& value, std::memory_order order) const {
    return std::atomic_ref<T>(const_cast<T&>(value)).load(order);
  }

  template <typename T>
  void AtomicStore(T& target, T value, std::memory_order order) {
    std::atomic_ref<T>(target).store(value, order);
  }

  uint64_t LoadRowItemCount(
      std::memory_order order = std::memory_order_acquire) const {
    return AtomicLoad(RowIndexHeader()->itemCount, order);
  }

  void IncrementRowItemCount() {
    std::atomic_ref<uint64_t>(RowIndexHeader()->itemCount)
        .fetch_add(1, std::memory_order_release);
  }

  uint64_t NextRowSequence() {
    return std::atomic_ref<uint64_t>(RowIndexHeader()->nextSequence)
        .fetch_add(1, std::memory_order_relaxed);
  }

  Status ReserveRowIndexInsertSlot() {
    const uint64_t bucketCount = RowIndexHeader()->bucketCount;
    for (;;) {
      const uint64_t itemCount = LoadRowItemCount();
      uint64_t pending = rowPendingInsertCount_.load(std::memory_order_relaxed);
      const double loadAfterInsert =
          static_cast<double>(itemCount + pending + 1) /
          static_cast<double>(bucketCount);
      if (loadAfterInsert > options_.maxLoadFactor) {
        return Status::InvalidArgument(
            "row index capacity exceeded; increase initialRowBucketCount");
      }
      if (rowPendingInsertCount_.compare_exchange_weak(
              pending, pending + 1, std::memory_order_acq_rel,
              std::memory_order_relaxed)) {
        return Status::Ok();
      }
    }
  }

  void ReleaseRowIndexInsertSlot() {
    rowPendingInsertCount_.fetch_sub(1, std::memory_order_acq_rel);
  }

  Status LoadRowBucketSnapshot(uint64_t index, detail::RowIndexBucket& snapshot,
                               RowBucketSnapshotState& snapshotState) const {
    const detail::RowIndexBucket& bucket = RowBuckets()[index];
    const uint32_t state = AtomicLoad(bucket.state, std::memory_order_acquire);
    if (state == detail::kRowBucketEmpty) {
      snapshotState = RowBucketSnapshotState::kEmpty;
      return Status::Ok();
    }
    if (state == detail::kRowBucketWriting) {
      snapshotState = RowBucketSnapshotState::kWriting;
      return Status::Ok();
    }
    if (state != detail::kRowBucketFilled) {
      return Status::Corruption("row index bucket state is invalid");
    }

    snapshot.state = detail::kRowBucketFilled;
    snapshot.shardId = AtomicLoad(bucket.shardId, std::memory_order_relaxed);
    snapshot.columnHash = AtomicLoad(bucket.columnHash, std::memory_order_relaxed);
    snapshot.rowId = AtomicLoad(bucket.rowId, std::memory_order_relaxed);
    snapshot.blockOffset = AtomicLoad(bucket.blockOffset, std::memory_order_relaxed);
    snapshot.blockSize = AtomicLoad(bucket.blockSize, std::memory_order_relaxed);
    snapshot.sequence = AtomicLoad(bucket.sequence, std::memory_order_relaxed);

    const uint32_t stateAfter = AtomicLoad(bucket.state, std::memory_order_acquire);
    if (stateAfter != detail::kRowBucketFilled) {
      snapshotState = RowBucketSnapshotState::kWriting;
      return Status::Ok();
    }

    snapshotState = RowBucketSnapshotState::kFilled;
    return Status::Ok();
  }

  Status LoadFilledRowBucketSnapshot(uint64_t index,
                                     detail::RowIndexBucket& snapshot) const {
    for (;;) {
      RowBucketSnapshotState snapshotState = RowBucketSnapshotState::kEmpty;
      Status status = LoadRowBucketSnapshot(index, snapshot, snapshotState);
      if (!status) {
        return status;
      }
      if (snapshotState == RowBucketSnapshotState::kFilled) {
        return Status::Ok();
      }
      if (snapshotState == RowBucketSnapshotState::kEmpty) {
        return Status::Corruption("row index bucket is empty");
      }
      std::this_thread::yield();
    }
  }

  Status LoadFilledRowBucketSnapshotIfPresent(uint64_t index,
                                              detail::RowIndexBucket& snapshot) const {
    for (;;) {
      RowBucketSnapshotState snapshotState = RowBucketSnapshotState::kEmpty;
      Status status = LoadRowBucketSnapshot(index, snapshot, snapshotState);
      if (!status) {
        return status;
      }
      if (snapshotState == RowBucketSnapshotState::kFilled) {
        return Status::Ok();
      }
      if (snapshotState == RowBucketSnapshotState::kEmpty) {
        snapshot = {};
        return Status::Ok();
      }
      std::this_thread::yield();
    }
  }

  void StoreRowBucketData(detail::RowIndexBucket& bucket,
                          const RowBucketData& data) {
    AtomicStore(bucket.shardId, data.shardId, std::memory_order_relaxed);
    AtomicStore(bucket.columnHash, data.columnHash, std::memory_order_relaxed);
    AtomicStore(bucket.rowId, data.rowId, std::memory_order_relaxed);
    AtomicStore(bucket.blockOffset, data.blockOffset, std::memory_order_relaxed);
    AtomicStore(bucket.blockSize, data.blockSize, std::memory_order_relaxed);
    AtomicStore(bucket.sequence, data.sequence, std::memory_order_relaxed);
  }

  Status InsertOrUpdateRowBucket(std::string_view column,
                                 const RowBucketData& data, bool& inserted) {
    const uint64_t bucketCount = RowIndexHeader()->bucketCount;
    const uint64_t mask = bucketCount - 1;
    const uint64_t start = detail::MixHashes(data.columnHash, data.rowId) & mask;

    for (;;) {
      bool retry = false;
      for (uint64_t probe = 0; probe < bucketCount; ++probe) {
        const uint64_t index = (start + probe) & mask;
        detail::RowIndexBucket& target = RowBuckets()[index];

        detail::RowIndexBucket snapshot;
        RowBucketSnapshotState snapshotState = RowBucketSnapshotState::kEmpty;
        Status status = LoadRowBucketSnapshot(index, snapshot, snapshotState);
        if (!status) {
          return status;
        }

        std::atomic_ref<uint32_t> stateRef(target.state);
        if (snapshotState == RowBucketSnapshotState::kEmpty) {
          uint32_t expected = detail::kRowBucketEmpty;
          if (!stateRef.compare_exchange_strong(
                  expected, detail::kRowBucketWriting, std::memory_order_acq_rel,
                  std::memory_order_acquire)) {
            retry = true;
            break;
          }
          StoreRowBucketData(target, data);
          stateRef.store(detail::kRowBucketFilled, std::memory_order_release);
          inserted = true;
          return Status::Ok();
        }

        if (snapshotState == RowBucketSnapshotState::kWriting) {
          retry = true;
          break;
        }

        if (snapshot.columnHash != data.columnHash || snapshot.rowId != data.rowId) {
          continue;
        }

        bool matches = false;
        status = RowBlockMatches(snapshot, column, data.rowId, matches);
        if (!status) {
          return status;
        }
        if (!matches) {
          continue;
        }

        uint32_t expected = detail::kRowBucketFilled;
        if (!stateRef.compare_exchange_strong(
                expected, detail::kRowBucketWriting, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
          retry = true;
          break;
        }
        StoreRowBucketData(target, data);
        stateRef.store(detail::kRowBucketFilled, std::memory_order_release);
        inserted = false;
        return Status::Ok();
      }

      if (!retry) {
        return Status::Corruption("row index table is full");
      }
      std::this_thread::yield();
    }
  }

  Status FindRowBucket(std::string_view column, uint64_t rowId, uint64_t columnHash,
                       RowBucketLookup& lookup) const {
    const uint64_t bucketCount = RowIndexHeader()->bucketCount;
    const uint64_t mask = bucketCount - 1;
    const uint64_t start = detail::MixHashes(columnHash, rowId) & mask;

    for (;;) {
      bool retry = false;
      for (uint64_t probe = 0; probe < bucketCount; ++probe) {
        const uint64_t index = (start + probe) & mask;
        detail::RowIndexBucket bucket;
        RowBucketSnapshotState snapshotState = RowBucketSnapshotState::kEmpty;
        Status status = LoadRowBucketSnapshot(index, bucket, snapshotState);
        if (!status) {
          return status;
        }
        if (snapshotState == RowBucketSnapshotState::kEmpty) {
          lookup.index = index;
          lookup.found = false;
          return Status::Ok();
        }
        if (snapshotState == RowBucketSnapshotState::kWriting) {
          retry = true;
          break;
        }
        if (bucket.columnHash != columnHash || bucket.rowId != rowId) {
          continue;
        }

        bool matches = false;
        detail::RowBlockHeader blockHeader;
        status = RowBlockMatches(bucket, column, rowId, matches, &blockHeader);
        if (!status) {
          return status;
        }
        if (matches) {
          lookup.index = index;
          lookup.found = true;
          lookup.bucket = bucket;
          lookup.blockHeader = blockHeader;
          return Status::Ok();
        }
      }

      if (!retry) {
        return Status::Corruption("row index table is full");
      }
      std::this_thread::yield();
    }
  }

  Status RowBlockMatches(const detail::RowIndexBucket& bucket, std::string_view column,
                         uint64_t rowId, bool& matches,
                         detail::RowBlockHeader* matchingHeader = nullptr) const {
    matches = false;
    if (bucket.shardId >= rowValueFiles_.size()) {
      return Status::Corruption("row index points to an invalid shard");
    }

    detail::RowBlockHeader blockHeader;
    Status status = ReadRowValueAt(bucket.shardId, &blockHeader, sizeof(blockHeader),
                                   bucket.blockOffset);
    if (!status) {
      return status;
    }
    if (!IsValidRowBlockHeader(blockHeader) || blockHeader.columnHash != bucket.columnHash ||
        blockHeader.rowId != rowId || blockHeader.blockSize != bucket.blockSize) {
      return Status::Corruption("row index points to an invalid row block");
    }
    if (blockHeader.columnSize != column.size()) {
      return Status::Ok();
    }

    std::string storedColumn;
    status = ReadRowBlockColumn(bucket.shardId, bucket.blockOffset, blockHeader,
                                storedColumn);
    if (!status) {
      return status;
    }

    matches = storedColumn == column;
    if (matches && matchingHeader != nullptr) {
      *matchingHeader = blockHeader;
    }
    return Status::Ok();
  }

  uint32_t ShardForRow(uint64_t columnHash, uint64_t rowId) const {
    return static_cast<uint32_t>(
        detail::MixHashes(columnHash, rowId) % rowValueFiles_.size());
  }

  Status FindBucket(std::string_view column, std::string_view key, uint64_t columnHash,
                    uint64_t keyHash, BucketLookup& lookup) const {
    const uint64_t bucketCount = IndexHeader()->bucketCount;
    const uint64_t mask = bucketCount - 1;
    const uint64_t hash = ObjectIndexHash(columnHash, keyHash);
    const uint64_t start = hash & mask;

    for (uint64_t probe = 0; probe < bucketCount; ++probe) {
      const uint64_t index = (start + probe) & mask;
      const detail::IndexBucket& bucket = Buckets()[index];
      if (bucket.hash == 0) {
        lookup.index = index;
        lookup.found = false;
        return Status::Ok();
      }
      if (bucket.hash != hash) {
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
    Status status = ReadValueAt(&recordHeader, sizeof(recordHeader),
                                bucket.recordOffset);
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
  std::filesystem::path rowIndexPath_;
  std::filesystem::path objectWriteMarkerPath_;
  std::filesystem::path objectWriteMarkerTempPath_;
  DatabaseOptions options_;
  detail::FileDescriptor valueFile_;
  detail::FileDescriptor indexFile_;
  detail::FileDescriptor rowIndexFile_;
  detail::MappedFile valueMap_;
  detail::MappedFile indexMap_;
  detail::MappedFile rowIndexMap_;
  std::vector<detail::FileDescriptor> rowValueFiles_;
  std::vector<detail::MappedFile> rowValueMaps_;
  std::vector<std::unique_ptr<std::atomic<uint64_t>>> rowAppendOffsets_;
  std::vector<detail::IndexBucket> pendingUniqueBuckets_;
  std::vector<std::byte> objectWriteBuffer_;
  std::vector<uint64_t> objectBucketOccupancy_;
  std::atomic<uint64_t> rowPendingInsertCount_{0};
  uint64_t appendOffset_ = 0;
  uint64_t recoveredObjectBucketCount_ = 0;
  bool open_ = false;
  bool readOnly_ = false;
  bool objectBucketOccupancyValid_ = false;
  bool objectWriteMarkerActive_ = false;
  bool objectIndexConsistent_ = true;
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

Status Database::OpenReadOnly(const std::filesystem::path& directory,
                              const DatabaseOptions& options) {
  if (impl_ == nullptr) {
    impl_ = new Impl();
  }
  return impl_->OpenReadOnly(directory, options);
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

Status Database::PutStructs(std::string_view column,
                            std::span<const StructEntry> entries) {
  return impl_ == nullptr ? Status::NotOpen("database is not open")
                          : impl_->PutStructs(column, entries);
}

Status Database::PutUniqueStructs(std::string_view column,
                                  std::span<const StructEntry> entries) {
  return impl_ == nullptr ? Status::NotOpen("database is not open")
                          : impl_->PutUniqueStructs(column, entries);
}

Status Database::GetStruct(std::string_view column, std::string_view key,
                           std::vector<std::byte>& flatBufferBytes) const {
  return impl_ == nullptr ? Status::NotOpen("database is not open")
                          : impl_->GetStruct(column, key, flatBufferBytes);
}

Status Database::PutRowStruct(std::string_view column, uint64_t rowId,
                              std::string_view key,
                              std::span<const std::byte> flatBufferBytes) {
  return impl_ == nullptr ? Status::NotOpen("database is not open")
                          : impl_->PutRowStruct(column, rowId, key, flatBufferBytes);
}

Status Database::PutRowStructs(std::string_view column, uint64_t rowId,
                               std::span<const RowStructEntry> entries) {
  return impl_ == nullptr ? Status::NotOpen("database is not open")
                          : impl_->PutRowStructs(column, rowId, entries);
}

Status Database::GetRowStruct(std::string_view column, uint64_t rowId,
                              std::string_view key,
                              std::vector<std::byte>& flatBufferBytes) const {
  return impl_ == nullptr ? Status::NotOpen("database is not open")
                          : impl_->GetRowStruct(column, rowId, key, flatBufferBytes);
}

Status Database::GetMany(std::string_view column, const std::vector<std::string>& keys,
                         std::vector<std::vector<std::byte>>& values) const {
  return impl_ == nullptr ? Status::NotOpen("database is not open")
                          : impl_->GetMany(column, keys, values);
}

Status Database::GetColumnStats(std::vector<ColumnStats>& stats) const {
  return impl_ == nullptr ? Status::NotOpen("database is not open")
                          : impl_->GetColumnStats(stats);
}

Status Database::DumpColumnStats(std::ostream& output) const {
  return impl_ == nullptr ? Status::NotOpen("database is not open")
                          : impl_->DumpColumnStats(output);
}

Status Database::Dump(std::ostream& output) const {
  return impl_ == nullptr ? Status::NotOpen("database is not open")
                          : impl_->Dump(output);
}

Status Database::DumpColumn(std::ostream& output, std::string_view column,
                            const DumpDeserializer& deserialize) const {
  return impl_ == nullptr ? Status::NotOpen("database is not open")
                          : impl_->DumpColumn(output, column, deserialize);
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

uint64_t Database::RowCount() const {
  return impl_ == nullptr ? 0 : impl_->RowCount();
}

uint64_t Database::RowBucketCount() const {
  return impl_ == nullptr ? 0 : impl_->RowBucketCount();
}

}  // namespace LumoDB
