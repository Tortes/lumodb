#include "lumodb/database.h"

#include <algorithm>
#include <array>
#include <atomic>
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

template <typename T>
void AppendPod(std::vector<std::byte>& output, const T& value) {
  const auto* data = reinterpret_cast<const std::byte*>(&value);
  output.insert(output.end(), data, data + sizeof(T));
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
    rowPendingInsertCount_.store(0, std::memory_order_relaxed);
    options_.initialBucketCount = RoundUpPowerOfTwo(options_.initialBucketCount);
    options_.initialRowBucketCount = RoundUpPowerOfTwo(options_.initialRowBucketCount);
    if (options_.rowShardCount == 0) {
      return Status::InvalidArgument("rowShardCount must be greater than zero");
    }

    Status status = detail::EnsureDirectory(directory_);
    if (!status) {
      return status;
    }

    valuePath_ = directory_ / "values.lumov";
    indexPath_ = directory_ / "index.lumoi";
    rowIndexPath_ = directory_ / "row_index.lumori";

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
    return status;
  }

  Status Flush() {
    if (!indexMap_.IsMapped()) {
      return Status::Ok();
    }
    Status status = Status::Ok();
    status = rowIndexMap_.Sync();
    if (!status) {
      return status;
    }
    if (rowIndexFile_.IsValid()) {
      status = detail::SyncFile(rowIndexFile_.Get());
      if (!status) {
        return status;
      }
    }

    status = indexMap_.Sync();
    if (!status) {
      return status;
    }
    status = detail::SyncFile(indexFile_.Get());
    if (!status) {
      return status;
    }
    status = detail::SyncFile(valueFile_.Get());
    if (!status) {
      return status;
    }

    for (detail::FileDescriptor& rowValueFile : rowValueFiles_) {
      status = detail::SyncFile(rowValueFile.Get());
      if (!status) {
        return status;
      }
    }
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
    if (!open_) {
      return Status::NotOpen("database is not open");
    }
    if (!FitsUint32(column.size())) {
      return Status::InvalidArgument("column must fit in uint32 length");
    }
    if (entries.empty()) {
      return Status::Ok();
    }
    for (const StructEntry& entry : entries) {
      if (!FitsUint32(entry.key.size())) {
        return Status::InvalidArgument("key must fit in uint32 length");
      }
    }

    Status status = EnsureCapacityForInsert(entries.size());
    if (!status) {
      return status;
    }

    struct PendingRecord {
      detail::ValueRecordHeader header;
      std::string_view key;
      std::span<const std::byte> value;
      uint64_t recordOffset = 0;
      uint64_t valueOffset = 0;
      uint64_t recordSize = 0;
    };

    const uint64_t columnHash = detail::HashString(column);
    uint64_t nextSequence = IndexHeader()->nextSequence;
    uint64_t nextAppendOffset = appendOffset_;
    std::vector<PendingRecord> pending;
    pending.reserve(entries.size());
    std::vector<detail::WriteSlice> recordSlices;
    recordSlices.reserve(entries.size() * 4);

    for (const StructEntry& entry : entries) {
      PendingRecord& record = pending.emplace_back();
      record.key = entry.key;
      record.value = entry.flatBufferBytes;
      record.recordOffset = nextAppendOffset;
      record.valueOffset =
          record.recordOffset + sizeof(detail::ValueRecordHeader) + column.size() +
          entry.key.size();
      record.recordSize = sizeof(detail::ValueRecordHeader) + column.size() +
                          entry.key.size() + entry.flatBufferBytes.size();
      record.header.sequence = nextSequence++;
      record.header.columnHash = columnHash;
      record.header.keyHash = detail::HashString(entry.key);
      record.header.columnSize = static_cast<uint32_t>(column.size());
      record.header.keySize = static_cast<uint32_t>(entry.key.size());
      record.header.valueSize = entry.flatBufferBytes.size();

      recordSlices.push_back({.data = &record.header, .size = sizeof(record.header)});
      recordSlices.push_back({.data = column.data(), .size = column.size()});
      recordSlices.push_back({.data = entry.key.data(), .size = entry.key.size()});
      recordSlices.push_back(
          {.data = entry.flatBufferBytes.data(), .size = entry.flatBufferBytes.size()});
      nextAppendOffset += record.recordSize;
    }

    status = detail::WriteVAllAt(valueFile_.Get(), recordSlices, appendOffset_);
    if (!status) {
      return status;
    }

    for (const PendingRecord& record : pending) {
      BucketLookup lookup;
      status = FindBucket(column, record.key, columnHash, record.header.keyHash, lookup);
      if (!status) {
        return status;
      }

      detail::IndexBucket bucket;
      bucket.state = detail::kBucketFilled;
      bucket.columnHash = columnHash;
      bucket.keyHash = record.header.keyHash;
      bucket.recordOffset = record.recordOffset;
      bucket.valueOffset = record.valueOffset;
      bucket.valueSize = record.value.size();
      bucket.recordSize = record.recordSize;
      bucket.sequence = record.header.sequence;

      Buckets()[lookup.index] = bucket;
      if (!lookup.found) {
        ++IndexHeader()->itemCount;
      }
    }
    IndexHeader()->nextSequence = nextSequence;
    appendOffset_ = nextAppendOffset;
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

  Status PutRowStruct(std::string_view column, uint64_t rowId, std::string_view key,
                      std::span<const std::byte> flatBufferBytes) {
    if (!open_) {
      return Status::NotOpen("database is not open");
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
    std::vector<std::byte> block;
    status = BuildRowBlock(column, rowId, entries, sequence, columnHash, block);
    if (!status) {
      if (reserved) {
        ReleaseRowIndexInsertSlot();
      }
      return status;
    }

    const uint32_t shardId = ShardForRow(columnHash, rowId);
    const uint64_t blockOffset = rowAppendOffsets_[shardId]->fetch_add(
        static_cast<uint64_t>(block.size()), std::memory_order_relaxed);
    status = detail::WriteAllAt(rowValueFiles_[shardId].Get(), block.data(),
                                block.size(), blockOffset);
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
    bucket.blockSize = static_cast<uint64_t>(block.size());
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
    return FindValueInRowBlock(block, column, rowId, key, flatBufferBytes);
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
      if (bucket.state == detail::kBucketEmpty) {
        continue;
      }
      if (bucket.state != detail::kBucketFilled) {
        return Status::Corruption("object index bucket state is invalid");
      }

      detail::ValueRecordHeader recordHeader;
      Status status = detail::ReadAllAt(valueFile_.Get(), &recordHeader,
                                        sizeof(recordHeader), bucket.recordOffset);
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
      status = detail::ReadAllAt(rowValueFiles_[bucket.shardId].Get(), &blockHeader,
                                 sizeof(blockHeader), bucket.blockOffset);
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
    if (!open_) {
      return Status::NotOpen("database is not open");
    }

    output << "LumoDB dump\n";
    output << "object_index: entries=" << EntryCount()
           << " buckets=" << BucketCount()
           << " next_sequence=" << IndexHeader()->nextSequence << '\n';
    output << "objects:\n";
    for (uint64_t index = 0; index < IndexHeader()->bucketCount; ++index) {
      const detail::IndexBucket& bucket = Buckets()[index];
      if (bucket.state == detail::kBucketEmpty) {
        continue;
      }
      if (bucket.state != detail::kBucketFilled) {
        return Status::Corruption("object index bucket state is invalid");
      }

      detail::ValueRecordHeader recordHeader;
      Status status = detail::ReadAllAt(valueFile_.Get(), &recordHeader,
                                        sizeof(recordHeader), bucket.recordOffset);
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
      std::vector<std::byte> value(static_cast<size_t>(bucket.valueSize));
      status = detail::ReadAllAt(valueFile_.Get(), value.data(), value.size(),
                                 bucket.valueOffset);
      if (!status) {
        return status;
      }

      output << "  object bucket=" << index
             << " column=" << EscapeText(column)
             << " key=" << EscapeText(key)
             << " sequence=" << bucket.sequence
             << " column_hash=" << bucket.columnHash
             << " key_hash=" << bucket.keyHash
             << " record_offset=" << bucket.recordOffset
             << " value_offset=" << bucket.valueOffset
             << " value_size=" << bucket.valueSize
             << " record_size=" << bucket.recordSize
             << " value_hex=" << HexBytes(value) << '\n';
    }

    output << "row_index: rows=" << RowCount()
           << " buckets=" << RowBucketCount()
           << " shards=" << rowValueFiles_.size()
           << " next_sequence="
           << AtomicLoad(RowIndexHeader()->nextSequence, std::memory_order_acquire) << '\n';
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
        output << "    key=" << EscapeText(keys[entry])
               << " value_size=" << values[entry].size()
               << " value_hex=" << HexBytes(values[entry]) << '\n';
      }
    }

    output << "column_statistics:\n";
    std::vector<ColumnStats> stats;
    Status status = GetColumnStats(stats);
    if (!status) {
      return status;
    }
    for (const ColumnStats& columnStats : stats) {
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
  };

  struct RowBucketData {
    uint64_t columnHash = 0;
    uint64_t rowId = 0;
    uint32_t shardId = 0;
    uint64_t blockOffset = 0;
    uint64_t blockSize = 0;
    uint64_t sequence = 0;
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

  void CloseFiles() {
    rowIndexMap_.Unmap();
    rowIndexFile_.Reset();
    rowValueFiles_.clear();
    rowAppendOffsets_.clear();
    rowPendingInsertCount_.store(0, std::memory_order_relaxed);
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

  Status OpenRowValueFiles() {
    rowValueFiles_.clear();
    rowAppendOffsets_.clear();
    rowValueFiles_.resize(options_.rowShardCount);
    rowAppendOffsets_.reserve(options_.rowShardCount);
    for (uint32_t shardId = 0; shardId < options_.rowShardCount; ++shardId) {
      rowAppendOffsets_.push_back(std::make_unique<std::atomic<uint64_t>>(0));
    }

    for (uint32_t shardId = 0; shardId < options_.rowShardCount; ++shardId) {
      Status status =
          detail::OpenReadWriteCreate(RowValuePath(directory_, shardId),
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
    }

    return Status::Ok();
  }

  Status OpenRowIndexFile() {
    Status status = detail::OpenReadWriteCreate(rowIndexPath_, rowIndexFile_);
    if (!status) {
      return status;
    }

    uint64_t fileSize = 0;
    status = detail::GetFileSize(rowIndexFile_.Get(), fileSize);
    if (!status) {
      return status;
    }

    bool shouldRebuild = fileSize == 0;
    if (!shouldRebuild) {
      status = rowIndexMap_.Map(rowIndexFile_.Get(), fileSize);
      if (!status) {
        return status;
      }
      shouldRebuild = !HasValidRowIndexLayout(fileSize);
      if (shouldRebuild) {
        rowIndexMap_.Unmap();
      }
    }

    if (shouldRebuild) {
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
    return detail::ReadAllAt(rowValueFiles_[shardId].Get(), column.data(),
                             column.size(), columnOffset);
  }

  Status LoadRowBlock(const detail::RowIndexBucket& bucket,
                      std::vector<std::byte>& block) const {
    if (bucket.shardId >= rowValueFiles_.size()) {
      return Status::Corruption("row index points to an invalid shard");
    }
    block.resize(static_cast<size_t>(bucket.blockSize));
    return detail::ReadAllAt(rowValueFiles_[bucket.shardId].Get(), block.data(),
                             block.size(), bucket.blockOffset);
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

  Status BuildRowBlock(std::string_view column, uint64_t rowId,
                       std::span<const RowStructEntry> entries, uint64_t sequence,
                       uint64_t columnHash, std::vector<std::byte>& block) const {
    if (entries.size() > std::numeric_limits<uint32_t>::max()) {
      return Status::InvalidArgument("row entry count must fit in uint32");
    }

    const uint64_t bucketCount = RoundUpPowerOfTwo(
        static_cast<uint64_t>(static_cast<double>(entries.size()) / options_.maxLoadFactor) +
        1);
    if (bucketCount > std::numeric_limits<uint32_t>::max()) {
      return Status::InvalidArgument("row key bucket count must fit in uint32");
    }

    std::vector<detail::RowKeyBucket> keyBuckets(static_cast<size_t>(bucketCount));
    std::vector<std::byte> keyBytes;
    std::vector<std::byte> valueBytes;
    uint32_t itemCount = 0;

    for (const RowStructEntry& entry : entries) {
      const uint64_t keyHash = detail::HashString(entry.key);
      const uint64_t mask = bucketCount - 1;
      const uint64_t start = detail::MixHashes(keyHash, rowId) & mask;
      bool placed = false;

      for (uint64_t probe = 0; probe < bucketCount; ++probe) {
        detail::RowKeyBucket& bucket = keyBuckets[(start + probe) & mask];
        if (bucket.state == detail::kBucketEmpty) {
          const uint64_t keyOffset = keyBytes.size();
          const uint64_t valueOffset = valueBytes.size();
          AppendBytes(keyBytes, entry.key.data(), entry.key.size());
          AppendBytes(valueBytes, entry.flatBufferBytes.data(),
                      entry.flatBufferBytes.size());

          bucket.state = detail::kBucketFilled;
          bucket.keyHash = keyHash;
          bucket.keyOffset = keyOffset;
          bucket.keySize = entry.key.size();
          bucket.valueOffset = valueOffset;
          bucket.valueSize = entry.flatBufferBytes.size();
          ++itemCount;
          placed = true;
          break;
        }

        if (bucket.keyHash != keyHash || bucket.keySize != entry.key.size()) {
          continue;
        }
        const auto* storedKey =
            reinterpret_cast<const char*>(keyBytes.data() + bucket.keyOffset);
        if (std::string_view(storedKey, static_cast<size_t>(bucket.keySize)) !=
            entry.key) {
          continue;
        }

        const uint64_t valueOffset = valueBytes.size();
        AppendBytes(valueBytes, entry.flatBufferBytes.data(), entry.flatBufferBytes.size());
        bucket.valueOffset = valueOffset;
        bucket.valueSize = entry.flatBufferBytes.size();
        placed = true;
        break;
      }

      if (!placed) {
        return Status::Corruption("row key table is full");
      }
    }

    detail::RowBlockHeader blockHeader;
    blockHeader.sequence = sequence;
    blockHeader.columnHash = columnHash;
    blockHeader.rowId = rowId;
    blockHeader.columnSize = static_cast<uint32_t>(column.size());
    blockHeader.itemCount = itemCount;
    blockHeader.bucketCount = static_cast<uint32_t>(bucketCount);
    blockHeader.keyBytesSize = keyBytes.size();
    blockHeader.valueBytesSize = valueBytes.size();
    blockHeader.blockSize =
        sizeof(detail::RowBlockHeader) + bucketCount * sizeof(detail::RowKeyBucket) +
        column.size() + keyBytes.size() + valueBytes.size();

    block.clear();
    block.reserve(static_cast<size_t>(blockHeader.blockSize));
    AppendPod(block, blockHeader);
    AppendBytes(block, keyBuckets.data(), keyBuckets.size() * sizeof(detail::RowKeyBucket));
    AppendBytes(block, column.data(), column.size());
    AppendBytes(block, keyBytes.data(), keyBytes.size());
    AppendBytes(block, valueBytes.data(), valueBytes.size());
    return Status::Ok();
  }

  Status FindValueInRowBlock(const std::vector<std::byte>& block,
                             std::string_view column, uint64_t rowId,
                             std::string_view key,
                             std::vector<std::byte>& flatBufferBytes) const {
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
    const uint64_t keyHash = detail::HashString(key);
    const uint64_t mask = header->bucketCount - 1;
    const uint64_t start = detail::MixHashes(keyHash, rowId) & mask;

    for (uint64_t probe = 0; probe < header->bucketCount; ++probe) {
      const detail::RowKeyBucket& bucket = keyBuckets[(start + probe) & mask];
      if (bucket.state == detail::kBucketEmpty) {
        return Status::NotFound("key not found in row");
      }
      if (bucket.keyHash != keyHash || bucket.keySize != key.size()) {
        continue;
      }
      if (bucket.keyOffset + bucket.keySize > header->keyBytesSize ||
          bucket.valueOffset + bucket.valueSize > header->valueBytesSize) {
        return Status::Corruption("row key bucket points outside block");
      }

      const auto* storedKey = reinterpret_cast<const char*>(
          block.data() + keyBytesOffset + bucket.keyOffset);
      if (std::string_view(storedKey, static_cast<size_t>(bucket.keySize)) != key) {
        continue;
      }

      const std::byte* valueBegin =
          block.data() + valueBytesOffset + bucket.valueOffset;
      flatBufferBytes.assign(valueBegin, valueBegin + bucket.valueSize);
      return Status::Ok();
    }

    return Status::NotFound("key not found in row");
  }

  Status EnsureCapacityForInsert(uint64_t insertCount = 1) {
    const detail::IndexFileHeader* header = IndexHeader();
    uint64_t bucketCount = header->bucketCount;
    while (static_cast<double>(header->itemCount + insertCount) /
               static_cast<double>(bucketCount) >
           options_.maxLoadFactor) {
      bucketCount *= 2;
    }
    if (bucketCount == header->bucketCount) {
      return Status::Ok();
    }
    return ResizeIndex(bucketCount);
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
        status = RowBlockMatches(bucket, column, rowId, matches);
        if (!status) {
          return status;
        }
        if (matches) {
          lookup.index = index;
          lookup.found = true;
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
                         uint64_t rowId, bool& matches) const {
    matches = false;
    if (bucket.shardId >= rowValueFiles_.size()) {
      return Status::Corruption("row index points to an invalid shard");
    }

    detail::RowBlockHeader blockHeader;
    Status status = detail::ReadAllAt(rowValueFiles_[bucket.shardId].Get(), &blockHeader,
                                      sizeof(blockHeader), bucket.blockOffset);
    if (!status) {
      return status;
    }
    if (!IsValidRowBlockHeader(blockHeader) || blockHeader.rowId != rowId ||
        blockHeader.blockSize != bucket.blockSize) {
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
  std::filesystem::path rowIndexPath_;
  DatabaseOptions options_;
  detail::FileDescriptor valueFile_;
  detail::FileDescriptor indexFile_;
  detail::FileDescriptor rowIndexFile_;
  detail::MappedFile indexMap_;
  detail::MappedFile rowIndexMap_;
  std::vector<detail::FileDescriptor> rowValueFiles_;
  std::vector<std::unique_ptr<std::atomic<uint64_t>>> rowAppendOffsets_;
  std::atomic<uint64_t> rowPendingInsertCount_{0};
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

Status Database::PutStructs(std::string_view column,
                            std::span<const StructEntry> entries) {
  return impl_ == nullptr ? Status::NotOpen("database is not open")
                          : impl_->PutStructs(column, entries);
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
