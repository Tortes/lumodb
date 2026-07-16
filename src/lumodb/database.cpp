#include "lumodb/database.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <limits>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "lumodb/file.h"
#include "lumodb/hash.h"
#include "lumodb/storage_format.h"

namespace LumoDB {
namespace {

constexpr uint64_t kMinimumIndexBucketCount = 16;
constexpr uint32_t kMaximumWriterThreads = 32;
constexpr uint32_t kMaximumRowShards = 64;
constexpr uint32_t kMaximumSpillPartitions = 128;
constexpr uint64_t kMinimumMemoryBudget = 64ULL * 1024 * 1024;
constexpr uint32_t kMinimumStageBuffer = 4 * 1024;
constexpr uint32_t kMaximumStageBuffer = 16 * 1024 * 1024;
constexpr uint32_t kMaximumDedupPresizeEntries = 64 * 1024;
constexpr double kRowLoadFactor = 0.75;
constexpr uint64_t kExplicitRowBit = 1ULL << 63;
constexpr std::string_view kIncompleteMarkerName = "build.incomplete";
constexpr std::string_view kIncompleteMarkerTempName = "build.incomplete.tmp";

template <typename T>
bool MagicEquals(const std::array<char, 8>& lhs, const T& rhs) {
  return std::equal(lhs.begin(), lhs.end(), rhs.begin());
}

uint64_t NormalizeHash(uint64_t hash) { return hash == 0 ? 1 : hash; }

uint64_t RoundUpPowerOfTwo(uint64_t value, uint64_t minimum = 1) {
  value = std::max(value, minimum);
  if (value > (1ULL << 63)) {
    return 0;
  }
  return std::bit_ceil(value);
}

bool CheckedAdd(uint64_t lhs, uint64_t rhs, uint64_t& result) {
  if (lhs > std::numeric_limits<uint64_t>::max() - rhs) {
    return false;
  }
  result = lhs + rhs;
  return true;
}

bool CheckedMultiply(uint64_t lhs, uint64_t rhs, uint64_t& result) {
  if (lhs != 0 && rhs > std::numeric_limits<uint64_t>::max() / lhs) {
    return false;
  }
  result = lhs * rhs;
  return true;
}

uint64_t DivideRoundUp(uint64_t value, uint64_t divisor) {
  return value / divisor + (value % divisor != 0 ? 1 : 0);
}

uint64_t AlignUp8(uint64_t value) { return (value + 7) & ~uint64_t{7}; }

uint32_t RoutingTargetEntries(uint32_t rowCapacity) {
  const uint32_t standardDeviation =
      static_cast<uint32_t>(std::ceil(std::sqrt(static_cast<double>(rowCapacity))));
  const uint32_t distributionHeadroom = standardDeviation * 4;
  return rowCapacity > distributionHeadroom ? rowCapacity - distributionHeadroom : 1;
}

std::span<const std::byte> AsBytes(std::string_view value) {
  return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

std::filesystem::path RowValuePath(const std::filesystem::path& directory, uint32_t shardId) {
  std::ostringstream name;
  name << "row_values-" << std::setw(3) << std::setfill('0') << shardId << ".lumorv";
  return directory / name.str();
}

std::filesystem::path StagePath(const std::filesystem::path& directory, uint32_t partitionId) {
  std::ostringstream name;
  name << "stage-" << std::setw(3) << std::setfill('0') << partitionId << ".lumost";
  return directory / name.str();
}

uint32_t ResolvedWriterThreads(const DatabaseOptions& options) {
  if (options.writerThreadCount != 0) {
    return std::min(options.writerThreadCount, kMaximumWriterThreads);
  }
  const uint32_t hardware = std::thread::hardware_concurrency();
  return std::clamp(hardware == 0 ? 1U : hardware, 1U, kMaximumWriterThreads);
}

void AppendRaw(std::vector<std::byte>& output, const void* data, size_t size) {
  if (size == 0) {
    return;
  }
  const size_t offset = output.size();
  output.resize(offset + size);
  std::memcpy(output.data() + offset, data, size);
}

}  // namespace

class Database::Impl {
 public:
  ~Impl() { static_cast<void>(Close()); }

  Status Open(const std::filesystem::path& directory, const DatabaseOptions& options) {
    return OpenInternal(directory, options, false);
  }

  Status OpenReadOnly(const std::filesystem::path& directory) {
    return OpenInternal(directory, DatabaseOptions{}, true);
  }

  Status Close() {
    std::unique_lock lifecycleLock(lifecycleMutex_);
    if (!open_) {
      CloseFiles();
      return Status::Ok();
    }

    Status status = Status::Ok();
    if (!readOnly_ && !writePoisoned_.load(std::memory_order_acquire)) {
      status = FlushLocked();
    } else if (writePoisoned_.load(std::memory_order_acquire)) {
      status = Status::Corruption("write batch failed; delete the output directory and rebuild it");
    }
    CloseFiles();
    open_ = false;
    readOnly_ = false;
    return status;
  }

  Status Flush() {
    std::unique_lock lifecycleLock(lifecycleMutex_);
    if (!open_) {
      return Status::NotOpen("database is not open");
    }
    return FlushLocked();
  }

  Status Put(std::string_view column, std::string_view key, std::span<const std::byte> value) {
    return PutInternal(column, 0, false, key, value);
  }

  Status Put(std::string_view column, uint64_t rowId, std::string_view key,
             std::span<const std::byte> value) {
    return PutInternal(column, rowId, true, key, value);
  }

  Status PutInternal(std::string_view column, uint64_t requestedRowId, bool explicitRow,
                     std::string_view key, std::span<const std::byte> value) {
    std::shared_lock lifecycleLock(lifecycleMutex_);
    if (!open_) {
      return Status::NotOpen("database is not open");
    }
    if (readOnly_) {
      return Status::InvalidArgument("database is read-only");
    }
    if (writePoisoned_.load(std::memory_order_acquire)) {
      return Status::Corruption("write batch failed; delete the output directory and rebuild it");
    }
    if (column.empty()) {
      return Status::InvalidArgument("column must not be empty");
    }
    if (key.empty()) {
      return Status::InvalidArgument("key must not be empty");
    }
    if (explicitRow && requestedRowId >= kExplicitRowBit) {
      return Status::InvalidArgument("explicit rowId must be smaller than 2^63");
    }
    if (column.size() > std::numeric_limits<uint32_t>::max() ||
        key.size() > std::numeric_limits<uint32_t>::max() ||
        value.size() > std::numeric_limits<uint32_t>::max()) {
      return Status::InvalidArgument("column, key, and value must each be smaller than 4 GiB");
    }

    Status status = BeginBuild();
    if (!status) {
      writePoisoned_.store(true, std::memory_order_release);
      return status;
    }

    const uint64_t columnHash = NormalizeHash(detail::HashString(column));
    const uint64_t keyHash = NormalizeHash(detail::HashString(key));
    uint64_t rowId = requestedRowId | kExplicitRowBit;
    if (!explicitRow) {
      const uint64_t routeHash =
          detail::MixHashes(detail::MixHashes(columnHash, keyHash), RoutingSeed());
      rowId = routeHash % routeCount_;
    }
    const uint32_t partitionId =
        static_cast<uint32_t>(detail::MixHashes(columnHash, rowId) % spillPartitions_.size());

    detail::StageRecordHeader record;
    record.columnHash = columnHash;
    record.keyHash = keyHash;
    record.rowId = rowId;
    record.columnSize = static_cast<uint32_t>(column.size());
    record.keySize = static_cast<uint32_t>(key.size());
    record.valueSize = static_cast<uint32_t>(value.size());

    uint64_t recordSize = sizeof(record);
    if (!CheckedAdd(recordSize, column.size(), recordSize) ||
        !CheckedAdd(recordSize, key.size(), recordSize) ||
        !CheckedAdd(recordSize, value.size(), recordSize) ||
        recordSize > std::numeric_limits<size_t>::max()) {
      return Status::InvalidArgument("staged record is too large");
    }

    SpillPartition& partition = *spillPartitions_[partitionId];
    std::lock_guard partitionLock(partition.mutex);
    status = EnsureStageFileOpen(partitionId, partition);
    if (!status) {
      writePoisoned_.store(true, std::memory_order_release);
      return status;
    }

    if (!partition.buffer.empty() &&
        partition.buffer.size() + recordSize > options_.stageBufferBytes) {
      status = FlushStageBuffer(partition);
      if (!status) {
        writePoisoned_.store(true, std::memory_order_release);
        return status;
      }
    }

    if (recordSize > options_.stageBufferBytes) {
      std::vector<std::byte> packed;
      packed.reserve(static_cast<size_t>(recordSize));
      AppendRaw(packed, &record, sizeof(record));
      AppendRaw(packed, column.data(), column.size());
      AppendRaw(packed, key.data(), key.size());
      AppendRaw(packed, value.data(), value.size());
      status = detail::WriteAllAt(partition.file.Get(), packed.data(), packed.size(),
                                  partition.persistedSize);
      if (status) {
        partition.persistedSize += packed.size();
      }
    } else {
      partition.buffer.reserve(options_.stageBufferBytes);
      AppendRaw(partition.buffer, &record, sizeof(record));
      AppendRaw(partition.buffer, column.data(), column.size());
      AppendRaw(partition.buffer, key.data(), key.size());
      AppendRaw(partition.buffer, value.data(), value.size());
    }
    if (!status) {
      writePoisoned_.store(true, std::memory_order_release);
      return status;
    }

    stagedEntryCount_.fetch_add(1, std::memory_order_relaxed);
    return Status::Ok();
  }

  Status Get(std::string_view column, std::string_view key, std::vector<std::byte>& value) const {
    return GetInternal(column, 0, false, key, value);
  }

  Status Get(std::string_view column, uint64_t rowId, std::string_view key,
             std::vector<std::byte>& value) const {
    return GetInternal(column, rowId, true, key, value);
  }

  Status GetInternal(std::string_view column, uint64_t requestedRowId, bool explicitRow,
                     std::string_view key, std::vector<std::byte>& value) const {
    std::shared_lock lifecycleLock(lifecycleMutex_);
    value.clear();
    if (!open_) {
      return Status::NotOpen("database is not open");
    }
    if (buildActive_.load(std::memory_order_acquire)) {
      return Status::InvalidArgument("uncommitted writes exist; call Flush before Get");
    }
    if (column.empty() || key.empty()) {
      return Status::InvalidArgument("column and key must not be empty");
    }
    if (explicitRow && requestedRowId >= kExplicitRowBit) {
      return Status::InvalidArgument("explicit rowId must be smaller than 2^63");
    }

    const uint64_t columnHash = NormalizeHash(detail::HashString(column));
    const uint64_t keyHash = NormalizeHash(detail::HashString(key));
    uint64_t rowId = requestedRowId | kExplicitRowBit;
    if (!explicitRow) {
      const uint64_t routeHash =
          detail::MixHashes(detail::MixHashes(columnHash, keyHash), RoutingSeed());
      rowId = routeHash % routeCount_;
    }

    RowLookup lookup;
    Status status = FindRowBucket(column, columnHash, rowId, lookup);
    if (!status) {
      return status;
    }
    if (!lookup.found) {
      return Status::NotFound("key not found");
    }

    const uint64_t bucketOffset =
        AlignUp8(sizeof(detail::RowBlockHeader) + lookup.header.columnSize);
    const uint64_t keyBytesOffset =
        bucketOffset +
        static_cast<uint64_t>(lookup.header.bucketCount) * sizeof(detail::RowKeyBucket);
    const uint64_t valueBytesOffset = keyBytesOffset + lookup.header.keyBytesSize;
    const uint64_t mask = lookup.header.bucketCount - 1;
    const uint64_t start = keyHash & mask;

    for (uint64_t probe = 0; probe < lookup.header.bucketCount; ++probe) {
      const uint64_t bucketIndex = (start + probe) & mask;
      detail::RowKeyBucket bucket;
      status = ReadRowAt(lookup.bucket.shardId, &bucket, sizeof(bucket),
                         lookup.bucket.blockOffset + bucketOffset + bucketIndex * sizeof(bucket));
      if (!status) {
        return status;
      }
      if (bucket.keyHash == 0) {
        return Status::NotFound("key not found");
      }
      if (bucket.keyHash != keyHash || bucket.keySize != key.size()) {
        continue;
      }
      if (bucket.keyOffset > lookup.header.keyBytesSize ||
          bucket.keySize > lookup.header.keyBytesSize - bucket.keyOffset ||
          bucket.valueOffset > lookup.header.valueBytesSize ||
          bucket.valueSize > lookup.header.valueBytesSize - bucket.valueOffset) {
        return Status::Corruption("row bucket points outside its block");
      }

      std::string storedKey(bucket.keySize, '\0');
      status = ReadRowAt(lookup.bucket.shardId, storedKey.data(), storedKey.size(),
                         lookup.bucket.blockOffset + keyBytesOffset + bucket.keyOffset);
      if (!status) {
        return status;
      }
      if (storedKey != key) {
        continue;
      }

      value.resize(bucket.valueSize);
      return ReadRowAt(lookup.bucket.shardId, value.data(), value.size(),
                       lookup.bucket.blockOffset + valueBytesOffset + bucket.valueOffset);
    }
    return Status::NotFound("key not found");
  }

  [[nodiscard]] bool IsOpen() const {
    std::shared_lock lifecycleLock(lifecycleMutex_);
    return open_;
  }

  [[nodiscard]] bool HasUncommittedWrites() const {
    return buildActive_.load(std::memory_order_acquire);
  }

  [[nodiscard]] uint64_t EntryCount() const {
    std::shared_lock lifecycleLock(lifecycleMutex_);
    return open_ && indexMap_.IsMapped() ? IndexHeader()->entryCount : 0;
  }

  [[nodiscard]] uint64_t RowCount() const {
    std::shared_lock lifecycleLock(lifecycleMutex_);
    return open_ && indexMap_.IsMapped() ? IndexHeader()->itemCount : 0;
  }

  [[nodiscard]] DatabaseLayout Layout() const {
    std::shared_lock lifecycleLock(lifecycleMutex_);
    DatabaseLayout layout;
    if (!open_ || !indexMap_.IsMapped()) {
      return layout;
    }
    const detail::RowIndexFileHeader& header = *IndexHeader();
    layout.expectedEntryCountPerColumn = header.expectedEntryCountPerColumn;
    layout.expectedExplicitRowCount = header.expectedExplicitRowCount;
    layout.routeCountPerColumn = header.routeCount;
    layout.targetEntriesPerRow = header.targetEntriesPerRow;
    layout.rowShardCount = header.shardCount;
    layout.spillPartitionCount = header.spillPartitionCount;
    layout.rowIndexBucketCount = header.bucketCount;
    return layout;
  }

 private:
  struct RowShard {
    detail::FileDescriptor file;
    detail::MappedFile map;
    std::mutex mutex;
    uint64_t appendOffset = 0;
  };

  struct SpillPartition {
    detail::FileDescriptor file;
    std::mutex mutex;
    std::vector<std::byte> buffer;
    uint64_t persistedSize = 0;
    bool used = false;
  };

  struct EntryView {
    std::string_view key;
    std::span<const std::byte> value;
    uint64_t keyHash = 0;
  };

  struct OwnedEntry {
    std::string key;
    std::vector<std::byte> value;
  };

  struct RowLookup {
    bool found = false;
    uint64_t index = 0;
    detail::RowIndexBucket bucket;
    detail::RowBlockHeader header;
  };

  struct StageGroupKey {
    uint64_t columnHash = 0;
    uint64_t rowId = 0;
    std::string_view column;

    bool operator==(const StageGroupKey& other) const {
      return columnHash == other.columnHash && rowId == other.rowId && column == other.column;
    }
  };

  struct StageGroupKeyHash {
    size_t operator()(const StageGroupKey& key) const {
      return static_cast<size_t>(detail::MixHashes(key.columnHash, key.rowId));
    }
  };

  struct StageGroup {
    std::string_view column;
    uint64_t columnHash = 0;
    uint64_t rowId = 0;
    std::vector<EntryView> entries;
  };

  struct TemporaryKeySlot {
    uint64_t keyHash = 0;
    size_t entryIndex = 0;
  };

  Status OpenInternal(const std::filesystem::path& directory, const DatabaseOptions& options,
                      bool readOnly) {
    {
      std::unique_lock lifecycleLock(lifecycleMutex_);
      if (open_) {
        return Status::InvalidArgument("close the database before opening another directory");
      }
      CloseFiles();
    }

    Status status = ValidateRuntimeOptions(options);
    if (!status) {
      return status;
    }

    directory_ = directory;
    options_ = options;
    readOnly_ = readOnly;
    writePoisoned_.store(false, std::memory_order_release);
    buildActive_.store(false, std::memory_order_release);
    stagedEntryCount_.store(0, std::memory_order_release);
    indexPath_ = directory_ / "row_index.lumori";
    markerPath_ = directory_ / kIncompleteMarkerName;
    markerTempPath_ = directory_ / kIncompleteMarkerTempName;

    if (readOnly_) {
      std::error_code error;
      if (!std::filesystem::is_directory(directory_, error)) {
        return Status::InvalidArgument("database directory does not exist: " + directory_.string());
      }
    } else {
      status = detail::EnsureDirectory(directory_);
      if (!status) {
        return status;
      }
    }

    status = RejectIncompleteBuildAndCleanStaging();
    if (!status) {
      CloseFiles();
      return status;
    }
    status = OpenIndexFile();
    if (!status) {
      CloseFiles();
      return status;
    }
    CacheRoutingConfiguration();
    status = OpenRowFiles();
    if (!status) {
      CloseFiles();
      return status;
    }
    if (!readOnly_) {
      spillPartitions_.reserve(spillPartitionCount_);
      for (uint32_t index = 0; index < spillPartitionCount_; ++index) {
        spillPartitions_.push_back(std::make_unique<SpillPartition>());
      }
    }
    nextSequence_.store(IndexHeader()->nextSequence, std::memory_order_release);
    open_ = true;
    return Status::Ok();
  }

  Status ValidateRuntimeOptions(const DatabaseOptions& options) const {
    if (options.writerThreadCount > kMaximumWriterThreads) {
      return Status::InvalidArgument("writerThreadCount must be <= 32");
    }
    if (options.rowShardCount > kMaximumRowShards) {
      return Status::InvalidArgument("rowShardCount must be <= 64");
    }
    if (options.spillPartitionCount > kMaximumSpillPartitions) {
      return Status::InvalidArgument("spillPartitionCount must be <= 128");
    }
    if (options.spillPartitionCount != 0 && !std::has_single_bit(options.spillPartitionCount)) {
      return Status::InvalidArgument("spillPartitionCount must be a power of two");
    }
    if (options.memoryBudgetBytes < kMinimumMemoryBudget) {
      return Status::InvalidArgument("memoryBudgetBytes must be at least 64 MiB");
    }
    if (options.stageBufferBytes < kMinimumStageBuffer ||
        options.stageBufferBytes > kMaximumStageBuffer) {
      return Status::InvalidArgument("stageBufferBytes must be between 4 KiB and 16 MiB");
    }
    if (!std::isfinite(options.maxLoadFactor) || options.maxLoadFactor <= 0.50 ||
        options.maxLoadFactor >= 0.95) {
      return Status::InvalidArgument("maxLoadFactor must be in (0.50, 0.95)");
    }
    return Status::Ok();
  }

  Status ConfigureNewDatabase(detail::RowIndexFileHeader& header) const {
    if (options_.expectedEntryCountPerColumn == 0) {
      return Status::InvalidArgument("expectedEntryCountPerColumn is required for a new database");
    }
    if (options_.expectedAutomaticColumnCount == 0) {
      return Status::InvalidArgument("expectedAutomaticColumnCount must be greater than zero");
    }
    if (options_.maxEntriesPerRow == 0) {
      return Status::InvalidArgument("maxEntriesPerRow must be greater than zero");
    }
    if (options_.targetRowBytes < 1024 * 1024) {
      return Status::InvalidArgument("targetRowBytes must be at least 1 MiB");
    }

    const uint32_t targetEntries = ChooseTargetEntries();
    if (targetEntries == 0) {
      return Status::InvalidArgument(
          "row target is too small for the configured average value size");
    }
    const uint64_t baseRouteCount =
        DivideRoundUp(options_.expectedEntryCountPerColumn, RoutingTargetEntries(targetEntries));
    if (baseRouteCount >= kExplicitRowBit) {
      return Status::InvalidArgument("automatic row count exceeds the supported range");
    }
    const uint32_t writers = ResolvedWriterThreads(options_);

    uint32_t spillCount = options_.spillPartitionCount;
    if (spillCount == 0) {
      uint64_t bytesPerEntry = sizeof(detail::StageRecordHeader) + 16;
      if (!CheckedAdd(bytesPerEntry, options_.averageKeyBytes, bytesPerEntry) ||
          !CheckedAdd(bytesPerEntry, options_.averageValueBytes, bytesPerEntry)) {
        bytesPerEntry = std::numeric_limits<uint64_t>::max();
      }
      uint64_t estimatedEntries = 0;
      if (!CheckedMultiply(options_.expectedEntryCountPerColumn,
                           options_.expectedAutomaticColumnCount, estimatedEntries) ||
          !CheckedAdd(estimatedEntries, options_.expectedExplicitEntryCount, estimatedEntries)) {
        estimatedEntries = std::numeric_limits<uint64_t>::max();
      }
      uint64_t estimatedBytes = 0;
      if (!CheckedMultiply(estimatedEntries, bytesPerEntry, estimatedBytes)) {
        estimatedBytes = std::numeric_limits<uint64_t>::max();
      }
      const uint64_t desiredPartitionBytes = std::max<uint64_t>(
          64ULL * 1024 * 1024, options_.memoryBudgetBytes / (static_cast<uint64_t>(writers) * 3));
      const uint64_t required =
          std::max<uint64_t>(1, DivideRoundUp(estimatedBytes, desiredPartitionBytes));
      uint64_t automatic = RoundUpPowerOfTwo(required);
      automatic = std::max<uint64_t>(automatic, std::min<uint64_t>(writers, baseRouteCount));
      automatic = RoundUpPowerOfTwo(automatic);
      automatic = std::min<uint64_t>(automatic, kMaximumSpillPartitions);
      spillCount = static_cast<uint32_t>(std::max<uint64_t>(1, automatic));
    }

    const uint64_t routeCount = baseRouteCount;
    uint64_t expectedRows = 0;
    if (!CheckedMultiply(routeCount, options_.expectedAutomaticColumnCount, expectedRows) ||
        !CheckedAdd(expectedRows, options_.expectedExplicitRowCount, expectedRows)) {
      return Status::InvalidArgument("configured row count overflows uint64");
    }
    const long double bucketsNeeded =
        static_cast<long double>(expectedRows) / options_.maxLoadFactor + 1;
    if (bucketsNeeded > static_cast<long double>(1ULL << 63)) {
      return Status::InvalidArgument("row index is too large");
    }
    const uint64_t bucketCount =
        RoundUpPowerOfTwo(static_cast<uint64_t>(bucketsNeeded), kMinimumIndexBucketCount);
    if (bucketCount == 0) {
      return Status::InvalidArgument("row index is too large");
    }

    uint32_t shardCount = options_.rowShardCount;
    if (shardCount == 0) {
      shardCount = std::min<uint32_t>(writers, 32);
    }
    shardCount = std::max<uint32_t>(1, shardCount);

    header.bucketCount = bucketCount;
    header.routeCount = routeCount;
    header.expectedEntryCountPerColumn = options_.expectedEntryCountPerColumn;
    header.targetEntriesPerRow = targetEntries;
    header.shardCount = shardCount;
    header.spillPartitionCount = spillCount;
    header.expectedAutomaticColumnCount = options_.expectedAutomaticColumnCount;
    header.expectedExplicitRowCount = options_.expectedExplicitRowCount;
    return Status::Ok();
  }

  uint32_t ChooseTargetEntries() const {
    uint32_t best = 0;
    for (uint64_t bucketCount = 2; bucketCount <= (1ULL << 31) && best < options_.maxEntriesPerRow;
         bucketCount <<= 1) {
      const uint64_t candidate = std::min<uint64_t>(
          options_.maxEntriesPerRow, static_cast<uint64_t>(bucketCount * kRowLoadFactor));
      uint64_t estimated = sizeof(detail::RowBlockHeader) + 32;
      uint64_t bucketBytes = 0;
      uint64_t payloadBytes = 0;
      if (!CheckedMultiply(bucketCount, sizeof(detail::RowKeyBucket), bucketBytes) ||
          !CheckedMultiply(
              candidate,
              static_cast<uint64_t>(options_.averageKeyBytes) + options_.averageValueBytes,
              payloadBytes) ||
          !CheckedAdd(estimated, bucketBytes, estimated) ||
          !CheckedAdd(estimated, payloadBytes, estimated)) {
        break;
      }
      if (estimated > options_.targetRowBytes) {
        break;
      }
      best = static_cast<uint32_t>(candidate);
    }
    return best;
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

    if (fileSize == 0) {
      if (readOnly_) {
        return Status::Corruption("row index file is empty");
      }
      detail::RowIndexFileHeader header;
      status = ConfigureNewDatabase(header);
      if (!status) {
        return status;
      }
      return CreateEmptyIndex(header.bucketCount, header);
    }
    if (fileSize < sizeof(detail::RowIndexFileHeader)) {
      return Status::Corruption("row index header is truncated");
    }

    detail::RowIndexFileHeader header;
    status = detail::ReadAllAt(indexFile_.Get(), &header, sizeof(header), 0);
    if (!status) {
      return status;
    }
    status = ValidateIndexHeader(header, fileSize);
    if (!status) {
      return status;
    }
    return readOnly_ ? indexMap_.MapReadOnly(indexFile_.Get(), fileSize)
                     : indexMap_.Map(indexFile_.Get(), fileSize);
  }

  Status ValidateIndexHeader(const detail::RowIndexFileHeader& header, uint64_t fileSize) const {
    if (!MagicEquals(header.magic, detail::kRowIndexFileMagic) ||
        header.version != detail::kStorageVersion ||
        header.headerSize != sizeof(detail::RowIndexFileHeader)) {
      return Status::Corruption(
          "unsupported row index format; create a new database for the "
          "automatic-row storage format");
    }
    if (!std::has_single_bit(header.bucketCount) || header.bucketCount < kMinimumIndexBucketCount ||
        header.itemCount > header.bucketCount || header.routeCount == 0 ||
        header.targetEntriesPerRow == 0 || header.expectedAutomaticColumnCount == 0 ||
        header.shardCount == 0 || header.shardCount > kMaximumRowShards ||
        header.spillPartitionCount == 0 || header.spillPartitionCount > kMaximumSpillPartitions ||
        !std::has_single_bit(header.spillPartitionCount) || header.routeCount >= kExplicitRowBit ||
        header.routingSeed == 0) {
      return Status::Corruption("row index header contains invalid routing data");
    }
    uint64_t bucketBytes = 0;
    uint64_t expectedSize = sizeof(detail::RowIndexFileHeader);
    if (!CheckedMultiply(header.bucketCount, sizeof(detail::RowIndexBucket), bucketBytes) ||
        !CheckedAdd(expectedSize, bucketBytes, expectedSize) || expectedSize != fileSize) {
      return Status::Corruption("row index file has an invalid size");
    }
    return Status::Ok();
  }

  Status CreateEmptyIndex(uint64_t bucketCount, const detail::RowIndexFileHeader& configuration) {
    uint64_t bucketBytes = 0;
    uint64_t fileSize = sizeof(detail::RowIndexFileHeader);
    if (!CheckedMultiply(bucketCount, sizeof(detail::RowIndexBucket), bucketBytes) ||
        !CheckedAdd(fileSize, bucketBytes, fileSize) ||
        fileSize > std::numeric_limits<size_t>::max()) {
      return Status::InvalidArgument("row index file is too large");
    }
    indexMap_.Unmap();
    Status status = detail::TruncateFile(indexFile_.Get(), fileSize);
    if (!status) {
      return status;
    }
    status = indexMap_.Map(indexFile_.Get(), fileSize);
    if (!status) {
      return status;
    }
    std::memset(indexMap_.Data(), 0, static_cast<size_t>(fileSize));
    detail::RowIndexFileHeader header = configuration;
    header.bucketCount = bucketCount;
    header.itemCount = 0;
    header.entryCount = 0;
    *IndexHeader() = header;
    return Status::Ok();
  }

  Status OpenRowFiles() {
    rowShards_.reserve(shardCount_);
    for (uint32_t shardId = 0; shardId < shardCount_; ++shardId) {
      auto shard = std::make_unique<RowShard>();
      const std::filesystem::path path = RowValuePath(directory_, shardId);
      Status status = readOnly_ ? detail::OpenReadOnly(path, shard->file)
                                : detail::OpenReadWriteCreate(path, shard->file);
      if (!status) {
        return status;
      }
      uint64_t fileSize = 0;
      status = detail::GetFileSize(shard->file.Get(), fileSize);
      if (!status) {
        return status;
      }
      if (fileSize == 0) {
        if (readOnly_) {
          return Status::Corruption("row value file is empty");
        }
        detail::RowValueFileHeader header;
        status = detail::WriteAllAt(shard->file.Get(), &header, sizeof(header), 0);
        if (!status) {
          return status;
        }
        fileSize = sizeof(header);
      } else {
        if (fileSize < sizeof(detail::RowValueFileHeader)) {
          return Status::Corruption("row value header is truncated");
        }
        detail::RowValueFileHeader header;
        status = detail::ReadAllAt(shard->file.Get(), &header, sizeof(header), 0);
        if (!status) {
          return status;
        }
        if (!MagicEquals(header.magic, detail::kRowValueFileMagic) ||
            header.version != detail::kStorageVersion ||
            header.headerSize != sizeof(detail::RowValueFileHeader)) {
          return Status::Corruption("unsupported row value format");
        }
      }
      shard->appendOffset = fileSize;
      if (readOnly_) {
        status = shard->map.MapReadOnly(shard->file.Get(), fileSize);
        if (!status) {
          return status;
        }
      }
      rowShards_.push_back(std::move(shard));
    }
    return Status::Ok();
  }

  Status RejectIncompleteBuildAndCleanStaging() {
    std::error_code error;
    const bool markerExists = std::filesystem::exists(markerPath_, error);
    if (error) {
      return Status::IoError("inspect incomplete marker: " + error.message());
    }
    if (markerExists) {
      return Status::Corruption(
          "database build is incomplete; delete the output directory and "
          "rebuild it");
    }
    if (!readOnly_) {
      Status status = RemoveStageFiles();
      if (!status) {
        return status;
      }
      std::filesystem::remove(markerTempPath_, error);
      if (error) {
        return Status::IoError("remove stale incomplete marker: " + error.message());
      }
    }
    return Status::Ok();
  }

  Status BeginBuild() {
    if (buildActive_.load(std::memory_order_acquire)) {
      return Status::Ok();
    }
    std::lock_guard buildLock(buildMutex_);
    if (buildActive_.load(std::memory_order_relaxed)) {
      return Status::Ok();
    }

    std::error_code error;
    if (std::filesystem::exists(markerPath_, error)) {
      return Status::Corruption("database build is already marked incomplete");
    }
    if (error) {
      return Status::IoError("inspect incomplete marker: " + error.message());
    }
    std::filesystem::remove(markerTempPath_, error);
    if (error) {
      return Status::IoError("remove stale incomplete marker: " + error.message());
    }

    detail::FileDescriptor file;
    Status status = detail::OpenReadWriteCreate(markerTempPath_, file);
    if (!status) {
      return status;
    }
    status = detail::TruncateFile(file.Get(), 0);
    if (!status) {
      return status;
    }
    status = detail::SyncFile(file.Get());
    if (!status) {
      return status;
    }
    file.Reset();
    std::filesystem::rename(markerTempPath_, markerPath_, error);
    if (error) {
      return Status::IoError("publish incomplete marker: " + error.message());
    }
    status = detail::SyncDirectory(directory_);
    if (!status) {
      return status;
    }
    buildActive_.store(true, std::memory_order_release);
    ReportProgress(WritePhase::kStaging, 0, 0);
    return Status::Ok();
  }

  Status EnsureStageFileOpen(uint32_t partitionId, SpillPartition& partition) {
    if (partition.file.IsValid()) {
      return Status::Ok();
    }
    Status status = detail::OpenReadWriteCreate(StagePath(directory_, partitionId), partition.file);
    if (!status) {
      return status;
    }
    uint64_t fileSize = 0;
    status = detail::GetFileSize(partition.file.Get(), fileSize);
    if (!status) {
      return status;
    }
    if (fileSize == 0) {
      detail::StageFileHeader header;
      header.partitionId = partitionId;
      header.partitionCount = static_cast<uint32_t>(spillPartitions_.size());
      status = detail::WriteAllAt(partition.file.Get(), &header, sizeof(header), 0);
      if (!status) {
        return status;
      }
      fileSize = sizeof(header);
    } else {
      detail::StageFileHeader header;
      if (fileSize < sizeof(header)) {
        return Status::Corruption("stage file header is truncated");
      }
      status = detail::ReadAllAt(partition.file.Get(), &header, sizeof(header), 0);
      if (!status) {
        return status;
      }
      if (!MagicEquals(header.magic, detail::kStageFileMagic) ||
          header.version != detail::kStorageVersion || header.headerSize != sizeof(header) ||
          header.partitionId != partitionId || header.partitionCount != spillPartitions_.size()) {
        return Status::Corruption("stage file header is invalid");
      }
    }
    partition.persistedSize = fileSize;
    partition.used = true;
    return Status::Ok();
  }

  Status FlushStageBuffer(SpillPartition& partition) {
    if (partition.buffer.empty()) {
      return Status::Ok();
    }
    Status status = detail::WriteAllAt(partition.file.Get(), partition.buffer.data(),
                                       partition.buffer.size(), partition.persistedSize);
    if (!status) {
      return status;
    }
    partition.persistedSize += partition.buffer.size();
    partition.buffer.clear();
    return Status::Ok();
  }

  Status FlushLocked() {
    if (readOnly_) {
      return Status::Ok();
    }
    if (writePoisoned_.load(std::memory_order_acquire)) {
      return Status::Corruption("write batch failed; delete the output directory and rebuild it");
    }
    if (!buildActive_.load(std::memory_order_acquire)) {
      return Status::Ok();
    }

    const uint64_t entryTotal = stagedEntryCount_.load(std::memory_order_acquire);
    ReportProgress(WritePhase::kStaging, 0, entryTotal);
    uint64_t maximumPartitionBytes = 0;
    uint32_t usedPartitions = 0;
    for (auto& partitionPtr : spillPartitions_) {
      SpillPartition& partition = *partitionPtr;
      if (!partition.used) {
        continue;
      }
      Status status = FlushStageBuffer(partition);
      if (!status) {
        writePoisoned_.store(true, std::memory_order_release);
        return status;
      }
      maximumPartitionBytes = std::max(maximumPartitionBytes, partition.persistedSize);
      ++usedPartitions;
    }
    ReportProgress(WritePhase::kStaging, entryTotal, entryTotal);

    uint32_t workerCount =
        std::min(ResolvedWriterThreads(options_), std::max<uint32_t>(1, usedPartitions));
    if (maximumPartitionBytes > 0) {
      const uint64_t estimatedWorkerMemory =
          maximumPartitionBytes > std::numeric_limits<uint64_t>::max() / 3
              ? std::numeric_limits<uint64_t>::max()
              : maximumPartitionBytes * 3;
      const uint64_t memoryWorkers = std::max<uint64_t>(
          1, options_.memoryBudgetBytes / std::max<uint64_t>(1, estimatedWorkerMemory));
      workerCount = static_cast<uint32_t>(std::min<uint64_t>(workerCount, memoryWorkers));
    }

    ReportProgress(WritePhase::kBuildingRows, 0, usedPartitions);
    std::atomic<uint32_t> nextPartition{0};
    std::atomic<uint32_t> completedPartitions{0};
    std::atomic<bool> stop{false};
    std::mutex errorMutex;
    Status firstError = Status::Ok();
    std::vector<std::thread> workers;
    workers.reserve(workerCount);
    for (uint32_t worker = 0; worker < workerCount; ++worker) {
      workers.emplace_back([&] {
        while (!stop.load(std::memory_order_acquire)) {
          const uint32_t partitionId = nextPartition.fetch_add(1, std::memory_order_relaxed);
          if (partitionId >= spillPartitions_.size()) {
            break;
          }
          if (!spillPartitions_[partitionId]->used) {
            continue;
          }
          Status status = ProcessStagePartition(partitionId);
          if (!status) {
            {
              std::lock_guard errorLock(errorMutex);
              if (firstError) {
                firstError = status;
              }
            }
            stop.store(true, std::memory_order_release);
            break;
          }
          completedPartitions.fetch_add(1, std::memory_order_relaxed);
        }
      });
    }
    for (std::thread& worker : workers) {
      worker.join();
    }
    if (!firstError) {
      writePoisoned_.store(true, std::memory_order_release);
      return firstError;
    }
    ReportProgress(WritePhase::kBuildingRows, completedPartitions.load(std::memory_order_relaxed),
                   usedPartitions);

    IndexHeader()->nextSequence = nextSequence_.load(std::memory_order_acquire);
    ReportProgress(WritePhase::kFlushing, 0, static_cast<uint64_t>(rowShards_.size()) + 2);
    uint64_t flushStep = 0;
    for (auto& shard : rowShards_) {
      Status status = detail::SyncFile(shard->file.Get());
      if (!status) {
        writePoisoned_.store(true, std::memory_order_release);
        return status;
      }
      ReportProgress(WritePhase::kFlushing, ++flushStep,
                     static_cast<uint64_t>(rowShards_.size()) + 2);
    }
    Status status = indexMap_.Sync();
    if (!status) {
      writePoisoned_.store(true, std::memory_order_release);
      return status;
    }
    ReportProgress(WritePhase::kFlushing, ++flushStep,
                   static_cast<uint64_t>(rowShards_.size()) + 2);
    status = detail::SyncFile(indexFile_.Get());
    if (!status) {
      writePoisoned_.store(true, std::memory_order_release);
      return status;
    }
    ReportProgress(WritePhase::kFlushing, ++flushStep,
                   static_cast<uint64_t>(rowShards_.size()) + 2);

    // Removing the marker is the commit point. Stale stage files left by a
    // crash after this point are harmless and are deleted on the next open.
    std::error_code error;
    std::filesystem::remove(markerPath_, error);
    if (error) {
      writePoisoned_.store(true, std::memory_order_release);
      return Status::IoError("remove incomplete marker after commit: " + error.message());
    }
    status = detail::SyncDirectory(directory_);
    if (!status) {
      writePoisoned_.store(true, std::memory_order_release);
      return status;
    }
    buildActive_.store(false, std::memory_order_release);
    stagedEntryCount_.store(0, std::memory_order_release);

    status = ResetAndRemoveStageFiles();
    if (!status) {
      return status;
    }
    return Status::Ok();
  }

  Status ProcessStagePartition(uint32_t partitionId) {
    SpillPartition& partition = *spillPartitions_[partitionId];
    if (partition.persistedSize < sizeof(detail::StageFileHeader)) {
      return Status::Corruption("stage file is truncated");
    }
    detail::MappedFile map;
    Status status = map.MapReadOnly(partition.file.Get(), partition.persistedSize);
    if (!status) {
      return status;
    }
    const auto* bytes = static_cast<const std::byte*>(map.Data());
    detail::StageFileHeader fileHeader;
    std::memcpy(&fileHeader, bytes, sizeof(fileHeader));
    if (!MagicEquals(fileHeader.magic, detail::kStageFileMagic) ||
        fileHeader.partitionId != partitionId ||
        fileHeader.partitionCount != spillPartitions_.size()) {
      return Status::Corruption("stage file header is invalid");
    }

    std::unordered_map<StageGroupKey, size_t, StageGroupKeyHash> groupIndexes;
    std::vector<StageGroup> groups;
    uint64_t approximateGroups = 0;
    uint64_t approximateAutomaticGroups = 0;
    const uint64_t explicitGroups =
        DivideRoundUp(expectedExplicitRowCount_, spillPartitions_.size());
    const bool groupEstimateValid =
        CheckedMultiply(DivideRoundUp(routeCount_, spillPartitions_.size()),
                        expectedAutomaticColumnCount_, approximateAutomaticGroups) &&
        CheckedAdd(approximateAutomaticGroups, explicitGroups, approximateGroups);
    const uint64_t maximumGroupsInFile =
        (partition.persistedSize - sizeof(detail::StageFileHeader)) /
        sizeof(detail::StageRecordHeader);
    if (groupEstimateValid) {
      approximateGroups = std::min(approximateGroups, maximumGroupsInFile);
    }
    if (groupEstimateValid && approximateGroups <= std::numeric_limits<size_t>::max()) {
      groupIndexes.reserve(static_cast<size_t>(approximateGroups));
      groups.reserve(static_cast<size_t>(approximateGroups));
    }

    uint64_t offset = sizeof(detail::StageFileHeader);
    while (offset < partition.persistedSize) {
      if (sizeof(detail::StageRecordHeader) > partition.persistedSize - offset) {
        return Status::Corruption("stage file has a partial record header");
      }
      detail::StageRecordHeader record;
      std::memcpy(&record, bytes + offset, sizeof(record));
      if (record.magic != detail::kStageRecordMagic || record.version != detail::kStorageVersion ||
          record.headerSize != sizeof(record) || record.columnHash == 0 || record.keyHash == 0) {
        return Status::Corruption("stage record header is invalid");
      }
      uint64_t recordSize = sizeof(record);
      if (!CheckedAdd(recordSize, record.columnSize, recordSize) ||
          !CheckedAdd(recordSize, record.keySize, recordSize) ||
          !CheckedAdd(recordSize, record.valueSize, recordSize) ||
          recordSize > partition.persistedSize - offset) {
        return Status::Corruption("stage record extends beyond its file");
      }
      if (detail::MixHashes(record.columnHash, record.rowId) % spillPartitions_.size() !=
          partitionId) {
        return Status::Corruption("stage record is in the wrong partition");
      }

      const char* columnData = reinterpret_cast<const char*>(bytes + offset + sizeof(record));
      const char* keyData = columnData + record.columnSize;
      const std::byte* valueData = reinterpret_cast<const std::byte*>(keyData + record.keySize);
      const std::string_view column(columnData, record.columnSize);
      const std::string_view key(keyData, record.keySize);
      const uint64_t rowId = record.rowId;
      const StageGroupKey groupKey{record.columnHash, rowId, column};
      auto [iterator, inserted] = groupIndexes.emplace(groupKey, groups.size());
      if (inserted) {
        groups.push_back(
            StageGroup{.column = column, .columnHash = record.columnHash, .rowId = rowId});
      }
      StageGroup& group = groups[iterator->second];
      group.entries.push_back(
          EntryView{.key = key,
                    .value = std::span<const std::byte>(valueData, record.valueSize),
                    .keyHash = record.keyHash});
      offset += recordSize;
    }
    if (offset != partition.persistedSize) {
      return Status::Corruption("stage file has trailing bytes");
    }

    for (const StageGroup& group : groups) {
      status = WriteStageGroup(group);
      if (!status) {
        return status;
      }
    }
    return Status::Ok();
  }

  Status WriteStageGroup(const StageGroup& group) {
    std::vector<OwnedEntry> existing;
    Status status = LoadExistingRow(group.column, group.columnHash, group.rowId, existing);
    if (!status) {
      return status;
    }
    std::vector<EntryView> entries;
    entries.reserve(existing.size() + group.entries.size());
    for (const OwnedEntry& entry : existing) {
      entries.push_back(EntryView{.key = entry.key,
                                  .value = entry.value,
                                  .keyHash = NormalizeHash(detail::HashString(entry.key))});
    }
    entries.insert(entries.end(), group.entries.begin(), group.entries.end());

    const uint64_t sequence = nextSequence_.fetch_add(1, std::memory_order_relaxed);
    std::vector<std::byte> block;
    uint32_t itemCount = 0;
    status = BuildRowBlock(group.column, group.columnHash, group.rowId, sequence, entries, block,
                           itemCount);
    if (!status) {
      return status;
    }
    const uint32_t shardId =
        static_cast<uint32_t>(detail::MixHashes(group.columnHash, group.rowId) % rowShards_.size());
    RowShard& shard = *rowShards_[shardId];
    uint64_t blockOffset = 0;
    {
      std::lock_guard shardLock(shard.mutex);
      blockOffset = shard.appendOffset;
      status = detail::WriteAllAt(shard.file.Get(), block.data(), block.size(), blockOffset);
      if (!status) {
        return status;
      }
      shard.appendOffset += block.size();
    }

    detail::RowIndexBucket bucket;
    bucket.columnHash = group.columnHash;
    bucket.rowId = group.rowId;
    bucket.blockOffset = blockOffset;
    bucket.sequence = sequence;
    bucket.blockSize = static_cast<uint32_t>(block.size());
    bucket.shardId = shardId;
    bucket.itemCount = itemCount;
    return PublishRowBucket(group.column, bucket);
  }

  Status LoadExistingRow(std::string_view column, uint64_t columnHash, uint64_t rowId,
                         std::vector<OwnedEntry>& entries) const {
    RowLookup lookup;
    {
      std::lock_guard indexLock(indexMutex_);
      Status status = FindRowBucket(column, columnHash, rowId, lookup);
      if (!status) {
        return status;
      }
    }
    if (!lookup.found) {
      return Status::Ok();
    }

    std::vector<std::byte> block(lookup.bucket.blockSize);
    Status status =
        ReadRowAt(lookup.bucket.shardId, block.data(), block.size(), lookup.bucket.blockOffset);
    if (!status) {
      return status;
    }
    detail::RowBlockHeader header;
    std::memcpy(&header, block.data(), sizeof(header));
    status = ValidateBlockHeader(header);
    if (!status) {
      return status;
    }
    const uint64_t bucketOffset = AlignUp8(sizeof(header) + header.columnSize);
    const uint64_t keyBytesOffset =
        bucketOffset + static_cast<uint64_t>(header.bucketCount) * sizeof(detail::RowKeyBucket);
    const uint64_t valueBytesOffset = keyBytesOffset + header.keyBytesSize;
    entries.reserve(header.itemCount);
    for (uint32_t index = 0; index < header.bucketCount; ++index) {
      detail::RowKeyBucket bucket;
      std::memcpy(&bucket,
                  block.data() + bucketOffset + static_cast<uint64_t>(index) * sizeof(bucket),
                  sizeof(bucket));
      if (bucket.keyHash == 0) {
        continue;
      }
      if (bucket.keyOffset > header.keyBytesSize ||
          bucket.keySize > header.keyBytesSize - bucket.keyOffset ||
          bucket.valueOffset > header.valueBytesSize ||
          bucket.valueSize > header.valueBytesSize - bucket.valueOffset) {
        return Status::Corruption("row bucket points outside its block");
      }
      OwnedEntry entry;
      entry.key.assign(
          reinterpret_cast<const char*>(block.data() + keyBytesOffset + bucket.keyOffset),
          bucket.keySize);
      entry.value.resize(bucket.valueSize);
      if (bucket.valueSize != 0) {
        std::memcpy(entry.value.data(), block.data() + valueBytesOffset + bucket.valueOffset,
                    bucket.valueSize);
      }
      entries.push_back(std::move(entry));
    }
    if (entries.size() != header.itemCount) {
      return Status::Corruption("row item count does not match its buckets");
    }
    return Status::Ok();
  }

  Status BuildRowBlock(std::string_view column, uint64_t columnHash, uint64_t rowId,
                       uint64_t sequence, std::span<const EntryView> entries,
                       std::vector<std::byte>& block, uint32_t& itemCount) const {
    if (entries.empty()) {
      return Status::InvalidArgument("row has an unsupported number of entries");
    }

    // Grow according to the number of unique keys, not the number of staged
    // records. Duplicate writes only replace an entry index and therefore do
    // not inflate either the temporary table or the persisted row table.
    auto bucketCountForEntries = [](uint64_t entryCount) {
      const uint64_t requiredBuckets =
          static_cast<uint64_t>(static_cast<long double>(entryCount) / kRowLoadFactor) + 1;
      return RoundUpPowerOfTwo(requiredBuckets, 2);
    };
    const uint64_t initialEntryEstimate =
        std::min<uint64_t>({entries.size(), kMaximumDedupPresizeEntries,
                            RoutingTargetEntries(IndexHeader()->targetEntriesPerRow)});
    const uint64_t initialBucketCount = bucketCountForEntries(initialEntryEstimate);
    if (initialBucketCount == 0 || initialBucketCount > std::numeric_limits<uint32_t>::max()) {
      return Status::InvalidArgument("row local index is too large");
    }

    std::vector<TemporaryKeySlot> temporary(static_cast<size_t>(initialBucketCount));
    auto rehashTemporaryTable = [&](uint64_t newBucketCount) -> Status {
      if (newBucketCount == 0 || newBucketCount > std::numeric_limits<uint32_t>::max() ||
          newBucketCount > std::numeric_limits<size_t>::max()) {
        return Status::InvalidArgument("row local index is too large");
      }
      if (newBucketCount == temporary.size()) {
        return Status::Ok();
      }
      std::vector<TemporaryKeySlot> rehashed(static_cast<size_t>(newBucketCount));
      const size_t mask = rehashed.size() - 1;
      for (const TemporaryKeySlot& source : temporary) {
        if (source.keyHash == 0) {
          continue;
        }
        const size_t start = static_cast<size_t>(source.keyHash) & mask;
        bool placed = false;
        for (size_t probe = 0; probe < rehashed.size(); ++probe) {
          TemporaryKeySlot& target = rehashed[(start + probe) & mask];
          if (target.keyHash == 0) {
            target = source;
            placed = true;
            break;
          }
        }
        if (!placed) {
          return Status::Corruption("row local index is unexpectedly full during rehash");
        }
      }
      temporary = std::move(rehashed);
      return Status::Ok();
    };

    uint32_t uniqueCount = 0;
    for (size_t entryIndex = 0; entryIndex < entries.size(); ++entryIndex) {
      const EntryView& entry = entries[entryIndex];
      const uint64_t keyHash = NormalizeHash(entry.keyHash);
      for (;;) {
        const size_t mask = temporary.size() - 1;
        const size_t start = static_cast<size_t>(keyHash) & mask;
        size_t emptyIndex = temporary.size();
        bool replaced = false;
        for (size_t probe = 0; probe < temporary.size(); ++probe) {
          const size_t slotIndex = (start + probe) & mask;
          TemporaryKeySlot& slot = temporary[slotIndex];
          if (slot.keyHash == 0) {
            emptyIndex = slotIndex;
            break;
          }
          if (slot.keyHash == keyHash && entries[slot.entryIndex].key == entry.key) {
            slot.entryIndex = entryIndex;
            replaced = true;
            break;
          }
        }
        if (replaced) {
          break;
        }
        if (emptyIndex == temporary.size()) {
          return Status::Corruption("row local index is unexpectedly full");
        }

        const uint64_t nextUniqueCount = static_cast<uint64_t>(uniqueCount) + 1;
        if (nextUniqueCount * 4 >= static_cast<uint64_t>(temporary.size()) * 3) {
          Status status = rehashTemporaryTable(static_cast<uint64_t>(temporary.size()) * 2);
          if (!status) {
            return status;
          }
          continue;
        }
        temporary[emptyIndex].keyHash = keyHash;
        temporary[emptyIndex].entryIndex = entryIndex;
        ++uniqueCount;
        break;
      }
    }

    const uint64_t bucketCount64 = bucketCountForEntries(uniqueCount);
    Status status = rehashTemporaryTable(bucketCount64);
    if (!status) {
      return status;
    }
    uint64_t minimumBlockSize = AlignUp8(sizeof(detail::RowBlockHeader) + column.size());
    uint64_t minimumBucketBytes = 0;
    if (!CheckedMultiply(bucketCount64, sizeof(detail::RowKeyBucket), minimumBucketBytes) ||
        !CheckedAdd(minimumBlockSize, minimumBucketBytes, minimumBlockSize) ||
        minimumBlockSize > std::numeric_limits<uint32_t>::max()) {
      return Status::InvalidArgument(
          "one routed row exceeds the 4 GiB compact-row limit; increase "
          "expectedEntryCountPerColumn");
    }
    const uint32_t bucketCount = static_cast<uint32_t>(bucketCount64);

    uint64_t keyBytesSize = 0;
    uint64_t valueBytesSize = 0;
    for (const TemporaryKeySlot& slot : temporary) {
      if (slot.keyHash == 0) {
        continue;
      }
      if (!CheckedAdd(keyBytesSize, entries[slot.entryIndex].key.size(), keyBytesSize) ||
          !CheckedAdd(valueBytesSize, entries[slot.entryIndex].value.size(), valueBytesSize)) {
        return Status::InvalidArgument("row payload size overflows uint64");
      }
    }
    if (keyBytesSize > std::numeric_limits<uint32_t>::max() ||
        valueBytesSize > std::numeric_limits<uint32_t>::max() ||
        column.size() > std::numeric_limits<uint32_t>::max()) {
      return Status::InvalidArgument(
          "one routed row exceeds the 4 GiB compact-row limit; increase "
          "expectedEntryCountPerColumn or reduce target row size");
    }

    const uint64_t bucketOffset = AlignUp8(sizeof(detail::RowBlockHeader) + column.size());
    uint64_t bucketBytes = 0;
    uint64_t keyBytesOffset = 0;
    uint64_t valueBytesOffset = 0;
    uint64_t blockSize = 0;
    if (!CheckedMultiply(bucketCount, sizeof(detail::RowKeyBucket), bucketBytes) ||
        !CheckedAdd(bucketOffset, bucketBytes, keyBytesOffset) ||
        !CheckedAdd(keyBytesOffset, keyBytesSize, valueBytesOffset) ||
        !CheckedAdd(valueBytesOffset, valueBytesSize, blockSize) ||
        blockSize > std::numeric_limits<uint32_t>::max() ||
        blockSize > std::numeric_limits<size_t>::max()) {
      return Status::InvalidArgument("row block exceeds the 4 GiB format limit");
    }

    block.assign(static_cast<size_t>(blockSize), std::byte{0});
    detail::RowBlockHeader header;
    header.sequence = sequence;
    header.columnHash = columnHash;
    header.rowId = rowId;
    header.columnSize = static_cast<uint32_t>(column.size());
    header.itemCount = uniqueCount;
    header.bucketCount = bucketCount;
    header.keyBytesSize = static_cast<uint32_t>(keyBytesSize);
    header.valueBytesSize = static_cast<uint32_t>(valueBytesSize);
    header.blockSize = static_cast<uint32_t>(blockSize);
    std::memcpy(block.data(), &header, sizeof(header));
    std::memcpy(block.data() + sizeof(header), column.data(), column.size());

    uint32_t keyCursor = 0;
    uint32_t valueCursor = 0;
    for (uint32_t bucketIndex = 0; bucketIndex < bucketCount; ++bucketIndex) {
      const TemporaryKeySlot& temporarySlot = temporary[bucketIndex];
      if (temporarySlot.keyHash == 0) {
        continue;
      }
      const EntryView& entry = entries[temporarySlot.entryIndex];
      detail::RowKeyBucket bucket;
      bucket.keyHash = temporarySlot.keyHash;
      bucket.keyOffset = keyCursor;
      bucket.keySize = static_cast<uint32_t>(entry.key.size());
      bucket.valueOffset = valueCursor;
      bucket.valueSize = static_cast<uint32_t>(entry.value.size());
      std::memcpy(block.data() + bucketOffset + static_cast<uint64_t>(bucketIndex) * sizeof(bucket),
                  &bucket, sizeof(bucket));
      if (!entry.key.empty()) {
        std::memcpy(block.data() + keyBytesOffset + keyCursor, entry.key.data(), entry.key.size());
      }
      if (!entry.value.empty()) {
        std::memcpy(block.data() + valueBytesOffset + valueCursor, entry.value.data(),
                    entry.value.size());
      }
      keyCursor += bucket.keySize;
      valueCursor += bucket.valueSize;
    }
    itemCount = uniqueCount;
    return Status::Ok();
  }

  Status PublishRowBucket(std::string_view column, const detail::RowIndexBucket& bucket) {
    std::lock_guard indexLock(indexMutex_);
    for (;;) {
      RowLookup lookup;
      Status status = FindRowBucket(column, bucket.columnHash, bucket.rowId, lookup);
      if (!status) {
        return status;
      }
      if (lookup.found) {
        IndexHeader()->entryCount -= lookup.bucket.itemCount;
        IndexHeader()->entryCount += bucket.itemCount;
        Buckets()[lookup.index] = bucket;
        return Status::Ok();
      }
      const long double loadAfterInsert =
          static_cast<long double>(IndexHeader()->itemCount + 1) / IndexHeader()->bucketCount;
      if (loadAfterInsert > options_.maxLoadFactor) {
        status = ResizeIndexLocked(IndexHeader()->bucketCount * 2);
        if (!status) {
          return status;
        }
        continue;
      }
      Buckets()[lookup.index] = bucket;
      ++IndexHeader()->itemCount;
      IndexHeader()->entryCount += bucket.itemCount;
      return Status::Ok();
    }
  }

  Status ResizeIndexLocked(uint64_t newBucketCount) {
    newBucketCount = RoundUpPowerOfTwo(newBucketCount, kMinimumIndexBucketCount);
    if (newBucketCount == 0) {
      return Status::InvalidArgument("row index cannot grow further");
    }
    const detail::RowIndexFileHeader configuration = *IndexHeader();
    std::vector<detail::RowIndexBucket> oldBuckets;
    oldBuckets.reserve(static_cast<size_t>(configuration.itemCount));
    for (uint64_t index = 0; index < configuration.bucketCount; ++index) {
      if (Buckets()[index].columnHash != 0) {
        oldBuckets.push_back(Buckets()[index]);
      }
    }
    Status status = CreateEmptyIndex(newBucketCount, configuration);
    if (!status) {
      return status;
    }
    for (const detail::RowIndexBucket& bucket : oldBuckets) {
      const uint64_t mask = IndexHeader()->bucketCount - 1;
      const uint64_t start = detail::MixHashes(bucket.columnHash, bucket.rowId) & mask;
      bool placed = false;
      for (uint64_t probe = 0; probe < IndexHeader()->bucketCount; ++probe) {
        detail::RowIndexBucket& target = Buckets()[(start + probe) & mask];
        if (target.columnHash == 0) {
          target = bucket;
          ++IndexHeader()->itemCount;
          IndexHeader()->entryCount += bucket.itemCount;
          placed = true;
          break;
        }
      }
      if (!placed) {
        return Status::Corruption("row index is full during resize");
      }
    }
    return Status::Ok();
  }

  Status FindRowBucket(std::string_view column, uint64_t columnHash, uint64_t rowId,
                       RowLookup& lookup) const {
    const uint64_t bucketCount = IndexHeader()->bucketCount;
    const uint64_t mask = bucketCount - 1;
    const uint64_t start = detail::MixHashes(columnHash, rowId) & mask;
    for (uint64_t probe = 0; probe < bucketCount; ++probe) {
      const uint64_t index = (start + probe) & mask;
      const detail::RowIndexBucket& bucket = Buckets()[index];
      if (bucket.columnHash == 0) {
        lookup.index = index;
        lookup.found = false;
        return Status::Ok();
      }
      if (bucket.columnHash != columnHash || bucket.rowId != rowId) {
        continue;
      }
      bool matches = false;
      detail::RowBlockHeader header;
      Status status = RowBucketMatches(bucket, column, matches, &header);
      if (!status) {
        return status;
      }
      if (matches) {
        lookup.index = index;
        lookup.found = true;
        lookup.bucket = bucket;
        lookup.header = header;
        return Status::Ok();
      }
    }
    return Status::Corruption("row index table is full");
  }

  Status RowBucketMatches(const detail::RowIndexBucket& bucket, std::string_view column,
                          bool& matches, detail::RowBlockHeader* outputHeader) const {
    matches = false;
    if (bucket.shardId >= rowShards_.size()) {
      return Status::Corruption("row index points to an invalid shard");
    }
    detail::RowBlockHeader header;
    Status status = ReadRowAt(bucket.shardId, &header, sizeof(header), bucket.blockOffset);
    if (!status) {
      return status;
    }
    status = ValidateBlockHeader(header);
    if (!status) {
      return status;
    }
    if (header.columnHash != bucket.columnHash || header.rowId != bucket.rowId ||
        header.sequence != bucket.sequence || header.blockSize != bucket.blockSize ||
        header.itemCount != bucket.itemCount) {
      return Status::Corruption("row index points to the wrong row block");
    }
    if (header.columnSize != column.size()) {
      return Status::Ok();
    }
    std::string storedColumn(header.columnSize, '\0');
    status = ReadRowAt(bucket.shardId, storedColumn.data(), storedColumn.size(),
                       bucket.blockOffset + sizeof(header));
    if (!status) {
      return status;
    }
    matches = storedColumn == column;
    if (matches && outputHeader != nullptr) {
      *outputHeader = header;
    }
    return Status::Ok();
  }

  Status ValidateBlockHeader(const detail::RowBlockHeader& header) const {
    if (header.magic != detail::kRowBlockMagic || header.version != detail::kStorageVersion ||
        header.headerSize != sizeof(detail::RowBlockHeader) || header.columnHash == 0 ||
        header.sequence == 0 || !std::has_single_bit(header.bucketCount) ||
        header.bucketCount < 2 || header.itemCount > header.bucketCount) {
      return Status::Corruption("row block header is invalid");
    }
    const uint64_t bucketOffset = AlignUp8(sizeof(detail::RowBlockHeader) + header.columnSize);
    uint64_t bucketBytes = 0;
    uint64_t expectedSize = 0;
    if (!CheckedMultiply(header.bucketCount, sizeof(detail::RowKeyBucket), bucketBytes) ||
        !CheckedAdd(bucketOffset, bucketBytes, expectedSize) ||
        !CheckedAdd(expectedSize, header.keyBytesSize, expectedSize) ||
        !CheckedAdd(expectedSize, header.valueBytesSize, expectedSize) ||
        expectedSize != header.blockSize) {
      return Status::Corruption("row block size is invalid");
    }
    return Status::Ok();
  }

  Status ReadRowAt(uint32_t shardId, void* data, size_t size, uint64_t offset) const {
    if (shardId >= rowShards_.size()) {
      return Status::Corruption("row read uses an invalid shard");
    }
    const RowShard& shard = *rowShards_[shardId];
    if (!shard.map.IsMapped()) {
      return detail::ReadAllAt(shard.file.Get(), data, size, offset);
    }
    if (offset > shard.map.Size() || size > shard.map.Size() - offset) {
      return Status::Corruption("row read extends beyond its value file");
    }
    if (size != 0) {
      std::memcpy(data, static_cast<const std::byte*>(shard.map.Data()) + offset, size);
    }
    return Status::Ok();
  }

  Status ResetAndRemoveStageFiles() {
    for (auto& partition : spillPartitions_) {
      partition->file.Reset();
      partition->buffer.clear();
      partition->persistedSize = 0;
      partition->used = false;
    }
    return RemoveStageFiles();
  }

  Status RemoveStageFiles() const {
    std::error_code error;
    if (!std::filesystem::exists(directory_, error)) {
      return error ? Status::IoError("inspect database directory: " + error.message())
                   : Status::Ok();
    }
    for (const auto& entry : std::filesystem::directory_iterator(directory_, error)) {
      if (error) {
        return Status::IoError("enumerate database directory: " + error.message());
      }
      const std::string name = entry.path().filename().string();
      if (name.starts_with("stage-") && name.ends_with(".lumost")) {
        std::filesystem::remove(entry.path(), error);
        if (error) {
          return Status::IoError("remove stale stage file: " + error.message());
        }
      }
    }
    return Status::Ok();
  }

  void ReportProgress(WritePhase phase, uint64_t completed, uint64_t total) const noexcept {
    if (!options_.writeProgress) {
      return;
    }
    try {
      options_.writeProgress(WriteProgress{.phase = phase, .completed = completed, .total = total});
    } catch (...) {
      // Observability must never make a valid write fail.
    }
  }

  uint64_t RoutingSeed() const { return routingSeed_; }

  void CacheRoutingConfiguration() {
    const detail::RowIndexFileHeader& header = *IndexHeader();
    routeCount_ = header.routeCount;
    routingSeed_ = header.routingSeed;
    expectedAutomaticColumnCount_ = header.expectedAutomaticColumnCount;
    expectedExplicitRowCount_ = header.expectedExplicitRowCount;
    shardCount_ = header.shardCount;
    spillPartitionCount_ = header.spillPartitionCount;
  }

  detail::RowIndexFileHeader* IndexHeader() {
    return static_cast<detail::RowIndexFileHeader*>(indexMap_.Data());
  }

  const detail::RowIndexFileHeader* IndexHeader() const {
    return static_cast<const detail::RowIndexFileHeader*>(indexMap_.Data());
  }

  detail::RowIndexBucket* Buckets() {
    return reinterpret_cast<detail::RowIndexBucket*>(static_cast<std::byte*>(indexMap_.Data()) +
                                                     sizeof(detail::RowIndexFileHeader));
  }

  const detail::RowIndexBucket* Buckets() const {
    return reinterpret_cast<const detail::RowIndexBucket*>(
        static_cast<const std::byte*>(indexMap_.Data()) + sizeof(detail::RowIndexFileHeader));
  }

  void CloseFiles() {
    spillPartitions_.clear();
    rowShards_.clear();
    indexMap_.Unmap();
    indexFile_.Reset();
    nextSequence_.store(1, std::memory_order_relaxed);
    stagedEntryCount_.store(0, std::memory_order_relaxed);
    buildActive_.store(false, std::memory_order_relaxed);
    writePoisoned_.store(false, std::memory_order_relaxed);
    routeCount_ = 0;
    routingSeed_ = detail::kRoutingSeed;
    expectedAutomaticColumnCount_ = 1;
    expectedExplicitRowCount_ = 0;
    shardCount_ = 0;
    spillPartitionCount_ = 0;
  }

  std::filesystem::path directory_;
  std::filesystem::path indexPath_;
  std::filesystem::path markerPath_;
  std::filesystem::path markerTempPath_;
  DatabaseOptions options_;
  detail::FileDescriptor indexFile_;
  detail::MappedFile indexMap_;
  std::vector<std::unique_ptr<RowShard>> rowShards_;
  std::vector<std::unique_ptr<SpillPartition>> spillPartitions_;
  mutable std::shared_mutex lifecycleMutex_;
  mutable std::mutex indexMutex_;
  std::mutex buildMutex_;
  std::atomic<uint64_t> nextSequence_{1};
  std::atomic<uint64_t> stagedEntryCount_{0};
  std::atomic<bool> buildActive_{false};
  std::atomic<bool> writePoisoned_{false};
  uint64_t routeCount_ = 0;
  uint64_t routingSeed_ = detail::kRoutingSeed;
  uint32_t expectedAutomaticColumnCount_ = 1;
  uint64_t expectedExplicitRowCount_ = 0;
  uint32_t shardCount_ = 0;
  uint32_t spillPartitionCount_ = 0;
  bool open_ = false;
  bool readOnly_ = false;
};

Database::Database() : impl_(new Impl()) {}

Database::~Database() { delete impl_; }

Database::Database(Database&& other) noexcept : impl_(std::exchange(other.impl_, nullptr)) {}

Database& Database::operator=(Database&& other) noexcept {
  if (this != &other) {
    delete impl_;
    impl_ = std::exchange(other.impl_, nullptr);
  }
  return *this;
}

Status Database::Open(const std::filesystem::path& directory, const DatabaseOptions& options) {
  if (impl_ == nullptr) {
    impl_ = new Impl();
  }
  return impl_->Open(directory, options);
}

Status Database::OpenReadOnly(const std::filesystem::path& directory) {
  if (impl_ == nullptr) {
    impl_ = new Impl();
  }
  return impl_->OpenReadOnly(directory);
}

Status Database::Close() { return impl_ == nullptr ? Status::Ok() : impl_->Close(); }

Status Database::Flush() {
  return impl_ == nullptr ? Status::NotOpen("database is not open") : impl_->Flush();
}

Status Database::Put(std::string_view column, std::string_view key,
                     std::span<const std::byte> value) {
  return impl_ == nullptr ? Status::NotOpen("database is not open")
                          : impl_->Put(column, key, value);
}

Status Database::Put(std::string_view column, std::string_view key, std::string_view value) {
  return Put(column, key, AsBytes(value));
}

Status Database::Put(std::string_view column, uint64_t rowId, std::string_view key,
                     std::span<const std::byte> value) {
  return impl_ == nullptr ? Status::NotOpen("database is not open")
                          : impl_->Put(column, rowId, key, value);
}

Status Database::Put(std::string_view column, uint64_t rowId, std::string_view key,
                     std::string_view value) {
  return Put(column, rowId, key, AsBytes(value));
}

Status Database::Get(std::string_view column, std::string_view key,
                     std::vector<std::byte>& value) const {
  return impl_ == nullptr ? Status::NotOpen("database is not open")
                          : impl_->Get(column, key, value);
}

Status Database::Get(std::string_view column, std::string_view key, std::string& value) const {
  std::vector<std::byte> bytes;
  Status status = Get(column, key, bytes);
  if (!status) {
    return status;
  }
  value.assign(reinterpret_cast<const char*>(bytes.data()), bytes.size());
  return Status::Ok();
}

Status Database::Get(std::string_view column, uint64_t rowId, std::string_view key,
                     std::vector<std::byte>& value) const {
  return impl_ == nullptr ? Status::NotOpen("database is not open")
                          : impl_->Get(column, rowId, key, value);
}

Status Database::Get(std::string_view column, uint64_t rowId, std::string_view key,
                     std::string& value) const {
  std::vector<std::byte> bytes;
  Status status = Get(column, rowId, key, bytes);
  if (!status) {
    return status;
  }
  value.assign(reinterpret_cast<const char*>(bytes.data()), bytes.size());
  return Status::Ok();
}

bool Database::IsOpen() const { return impl_ != nullptr && impl_->IsOpen(); }

bool Database::HasUncommittedWrites() const {
  return impl_ != nullptr && impl_->HasUncommittedWrites();
}

uint64_t Database::EntryCount() const { return impl_ == nullptr ? 0 : impl_->EntryCount(); }

uint64_t Database::RowCount() const { return impl_ == nullptr ? 0 : impl_->RowCount(); }

DatabaseLayout Database::Layout() const {
  return impl_ == nullptr ? DatabaseLayout{} : impl_->Layout();
}

}  // namespace LumoDB
