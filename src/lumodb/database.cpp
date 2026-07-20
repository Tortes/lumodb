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

#if defined(__aarch64__) && defined(__ARM_NEON)
#include <arm_neon.h>
#elif defined(__SSE2__)
#include <emmintrin.h>
#endif

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
constexpr size_t kMaximumBatchRoutingEntries = 64 * 1024;
constexpr uint32_t kControlGroupWidth = 16;
constexpr uint32_t kMaximumPrefixCount = 254;
constexpr std::array<uint32_t, 3> kCandidatePrefixLengths = {32, 16, 8};
constexpr double kRowLoadFactor = 0.875;
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

uint64_t AlignUp64(uint64_t value) { return (value + 63) & ~uint64_t{63}; }

uint8_t KeyFingerprint(uint64_t keyHash) {
  const uint8_t fingerprint = static_cast<uint8_t>(keyHash >> 56);
  return fingerprint == 0 ? 1 : fingerprint;
}

uint16_t ControlMatchMask(const std::byte* controls, uint8_t value) {
#if defined(__aarch64__) && defined(__ARM_NEON)
  const uint8x16_t group = vld1q_u8(reinterpret_cast<const uint8_t*>(controls));
  const uint8x16_t matches = vceqq_u8(group, vdupq_n_u8(value));
  const uint8x16_t bits = vshrq_n_u8(matches, 7);
  static constexpr std::array<uint8_t, 16> kBitWeights = {1, 2, 4, 8, 16, 32, 64, 128,
                                                          1, 2, 4, 8, 16, 32, 64, 128};
  const uint8x16_t weighted =
      vmulq_u8(bits, vld1q_u8(reinterpret_cast<const uint8_t*>(kBitWeights.data())));
  const uint16_t low = vaddv_u8(vget_low_u8(weighted));
  const uint16_t high = vaddv_u8(vget_high_u8(weighted));
  return static_cast<uint16_t>(low | (high << 8));
#elif defined(__SSE2__)
  const __m128i group =
      _mm_loadu_si128(reinterpret_cast<const __m128i*>(static_cast<const void*>(controls)));
  const __m128i matches = _mm_cmpeq_epi8(group, _mm_set1_epi8(static_cast<char>(value)));
  return static_cast<uint16_t>(_mm_movemask_epi8(matches));
#else
  uint16_t matches = 0;
  for (uint32_t lane = 0; lane < kControlGroupWidth; ++lane) {
    if (std::to_integer<uint8_t>(controls[lane]) == value) {
      matches |= static_cast<uint16_t>(1U << lane);
    }
  }
  return matches;
#endif
}

uint16_t InterleavedControlMatchMask(const std::byte* buckets, uint8_t value) {
#if defined(__aarch64__) && defined(__ARM_NEON)
  // vld4 deinterleaves sixteen {control, offset[0], offset[1], offset[2]}
  // buckets while loading exactly one 64-byte cache line.
  const uint8x16x4_t group = vld4q_u8(reinterpret_cast<const uint8_t*>(buckets));
  const uint8x16_t matches = vceqq_u8(group.val[0], vdupq_n_u8(value));
  const uint8x16_t bits = vshrq_n_u8(matches, 7);
  static constexpr std::array<uint8_t, 16> kBitWeights = {1, 2, 4, 8, 16, 32, 64, 128,
                                                          1, 2, 4, 8, 16, 32, 64, 128};
  const uint8x16_t weighted =
      vmulq_u8(bits, vld1q_u8(reinterpret_cast<const uint8_t*>(kBitWeights.data())));
  const uint16_t low = vaddv_u8(vget_low_u8(weighted));
  const uint16_t high = vaddv_u8(vget_high_u8(weighted));
  return static_cast<uint16_t>(low | (high << 8));
#else
  uint16_t matches = 0;
  for (uint32_t lane = 0; lane < kControlGroupWidth; ++lane) {
    if (std::to_integer<uint8_t>(buckets[lane * 4]) == value) {
      matches |= static_cast<uint16_t>(1U << lane);
    }
  }
  return matches;
#endif
}

uint32_t PackedByteCount(uint32_t symbolCount, uint8_t bitsPerSymbol) {
  return static_cast<uint32_t>((static_cast<uint64_t>(symbolCount) * bitsPerSymbol + 7) / 8);
}

void WriteRecordOffset(std::byte* output, uint32_t value, uint8_t width) {
  output[0] = static_cast<std::byte>(value & 0xff);
  output[1] = static_cast<std::byte>((value >> 8) & 0xff);
  output[2] = static_cast<std::byte>((value >> 16) & 0xff);
  if (width == 4) {
    output[3] = static_cast<std::byte>((value >> 24) & 0xff);
  }
}

uint32_t ReadRecordOffset(const std::byte* input, uint8_t width) {
  uint32_t value = std::to_integer<uint8_t>(input[0]) |
                   (static_cast<uint32_t>(std::to_integer<uint8_t>(input[1])) << 8) |
                   (static_cast<uint32_t>(std::to_integer<uint8_t>(input[2])) << 16);
  if (width == 4) {
    value |= static_cast<uint32_t>(std::to_integer<uint8_t>(input[3])) << 24;
  }
  return value;
}

size_t Varint32Size(uint32_t value) {
  size_t size = 1;
  while (value >= 0x80) {
    value >>= 7;
    ++size;
  }
  return size;
}

size_t WriteVarint32(std::byte* output, uint32_t value) {
  size_t size = 0;
  while (value >= 0x80) {
    output[size++] = static_cast<std::byte>((value & 0x7f) | 0x80);
    value >>= 7;
  }
  output[size++] = static_cast<std::byte>(value);
  return size;
}

bool ReadVarint32(const std::byte* data, size_t size, size_t& cursor, uint32_t& value) {
  value = 0;
  for (uint32_t shift = 0; shift <= 28; shift += 7) {
    if (cursor >= size) {
      return false;
    }
    const uint32_t byte = std::to_integer<uint8_t>(data[cursor++]);
    if (shift == 28 && (byte & 0xf0) != 0) {
      return false;
    }
    value |= (byte & 0x7f) << shift;
    if ((byte & 0x80) == 0) {
      return true;
    }
  }
  return false;
}

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

  Status PutStructs(std::string_view column, std::span<const StructEntry> entries) {
    std::shared_lock lifecycleLock(lifecycleMutex_);
    Status status = ValidateWritableState();
    if (!status) {
      return status;
    }
    if (entries.empty()) {
      return Status::Ok();
    }
    for (const StructEntry& entry : entries) {
      status = ValidateStageRecord(column, entry.key, entry.flatBufferBytes);
      if (!status) {
        return status;
      }
    }

    status = BeginBuild();
    if (!status) {
      writePoisoned_.store(true, std::memory_order_release);
      return status;
    }

    struct RoutedEntry {
      size_t index = 0;
      uint64_t keyHash = 0;
      uint64_t rowId = 0;
    };
    const uint64_t columnHash = NormalizeHash(detail::HashString(column));
    std::vector<std::vector<RoutedEntry>> partitionEntries(spillPartitions_.size());
    const size_t maximumChunkSize = std::min(kMaximumBatchRoutingEntries, entries.size());
    const size_t reservePerPartition = maximumChunkSize / spillPartitions_.size() + 1;
    for (auto& partitionGroup : partitionEntries) {
      partitionGroup.reserve(reservePerPartition);
    }

    for (size_t chunkBegin = 0; chunkBegin < entries.size();) {
      const size_t chunkSize = std::min(kMaximumBatchRoutingEntries, entries.size() - chunkBegin);
      const size_t chunkEnd = chunkBegin + chunkSize;
      for (size_t index = chunkBegin; index < chunkEnd; ++index) {
        const uint64_t keyHash = NormalizeHash(detail::HashString(entries[index].key));
        const uint64_t routeHash =
            detail::MixHashes(detail::MixHashes(columnHash, keyHash), RoutingSeed());
        const uint64_t rowId = routeHash % routeCount_;
        const uint32_t partitionId =
            static_cast<uint32_t>(detail::MixHashes(columnHash, rowId) % spillPartitions_.size());
        partitionEntries[partitionId].push_back(
            RoutedEntry{.index = index, .keyHash = keyHash, .rowId = rowId});
      }

      for (uint32_t partitionId = 0; partitionId < partitionEntries.size(); ++partitionId) {
        std::vector<RoutedEntry>& routedEntries = partitionEntries[partitionId];
        if (routedEntries.empty()) {
          continue;
        }
        std::stable_sort(
            routedEntries.begin(), routedEntries.end(),
            [](const RoutedEntry& lhs, const RoutedEntry& rhs) { return lhs.rowId < rhs.rowId; });
        SpillPartition& partition = *spillPartitions_[partitionId];
        std::lock_guard partitionLock(partition.mutex);
        std::vector<StageInput> stageEntries;
        size_t groupBegin = 0;
        while (groupBegin < routedEntries.size()) {
          size_t groupEnd = groupBegin + 1;
          while (groupEnd < routedEntries.size() &&
                 routedEntries[groupEnd].rowId == routedEntries[groupBegin].rowId) {
            ++groupEnd;
          }
          stageEntries.clear();
          stageEntries.reserve(groupEnd - groupBegin);
          for (size_t index = groupBegin; index < groupEnd; ++index) {
            const RoutedEntry& routed = routedEntries[index];
            const StructEntry& entry = entries[routed.index];
            stageEntries.push_back(StageInput{
                .key = entry.key, .value = entry.flatBufferBytes, .keyHash = routed.keyHash});
          }
          status = StageChunkLocked(partitionId, partition, column, columnHash,
                                    routedEntries[groupBegin].rowId, stageEntries);
          if (!status) {
            writePoisoned_.store(true, std::memory_order_release);
            return status;
          }
          groupBegin = groupEnd;
        }
      }
      for (auto& partitionGroup : partitionEntries) {
        partitionGroup.clear();
      }
      chunkBegin = chunkEnd;
    }
    return Status::Ok();
  }

  Status PutRowStructs(std::string_view column, uint64_t rowId,
                       std::span<const RowStructEntry> entries) {
    std::shared_lock lifecycleLock(lifecycleMutex_);
    Status status = ValidateWritableState();
    if (!status) {
      return status;
    }
    if (rowId >= kExplicitRowBit) {
      return Status::InvalidArgument("explicit rowId must be smaller than 2^63");
    }
    if (entries.empty()) {
      return Status::InvalidArgument("row entries must not be empty");
    }
    for (const RowStructEntry& entry : entries) {
      status = ValidateStageRecord(column, entry.key, entry.flatBufferBytes);
      if (!status) {
        return status;
      }
    }

    status = BeginBuild();
    if (!status) {
      writePoisoned_.store(true, std::memory_order_release);
      return status;
    }

    const uint64_t columnHash = NormalizeHash(detail::HashString(column));
    const uint64_t internalRowId = rowId | kExplicitRowBit;
    const uint32_t partitionId = static_cast<uint32_t>(
        detail::MixHashes(columnHash, internalRowId) % spillPartitions_.size());
    SpillPartition& partition = *spillPartitions_[partitionId];
    std::lock_guard partitionLock(partition.mutex);
    std::vector<StageInput> stageEntries;
    stageEntries.reserve(std::min(kMaximumBatchRoutingEntries, entries.size()));
    for (size_t chunkBegin = 0; chunkBegin < entries.size();
         chunkBegin += kMaximumBatchRoutingEntries) {
      const size_t chunkEnd = std::min(entries.size(), chunkBegin + kMaximumBatchRoutingEntries);
      stageEntries.clear();
      for (size_t index = chunkBegin; index < chunkEnd; ++index) {
        const RowStructEntry& entry = entries[index];
        stageEntries.push_back(StageInput{.key = entry.key,
                                          .value = entry.flatBufferBytes,
                                          .keyHash = NormalizeHash(detail::HashString(entry.key))});
      }
      status =
          StageChunkLocked(partitionId, partition, column, columnHash, internalRowId, stageEntries);
      if (!status) {
        writePoisoned_.store(true, std::memory_order_release);
        return status;
      }
    }
    return Status::Ok();
  }

  Status PutInternal(std::string_view column, uint64_t requestedRowId, bool explicitRow,
                     std::string_view key, std::span<const std::byte> value) {
    std::shared_lock lifecycleLock(lifecycleMutex_);
    Status status = ValidateWritableState();
    if (!status) {
      return status;
    }
    if (explicitRow && requestedRowId >= kExplicitRowBit) {
      return Status::InvalidArgument("explicit rowId must be smaller than 2^63");
    }
    status = ValidateStageRecord(column, key, value);
    if (!status) {
      return status;
    }

    status = BeginBuild();
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

    SpillPartition& partition = *spillPartitions_[partitionId];
    std::lock_guard partitionLock(partition.mutex);
    const std::array<StageInput, 1> entry = {
        StageInput{.key = key, .value = value, .keyHash = keyHash}};
    status = StageChunkLocked(partitionId, partition, column, columnHash, rowId, entry);
    if (!status) {
      writePoisoned_.store(true, std::memory_order_release);
    }
    return status;
  }

  Status ValidateWritableState() const {
    if (!open_) {
      return Status::NotOpen("database is not open");
    }
    if (readOnly_) {
      return Status::InvalidArgument("database is read-only");
    }
    if (writePoisoned_.load(std::memory_order_acquire)) {
      return Status::Corruption("write batch failed; delete the output directory and rebuild it");
    }
    if (options_.oneShotBuild && !buildActive_.load(std::memory_order_acquire) &&
        IndexHeader()->itemCount != 0) {
      return Status::InvalidArgument("oneShotBuild accepts no writes after its first Flush");
    }
    return Status::Ok();
  }

  Status ValidateStageRecord(std::string_view column, std::string_view key,
                             std::span<const std::byte> value) const {
    if (column.empty()) {
      return Status::InvalidArgument("column must not be empty");
    }
    if (key.empty()) {
      return Status::InvalidArgument("key must not be empty");
    }
    if (column.size() > std::numeric_limits<uint32_t>::max() ||
        key.size() > std::numeric_limits<uint32_t>::max() ||
        value.size() > std::numeric_limits<uint32_t>::max()) {
      return Status::InvalidArgument("column, key, and value must each be smaller than 4 GiB");
    }
    uint64_t recordSize = sizeof(detail::StageChunkHeader) + sizeof(uint64_t) + 10;
    if (!CheckedAdd(recordSize, column.size(), recordSize) ||
        !CheckedAdd(recordSize, key.size(), recordSize) ||
        !CheckedAdd(recordSize, value.size(), recordSize) ||
        recordSize > std::numeric_limits<size_t>::max()) {
      return Status::InvalidArgument("staged record is too large");
    }
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

    const uint64_t metadataOffset =
        AlignUp8(sizeof(detail::RowBlockHeader) + lookup.header.columnSize);
    const uint64_t localIndexOffset =
        AlignUp64(metadataOffset + lookup.header.keyMetadataBytesSize);
    const uint64_t offsetsOffset = localIndexOffset + lookup.header.bucketCount;
    const uint64_t recordsOffset =
        localIndexOffset +
        static_cast<uint64_t>(lookup.header.bucketCount) * (lookup.header.recordOffsetWidth + 1);
    const RowShard& shard = *rowShards_[lookup.bucket.shardId];
    const std::byte* mappedBlock = nullptr;
    if (shard.map.IsMapped()) {
      if (lookup.bucket.blockOffset > shard.map.Size() ||
          lookup.header.blockSize > shard.map.Size() - lookup.bucket.blockOffset) {
        return Status::Corruption("row block extends beyond its value file");
      }
      mappedBlock = static_cast<const std::byte*>(shard.map.Data()) + lookup.bucket.blockOffset;
    }

    std::vector<std::byte> ownedMetadata;
    std::span<const std::byte> metadataBytes;
    if (mappedBlock != nullptr) {
      metadataBytes = {mappedBlock + metadataOffset, lookup.header.keyMetadataBytesSize};
    } else {
      ownedMetadata.resize(lookup.header.keyMetadataBytesSize);
      status = ReadRowAt(lookup.bucket.shardId, ownedMetadata.data(), ownedMetadata.size(),
                         lookup.bucket.blockOffset + metadataOffset);
      if (!status) {
        return status;
      }
      metadataBytes = ownedMetadata;
    }
    KeyMetadataView metadata;
    status = ParseKeyMetadata(metadataBytes, metadata);
    if (!status) {
      return status;
    }

    const uint8_t fingerprint = KeyFingerprint(keyHash);
    const uint32_t groupCount = lookup.header.bucketCount / kControlGroupWidth;
    const uint32_t startGroup = static_cast<uint32_t>(keyHash) & (groupCount - 1);
    std::array<std::byte, kControlGroupWidth * 4> ownedIndexGroup{};
    std::array<std::byte, 4> ownedOffset{};
    std::array<std::byte, 11> ownedRecordHeader{};
    std::vector<std::byte> ownedEncodedSuffix;
    for (uint32_t groupProbe = 0; groupProbe < groupCount; ++groupProbe) {
      const uint32_t groupIndex = (startGroup + groupProbe) & (groupCount - 1);
      const uint32_t groupBase = groupIndex * kControlGroupWidth;
      const bool interleaved = lookup.header.recordOffsetWidth == 3;
      const size_t groupBytes = interleaved ? ownedIndexGroup.size() : kControlGroupWidth;
      const uint64_t groupOffset =
          localIndexOffset + static_cast<uint64_t>(groupBase) * (interleaved ? 4 : 1);
      const std::byte* groupData = nullptr;
      if (mappedBlock != nullptr) {
        groupData = mappedBlock + groupOffset;
      } else {
        status = ReadRowAt(lookup.bucket.shardId, ownedIndexGroup.data(), groupBytes,
                           lookup.bucket.blockOffset + groupOffset);
        if (!status) {
          return status;
        }
        groupData = ownedIndexGroup.data();
      }
      uint16_t matchingLanes = interleaved ? InterleavedControlMatchMask(groupData, fingerprint)
                                           : ControlMatchMask(groupData, fingerprint);
      while (matchingLanes != 0) {
        const uint32_t lane = std::countr_zero(matchingLanes);
        matchingLanes &= static_cast<uint16_t>(matchingLanes - 1);
        const uint32_t bucketIndex = groupBase + lane;
        const std::byte* offsetBytes = nullptr;
        if (interleaved) {
          offsetBytes = groupData + lane * 4 + 1;
        } else if (mappedBlock != nullptr) {
          offsetBytes = mappedBlock + offsetsOffset +
                        static_cast<uint64_t>(bucketIndex) * lookup.header.recordOffsetWidth;
        } else {
          status =
              ReadRowAt(lookup.bucket.shardId, ownedOffset.data(), lookup.header.recordOffsetWidth,
                        lookup.bucket.blockOffset + offsetsOffset +
                            static_cast<uint64_t>(bucketIndex) * lookup.header.recordOffsetWidth);
          if (!status) {
            return status;
          }
          offsetBytes = ownedOffset.data();
        }
        const uint32_t recordOffset =
            ReadRecordOffset(offsetBytes, lookup.header.recordOffsetWidth);
        RowRecordView record;
        if (mappedBlock != nullptr) {
          status = ParseRowRecord(std::span<const std::byte>(mappedBlock + recordsOffset,
                                                             lookup.header.recordBytesSize),
                                  recordOffset, metadata, record);
        } else {
          if (recordOffset >= lookup.header.recordBytesSize) {
            return Status::Corruption("row bucket points outside its record region");
          }
          const size_t headerBytes = std::min<size_t>(ownedRecordHeader.size(),
                                                      lookup.header.recordBytesSize - recordOffset);
          status = ReadRowAt(lookup.bucket.shardId, ownedRecordHeader.data(), headerBytes,
                             lookup.bucket.blockOffset + recordsOffset + recordOffset);
          if (!status) {
            return status;
          }
          size_t cursor = 0;
          if (!ReadVarint32(ownedRecordHeader.data(), headerBytes, cursor, record.suffixSize) ||
              !ReadVarint32(ownedRecordHeader.data(), headerBytes, cursor, record.valueSize)) {
            return Status::Corruption("row record has invalid lengths");
          }
          if (metadata.header.prefixCount != 0) {
            if (cursor >= headerBytes) {
              return Status::Corruption("row record has no prefix ID");
            }
            record.prefixId = std::to_integer<uint8_t>(ownedRecordHeader[cursor++]);
            if (record.prefixId > metadata.header.prefixCount) {
              return Status::Corruption("row record has an invalid prefix ID");
            }
          }
          record.encodedSuffixSize =
              PackedByteCount(record.suffixSize, metadata.header.bitsPerSymbol);
          const uint64_t recordEnd = static_cast<uint64_t>(recordOffset) + cursor +
                                     record.encodedSuffixSize + record.valueSize;
          if (recordEnd > lookup.header.recordBytesSize) {
            return Status::Corruption("row record extends beyond its record region");
          }
          ownedEncodedSuffix.resize(record.encodedSuffixSize);
          status =
              ReadRowAt(lookup.bucket.shardId, ownedEncodedSuffix.data(), ownedEncodedSuffix.size(),
                        lookup.bucket.blockOffset + recordsOffset + recordOffset + cursor);
          if (!status) {
            return status;
          }
          record.encodedSuffix = ownedEncodedSuffix.data();
          record.value = nullptr;
          record.endOffset = static_cast<uint32_t>(recordEnd);
        }
        if (!status) {
          return status;
        }
        bool matches = false;
        status = RecordKeyMatches(metadata, record, key, matches);
        if (!status) {
          return status;
        }
        if (!matches) {
          continue;
        }
        value.resize(record.valueSize);
        if (mappedBlock != nullptr) {
          if (record.valueSize != 0) {
            std::memcpy(value.data(), record.value, record.valueSize);
          }
          return Status::Ok();
        }
        const uint64_t valueOffset = record.endOffset - record.valueSize;
        return ReadRowAt(lookup.bucket.shardId, value.data(), value.size(),
                         lookup.bucket.blockOffset + recordsOffset + valueOffset);
      }
      const uint16_t emptyLanes =
          interleaved ? InterleavedControlMatchMask(groupData, 0) : ControlMatchMask(groupData, 0);
      if (emptyLanes != 0) {
        return Status::NotFound("key not found");
      }
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
    bool spilled = false;
  };

  struct StageInput {
    std::string_view key;
    std::span<const std::byte> value;
    uint64_t keyHash = 0;
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

  struct KeyEncodingPlan {
    uint8_t bitsPerSymbol = 8;
    std::vector<uint8_t> alphabet;
    std::array<uint8_t, 256> symbolCodes{};
    std::vector<std::string> prefixes;
    std::vector<uint8_t> prefixIdsByEntry;
    uint64_t encodedBytes = 0;
  };

  struct KeyMetadataView {
    detail::RowKeyMetadataHeader header;
    const uint8_t* alphabet = nullptr;
    const std::byte* prefixOffsets = nullptr;
    const char* prefixBytes = nullptr;
  };

  struct RowRecordView {
    uint8_t prefixId = 0;
    uint32_t suffixSize = 0;
    uint32_t valueSize = 0;
    const std::byte* encodedSuffix = nullptr;
    uint32_t encodedSuffixSize = 0;
    const std::byte* value = nullptr;
    uint32_t endOffset = 0;
  };

  uint64_t KeyMetadataSize(const KeyEncodingPlan& plan) const {
    uint64_t size = sizeof(detail::RowKeyMetadataHeader) + plan.alphabet.size();
    if (!plan.prefixes.empty()) {
      size += (plan.prefixes.size() + 1) * sizeof(uint16_t);
      for (const std::string& prefix : plan.prefixes) {
        size += prefix.size();
      }
    }
    return size;
  }

  std::string_view EntrySuffix(const KeyEncodingPlan& plan, size_t entryIndex,
                               std::string_view key) const {
    const uint8_t prefixId = plan.prefixIdsByEntry.empty() ? 0 : plan.prefixIdsByEntry[entryIndex];
    if (prefixId == 0) {
      return key;
    }
    return key.substr(plan.prefixes[prefixId - 1].size());
  }

  size_t EncodingEntryCount(std::span<const EntryView> entries,
                            std::span<const uint32_t> uniqueEntryIndices) const {
    return uniqueEntryIndices.empty() ? entries.size() : uniqueEntryIndices.size();
  }

  size_t EncodingEntryIndex(size_t position, std::span<const uint32_t> uniqueEntryIndices) const {
    return uniqueEntryIndices.empty() ? position : uniqueEntryIndices[position];
  }

  uint64_t MeasureEncoding(const KeyEncodingPlan& plan, std::span<const EntryView> entries,
                           std::span<const uint32_t> uniqueEntryIndices,
                           uint64_t* keyBytes = nullptr) const {
    uint64_t total = KeyMetadataSize(plan);
    if (keyBytes != nullptr) {
      *keyBytes = 0;
    }
    auto add = [&](uint64_t bytes) {
      if (total > std::numeric_limits<uint64_t>::max() - bytes) {
        total = std::numeric_limits<uint64_t>::max();
        return false;
      }
      total += bytes;
      return true;
    };
    const size_t entryCount = EncodingEntryCount(entries, uniqueEntryIndices);
    for (size_t position = 0; position < entryCount; ++position) {
      const size_t entryIndex = EncodingEntryIndex(position, uniqueEntryIndices);
      const EntryView& entry = entries[entryIndex];
      if (keyBytes != nullptr &&
          !CheckedAdd(*keyBytes, static_cast<uint64_t>(entry.key.size()), *keyBytes)) {
        *keyBytes = std::numeric_limits<uint64_t>::max();
      }
      const std::string_view suffix = EntrySuffix(plan, entryIndex, entry.key);
      if (!add(Varint32Size(static_cast<uint32_t>(suffix.size()))) ||
          !add(Varint32Size(static_cast<uint32_t>(entry.value.size()))) ||
          !add(plan.prefixes.empty() ? 0 : 1) ||
          !add(PackedByteCount(static_cast<uint32_t>(suffix.size()), plan.bitsPerSymbol)) ||
          !add(entry.value.size())) {
        return total;
      }
    }
    return total;
  }

  bool AddAlphabet(KeyEncodingPlan& plan, std::span<const EntryView> entries,
                   std::span<const uint32_t> uniqueEntryIndices) const {
    std::array<bool, 256> present{};
    uint32_t symbolCount = 0;
    const size_t entryCount = EncodingEntryCount(entries, uniqueEntryIndices);
    for (size_t position = 0; position < entryCount; ++position) {
      const size_t entryIndex = EncodingEntryIndex(position, uniqueEntryIndices);
      const std::string_view suffix = EntrySuffix(plan, entryIndex, entries[entryIndex].key);
      for (const unsigned char byte : suffix) {
        if (!present[byte]) {
          present[byte] = true;
          ++symbolCount;
          if (symbolCount > 64) {
            return false;
          }
        }
      }
    }
    if (symbolCount == 0) {
      return false;
    }
    plan.bitsPerSymbol = symbolCount <= 16 ? 4 : 6;
    plan.alphabet.reserve(symbolCount);
    plan.symbolCodes.fill(0xff);
    for (uint32_t byte = 0; byte < present.size(); ++byte) {
      if (present[byte]) {
        plan.symbolCodes[byte] = static_cast<uint8_t>(plan.alphabet.size());
        plan.alphabet.push_back(static_cast<uint8_t>(byte));
      }
    }
    return true;
  }

  KeyEncodingPlan ChooseKeyEncoding(std::span<const EntryView> entries,
                                    std::span<const uint32_t> uniqueEntryIndices) const {
    KeyEncodingPlan raw;
    raw.symbolCodes.fill(0xff);
    uint64_t keyBytes = 0;
    raw.encodedBytes = MeasureEncoding(raw, entries, uniqueEntryIndices, &keyBytes);
    const uint64_t uniqueCount = EncodingEntryCount(entries, uniqueEntryIndices);
    // Short keys are already compact. Keeping them raw avoids paying decode
    // CPU for only a few bytes of potential savings.
    if (uniqueCount == 0 || keyBytes < uniqueCount * 16) {
      return raw;
    }
    KeyEncodingPlan best = raw;

    KeyEncodingPlan packed = raw;
    if (AddAlphabet(packed, entries, uniqueEntryIndices)) {
      packed.encodedBytes = MeasureEncoding(packed, entries, uniqueEntryIndices);
      if (packed.encodedBytes < best.encodedBytes) {
        best = packed;
      }
    }

    std::unordered_map<std::string_view, uint32_t> prefixCounts;
    prefixCounts.reserve(static_cast<size_t>(std::min<uint64_t>(uniqueCount * 2, 128ULL * 1024)));
    for (size_t position = 0; position < uniqueCount; ++position) {
      const size_t entryIndex = EncodingEntryIndex(position, uniqueEntryIndices);
      const std::string_view key = entries[entryIndex].key;
      for (const uint32_t length : kCandidatePrefixLengths) {
        if (key.size() > length) {
          ++prefixCounts[key.substr(0, length)];
        }
      }
    }
    struct PrefixCandidate {
      std::string prefix;
      uint64_t score = 0;
    };
    std::vector<PrefixCandidate> candidates;
    candidates.reserve(prefixCounts.size());
    for (const auto& [prefix, count] : prefixCounts) {
      const uint64_t gross = static_cast<uint64_t>(count) * (prefix.size() - 1);
      const uint64_t metadataCost = prefix.size() + sizeof(uint16_t) * 2;
      if (count >= 2 && gross > metadataCost) {
        candidates.push_back(
            PrefixCandidate{.prefix = std::string(prefix), .score = gross - metadataCost});
      }
    }
    std::sort(candidates.begin(), candidates.end(),
              [](const PrefixCandidate& lhs, const PrefixCandidate& rhs) {
                if (lhs.score != rhs.score) {
                  return lhs.score > rhs.score;
                }
                if (lhs.prefix.size() != rhs.prefix.size()) {
                  return lhs.prefix.size() > rhs.prefix.size();
                }
                return lhs.prefix < rhs.prefix;
              });
    if (candidates.size() > kMaximumPrefixCount) {
      candidates.resize(kMaximumPrefixCount);
    }

    if (!candidates.empty()) {
      std::array<std::unordered_map<std::string_view, uint8_t>, kCandidatePrefixLengths.size()>
          selectedByLength;
      KeyEncodingPlan prefixPlan;
      prefixPlan.symbolCodes.fill(0xff);
      prefixPlan.prefixIdsByEntry.resize(entries.size(), 0);
      prefixPlan.prefixes.reserve(candidates.size());
      for (const PrefixCandidate& candidate : candidates) {
        prefixPlan.prefixes.push_back(candidate.prefix);
      }
      for (size_t candidateIndex = 0; candidateIndex < candidates.size(); ++candidateIndex) {
        const std::string& prefix = prefixPlan.prefixes[candidateIndex];
        const uint8_t id = static_cast<uint8_t>(candidateIndex + 1);
        for (size_t lengthIndex = 0; lengthIndex < kCandidatePrefixLengths.size(); ++lengthIndex) {
          if (prefix.size() == kCandidatePrefixLengths[lengthIndex]) {
            selectedByLength[lengthIndex].emplace(prefix, id);
            break;
          }
        }
      }
      std::vector<bool> used(prefixPlan.prefixes.size(), false);
      for (size_t position = 0; position < uniqueCount; ++position) {
        const size_t entryIndex = EncodingEntryIndex(position, uniqueEntryIndices);
        const std::string_view key = entries[entryIndex].key;
        for (size_t lengthIndex = 0; lengthIndex < kCandidatePrefixLengths.size(); ++lengthIndex) {
          const uint32_t length = kCandidatePrefixLengths[lengthIndex];
          if (key.size() <= length) {
            continue;
          }
          const auto found = selectedByLength[lengthIndex].find(key.substr(0, length));
          if (found != selectedByLength[lengthIndex].end()) {
            prefixPlan.prefixIdsByEntry[entryIndex] = found->second;
            used[found->second - 1] = true;
            break;
          }
        }
      }

      std::vector<uint8_t> remap(prefixPlan.prefixes.size() + 1, 0);
      std::vector<std::string> usedPrefixes;
      usedPrefixes.reserve(prefixPlan.prefixes.size());
      for (size_t index = 0; index < prefixPlan.prefixes.size(); ++index) {
        if (used[index]) {
          remap[index + 1] = static_cast<uint8_t>(usedPrefixes.size() + 1);
          usedPrefixes.push_back(std::move(prefixPlan.prefixes[index]));
        }
      }
      for (uint8_t& id : prefixPlan.prefixIdsByEntry) {
        id = remap[id];
      }
      prefixPlan.prefixes = std::move(usedPrefixes);
      prefixPlan.encodedBytes = MeasureEncoding(prefixPlan, entries, uniqueEntryIndices);
      if (prefixPlan.encodedBytes < best.encodedBytes) {
        best = prefixPlan;
      }
      KeyEncodingPlan prefixPacked = prefixPlan;
      if (AddAlphabet(prefixPacked, entries, uniqueEntryIndices)) {
        prefixPacked.encodedBytes = MeasureEncoding(prefixPacked, entries, uniqueEntryIndices);
        if (prefixPacked.encodedBytes < best.encodedBytes) {
          best = std::move(prefixPacked);
        }
      }
    }

    // Encoding must pay for its read-side branch and metadata. Small wins stay
    // in the raw format; larger wins are selected independently for each row.
    if (best.encodedBytes > raw.encodedBytes - raw.encodedBytes / 10 ||
        raw.encodedBytes - best.encodedBytes < uniqueCount * 8) {
      return raw;
    }
    return best;
  }

  Status SerializeKeyMetadata(const KeyEncodingPlan& plan, std::vector<std::byte>& output) const {
    uint64_t prefixBytesSize = 0;
    for (const std::string& prefix : plan.prefixes) {
      prefixBytesSize += prefix.size();
    }
    if (plan.prefixes.size() > kMaximumPrefixCount ||
        prefixBytesSize > std::numeric_limits<uint16_t>::max()) {
      return Status::InvalidArgument("row key prefix dictionary is too large");
    }
    detail::RowKeyMetadataHeader header;
    header.bitsPerSymbol = plan.bitsPerSymbol;
    header.alphabetSize = static_cast<uint8_t>(plan.alphabet.size());
    header.prefixCount = static_cast<uint16_t>(plan.prefixes.size());
    header.prefixBytesSize = static_cast<uint32_t>(prefixBytesSize);
    AppendRaw(output, &header, sizeof(header));
    AppendRaw(output, plan.alphabet.data(), plan.alphabet.size());
    if (!plan.prefixes.empty()) {
      uint16_t offset = 0;
      AppendRaw(output, &offset, sizeof(offset));
      for (const std::string& prefix : plan.prefixes) {
        offset = static_cast<uint16_t>(offset + prefix.size());
        AppendRaw(output, &offset, sizeof(offset));
      }
      for (const std::string& prefix : plan.prefixes) {
        AppendRaw(output, prefix.data(), prefix.size());
      }
    }
    return Status::Ok();
  }

  Status ParseKeyMetadata(std::span<const std::byte> metadata, KeyMetadataView& view) const {
    if (metadata.size() < sizeof(detail::RowKeyMetadataHeader)) {
      return Status::Corruption("row key metadata is truncated");
    }
    std::memcpy(&view.header, metadata.data(), sizeof(view.header));
    if ((view.header.bitsPerSymbol != 4 && view.header.bitsPerSymbol != 6 &&
         view.header.bitsPerSymbol != 8) ||
        view.header.prefixCount > kMaximumPrefixCount ||
        (view.header.bitsPerSymbol == 8 && view.header.alphabetSize != 0) ||
        (view.header.bitsPerSymbol == 4 &&
         (view.header.alphabetSize == 0 || view.header.alphabetSize > 16)) ||
        (view.header.bitsPerSymbol == 6 &&
         (view.header.alphabetSize <= 16 || view.header.alphabetSize > 64)) ||
        (view.header.prefixCount == 0 && view.header.prefixBytesSize != 0)) {
      return Status::Corruption("row key metadata header is invalid");
    }
    uint64_t expected = sizeof(view.header) + view.header.alphabetSize;
    if (view.header.prefixCount != 0) {
      expected += static_cast<uint64_t>(view.header.prefixCount + 1) * sizeof(uint16_t);
    }
    expected += view.header.prefixBytesSize;
    if (expected != metadata.size()) {
      return Status::Corruption("row key metadata size is invalid");
    }
    view.alphabet = reinterpret_cast<const uint8_t*>(metadata.data() + sizeof(view.header));
    const std::byte* cursor = metadata.data() + sizeof(view.header) + view.header.alphabetSize;
    view.prefixOffsets = view.header.prefixCount == 0 ? nullptr : cursor;
    if (view.header.prefixCount != 0) {
      uint16_t previous = 0;
      for (uint32_t index = 0; index <= view.header.prefixCount; ++index) {
        uint16_t current = 0;
        std::memcpy(&current, cursor + index * sizeof(uint16_t), sizeof(current));
        if ((index == 0 && current != 0) || current < previous ||
            current > view.header.prefixBytesSize) {
          return Status::Corruption("row key prefix offsets are invalid");
        }
        previous = current;
      }
      if (previous != view.header.prefixBytesSize) {
        return Status::Corruption("row key prefix bytes are incomplete");
      }
      cursor += static_cast<size_t>(view.header.prefixCount + 1) * sizeof(uint16_t);
    }
    view.prefixBytes = reinterpret_cast<const char*>(cursor);
    return Status::Ok();
  }

  std::string_view PrefixAt(const KeyMetadataView& metadata, uint8_t prefixId) const {
    if (prefixId == 0) {
      return {};
    }
    uint16_t begin = 0;
    uint16_t end = 0;
    std::memcpy(&begin, metadata.prefixOffsets + (prefixId - 1) * sizeof(uint16_t), sizeof(begin));
    std::memcpy(&end, metadata.prefixOffsets + prefixId * sizeof(uint16_t), sizeof(end));
    return {metadata.prefixBytes + begin, static_cast<size_t>(end - begin)};
  }

  Status ParseRowRecord(std::span<const std::byte> records, uint32_t recordOffset,
                        const KeyMetadataView& metadata, RowRecordView& record) const {
    if (recordOffset >= records.size()) {
      return Status::Corruption("row bucket points outside its record region");
    }
    size_t cursor = recordOffset;
    if (!ReadVarint32(records.data(), records.size(), cursor, record.suffixSize) ||
        !ReadVarint32(records.data(), records.size(), cursor, record.valueSize)) {
      return Status::Corruption("row record has invalid lengths");
    }
    if (metadata.header.prefixCount != 0) {
      if (cursor >= records.size()) {
        return Status::Corruption("row record has no prefix ID");
      }
      record.prefixId = std::to_integer<uint8_t>(records[cursor++]);
      if (record.prefixId > metadata.header.prefixCount) {
        return Status::Corruption("row record has an invalid prefix ID");
      }
    }
    record.encodedSuffixSize = PackedByteCount(record.suffixSize, metadata.header.bitsPerSymbol);
    const uint64_t end =
        static_cast<uint64_t>(cursor) + record.encodedSuffixSize + record.valueSize;
    if (end > records.size()) {
      return Status::Corruption("row record extends beyond its record region");
    }
    record.encodedSuffix = records.data() + cursor;
    record.value = record.encodedSuffix + record.encodedSuffixSize;
    record.endOffset = static_cast<uint32_t>(end);
    return Status::Ok();
  }

  uint8_t PackedSymbolAt(const std::byte* data, uint32_t index, uint8_t bitsPerSymbol) const {
    const uint64_t bitOffset = static_cast<uint64_t>(index) * bitsPerSymbol;
    const uint32_t byteOffset = static_cast<uint32_t>(bitOffset / 8);
    const uint32_t shift = static_cast<uint32_t>(bitOffset % 8);
    uint16_t word = std::to_integer<uint8_t>(data[byteOffset]);
    if (shift + bitsPerSymbol > 8) {
      word |= static_cast<uint16_t>(std::to_integer<uint8_t>(data[byteOffset + 1])) << 8;
    }
    return static_cast<uint8_t>((word >> shift) & ((1U << bitsPerSymbol) - 1));
  }

  Status RecordKeyMatches(const KeyMetadataView& metadata, const RowRecordView& record,
                          std::string_view key, bool& matches) const {
    matches = false;
    const std::string_view prefix = PrefixAt(metadata, record.prefixId);
    if (key.size() != prefix.size() + record.suffixSize || !key.starts_with(prefix)) {
      return Status::Ok();
    }
    const std::string_view suffix = key.substr(prefix.size());
    if (metadata.header.bitsPerSymbol == 8) {
      matches = std::memcmp(suffix.data(), record.encodedSuffix, suffix.size()) == 0;
      return Status::Ok();
    }
    if (metadata.header.bitsPerSymbol == 4) {
      for (uint64_t index = 0; index < record.suffixSize; index += 2) {
        const uint8_t packed = std::to_integer<uint8_t>(record.encodedSuffix[index / 2]);
        const uint8_t first = packed & 0x0f;
        if (first >= metadata.header.alphabetSize ||
            static_cast<uint8_t>(suffix[index]) != metadata.alphabet[first]) {
          return first >= metadata.header.alphabetSize
                     ? Status::Corruption("row key uses an invalid alphabet symbol")
                     : Status::Ok();
        }
        if (index + 1 < record.suffixSize) {
          const uint8_t second = packed >> 4;
          if (second >= metadata.header.alphabetSize ||
              static_cast<uint8_t>(suffix[index + 1]) != metadata.alphabet[second]) {
            return second >= metadata.header.alphabetSize
                       ? Status::Corruption("row key uses an invalid alphabet symbol")
                       : Status::Ok();
          }
        }
      }
    } else {
      for (uint64_t index = 0; index < record.suffixSize; index += 4) {
        const uint64_t byteIndex = (index / 4) * 3;
        uint32_t packed = std::to_integer<uint8_t>(record.encodedSuffix[byteIndex]);
        if (byteIndex + 1 < record.encodedSuffixSize) {
          packed |=
              static_cast<uint32_t>(std::to_integer<uint8_t>(record.encodedSuffix[byteIndex + 1]))
              << 8;
        }
        if (byteIndex + 2 < record.encodedSuffixSize) {
          packed |=
              static_cast<uint32_t>(std::to_integer<uint8_t>(record.encodedSuffix[byteIndex + 2]))
              << 16;
        }
        const uint32_t count =
            static_cast<uint32_t>(std::min<uint64_t>(4, record.suffixSize - index));
        for (uint32_t lane = 0; lane < count; ++lane) {
          const uint8_t code = static_cast<uint8_t>((packed >> (lane * 6)) & 0x3f);
          if (code >= metadata.header.alphabetSize ||
              static_cast<uint8_t>(suffix[index + lane]) != metadata.alphabet[code]) {
            return code >= metadata.header.alphabetSize
                       ? Status::Corruption("row key uses an invalid alphabet symbol")
                       : Status::Ok();
          }
        }
      }
    }
    matches = true;
    return Status::Ok();
  }

  Status DecodeRecordKey(const KeyMetadataView& metadata, const RowRecordView& record,
                         std::string& key) const {
    const std::string_view prefix = PrefixAt(metadata, record.prefixId);
    key.assign(prefix);
    const size_t prefixSize = key.size();
    key.resize(prefixSize + record.suffixSize);
    if (metadata.header.bitsPerSymbol == 8) {
      std::memcpy(key.data() + prefixSize, record.encodedSuffix, record.suffixSize);
      return Status::Ok();
    }
    for (uint32_t index = 0; index < record.suffixSize; ++index) {
      const uint8_t code =
          PackedSymbolAt(record.encodedSuffix, index, metadata.header.bitsPerSymbol);
      if (code >= metadata.header.alphabetSize) {
        return Status::Corruption("row key uses an invalid alphabet symbol");
      }
      key[prefixSize + index] = static_cast<char>(metadata.alphabet[code]);
    }
    return Status::Ok();
  }

  void WriteEncodedSuffix(std::byte* output, std::string_view suffix,
                          const KeyEncodingPlan& plan) const {
    const uint32_t outputSize =
        PackedByteCount(static_cast<uint32_t>(suffix.size()), plan.bitsPerSymbol);
    if (plan.bitsPerSymbol == 8) {
      std::memcpy(output, suffix.data(), suffix.size());
      return;
    }
    if (plan.bitsPerSymbol == 4) {
      for (size_t index = 0; index < suffix.size(); index += 2) {
        uint8_t packed = plan.symbolCodes[static_cast<uint8_t>(suffix[index])];
        if (index + 1 < suffix.size()) {
          packed |=
              static_cast<uint8_t>(plan.symbolCodes[static_cast<uint8_t>(suffix[index + 1])] << 4);
        }
        output[index / 2] = static_cast<std::byte>(packed);
      }
    } else {
      for (size_t index = 0; index < suffix.size(); index += 4) {
        const uint32_t count = static_cast<uint32_t>(std::min<size_t>(4, suffix.size() - index));
        uint32_t packed = 0;
        for (uint32_t lane = 0; lane < count; ++lane) {
          packed |=
              static_cast<uint32_t>(plan.symbolCodes[static_cast<uint8_t>(suffix[index + lane])])
              << (lane * 6);
        }
        const size_t byteIndex = (index / 4) * 3;
        output[byteIndex] = static_cast<std::byte>(packed & 0xff);
        if (byteIndex + 1 < outputSize) {
          output[byteIndex + 1] = static_cast<std::byte>((packed >> 8) & 0xff);
        }
        if (byteIndex + 2 < outputSize) {
          output[byteIndex + 2] = static_cast<std::byte>((packed >> 16) & 0xff);
        }
      }
    }
  }

  Status StageChunkLocked(uint32_t partitionId, SpillPartition& partition, std::string_view column,
                          uint64_t columnHash, uint64_t rowId,
                          std::span<const StageInput> entries) {
    if (entries.empty()) {
      return Status::Ok();
    }
    uint64_t chunkSize = sizeof(detail::StageChunkHeader) + column.size();
    for (const StageInput& entry : entries) {
      uint64_t recordSize = sizeof(uint64_t) +
                            Varint32Size(static_cast<uint32_t>(entry.key.size())) +
                            Varint32Size(static_cast<uint32_t>(entry.value.size()));
      if (!CheckedAdd(recordSize, entry.key.size(), recordSize) ||
          !CheckedAdd(recordSize, entry.value.size(), recordSize) ||
          !CheckedAdd(chunkSize, recordSize, chunkSize)) {
        return Status::InvalidArgument("staged chunk is too large");
      }
    }
    if (chunkSize > std::numeric_limits<uint32_t>::max() && entries.size() > 1) {
      const size_t midpoint = entries.size() / 2;
      Status status = StageChunkLocked(partitionId, partition, column, columnHash, rowId,
                                       entries.first(midpoint));
      if (!status) {
        return status;
      }
      return StageChunkLocked(partitionId, partition, column, columnHash, rowId,
                              entries.subspan(midpoint));
    }
    if (chunkSize > std::numeric_limits<uint32_t>::max() ||
        chunkSize > std::numeric_limits<size_t>::max() ||
        entries.size() > std::numeric_limits<uint32_t>::max()) {
      return Status::InvalidArgument("staged chunk must be smaller than 4 GiB");
    }

    std::vector<std::byte> packed;
    packed.reserve(static_cast<size_t>(chunkSize));
    detail::StageChunkHeader header;
    header.chunkSize = static_cast<uint32_t>(chunkSize);
    header.recordCount = static_cast<uint32_t>(entries.size());
    header.columnHash = columnHash;
    header.rowId = rowId;
    header.columnSize = static_cast<uint32_t>(column.size());
    AppendRaw(packed, &header, sizeof(header));
    AppendRaw(packed, column.data(), column.size());
    for (const StageInput& entry : entries) {
      AppendRaw(packed, &entry.keyHash, sizeof(entry.keyHash));
      const size_t lengthsOffset = packed.size();
      packed.resize(lengthsOffset + Varint32Size(static_cast<uint32_t>(entry.key.size())) +
                    Varint32Size(static_cast<uint32_t>(entry.value.size())));
      size_t lengthCursor = lengthsOffset;
      lengthCursor +=
          WriteVarint32(packed.data() + lengthCursor, static_cast<uint32_t>(entry.key.size()));
      WriteVarint32(packed.data() + lengthCursor, static_cast<uint32_t>(entry.value.size()));
      AppendRaw(packed, entry.key.data(), entry.key.size());
      AppendRaw(packed, entry.value.data(), entry.value.size());
    }
    if (packed.size() != chunkSize) {
      return Status::Corruption("staged chunk size mismatch");
    }

    partition.used = true;
    if (!partition.spilled && partition.buffer.size() <= stageMemoryLimitPerPartition_ &&
        packed.size() <= stageMemoryLimitPerPartition_ - partition.buffer.size()) {
      AppendRaw(partition.buffer, packed.data(), packed.size());
    } else {
      Status status = Status::Ok();
      if (!partition.spilled) {
        status = EnsureStageFileOpen(partitionId, partition);
        if (!status) {
          return status;
        }
        partition.spilled = true;
        status = FlushStageBuffer(partition);
        if (!status) {
          return status;
        }
      }
      const uint64_t ioBufferLimit =
          std::min<uint64_t>(options_.stageBufferBytes, stageMemoryLimitPerPartition_);
      if (!partition.buffer.empty() && partition.buffer.size() + packed.size() > ioBufferLimit) {
        status = FlushStageBuffer(partition);
        if (!status) {
          return status;
        }
      }
      if (packed.size() > ioBufferLimit) {
        status = detail::WriteAllAt(partition.file.Get(), packed.data(), packed.size(),
                                    partition.persistedSize);
        if (!status) {
          return status;
        }
        partition.persistedSize += packed.size();
      } else {
        partition.buffer.reserve(static_cast<size_t>(ioBufferLimit));
        AppendRaw(partition.buffer, packed.data(), packed.size());
      }
    }

    stagedEntryCount_.fetch_add(entries.size(), std::memory_order_relaxed);
    return Status::Ok();
  }

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
    if (!readOnly_ && options_.oneShotBuild && IndexHeader()->itemCount != 0) {
      CloseFiles();
      return Status::InvalidArgument(
          "oneShotBuild requires an empty database and cannot append to an existing build");
    }
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
      stageMemoryLimitPerPartition_ = std::max<uint64_t>(
          1, options_.memoryBudgetBytes / 2 / std::max<size_t>(1, spillPartitions_.size()));
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
      // Compact stage records retain the key hash plus two small varints. The
      // chunk/column header is amortized over a batch.
      uint64_t bytesPerEntry = sizeof(uint64_t) + 4;
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
    for (uint64_t bucketCount = kControlGroupWidth;
         bucketCount <= (1ULL << 31) && best < options_.maxEntriesPerRow; bucketCount <<= 1) {
      const uint64_t candidate = std::min<uint64_t>(
          options_.maxEntriesPerRow, static_cast<uint64_t>(bucketCount * kRowLoadFactor));
      uint64_t estimated = sizeof(detail::RowBlockHeader) + 32;
      uint64_t bucketBytes = 0;
      uint64_t payloadBytes = 0;
      const uint64_t payloadBytesPerEntry =
          static_cast<uint64_t>(options_.averageKeyBytes) + options_.averageValueBytes +
          Varint32Size(options_.averageKeyBytes) + Varint32Size(options_.averageValueBytes);
      // One control byte plus a conservative 32-bit record offset. Rows below
      // 16 MiB actually use a 24-bit offset.
      if (!CheckedMultiply(bucketCount, 5, bucketBytes) ||
          !CheckedMultiply(candidate, payloadBytesPerEntry, payloadBytes) ||
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
    uint64_t residentStageBytes = 0;
    uint32_t usedPartitions = 0;
    for (auto& partitionPtr : spillPartitions_) {
      SpillPartition& partition = *partitionPtr;
      if (!partition.used) {
        continue;
      }
      if (partition.spilled) {
        Status status = FlushStageBuffer(partition);
        if (!status) {
          writePoisoned_.store(true, std::memory_order_release);
          return status;
        }
      }
      const uint64_t partitionBytes =
          partition.spilled ? partition.persistedSize - sizeof(detail::StageFileHeader)
                            : partition.buffer.size();
      maximumPartitionBytes = std::max(maximumPartitionBytes, partitionBytes);
      if (!partition.spilled) {
        residentStageBytes += partitionBytes;
      }
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
      const uint64_t availableWorkerMemory = options_.memoryBudgetBytes > residentStageBytes
                                                 ? options_.memoryBudgetBytes - residentStageBytes
                                                 : 1;
      const uint64_t memoryWorkers = std::max<uint64_t>(
          1, availableWorkerMemory / std::max<uint64_t>(1, estimatedWorkerMemory));
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
    detail::MappedFile map;
    const std::byte* bytes = nullptr;
    uint64_t byteCount = 0;
    if (partition.spilled) {
      if (partition.persistedSize < sizeof(detail::StageFileHeader)) {
        return Status::Corruption("stage file is truncated");
      }
      Status status = map.MapReadOnly(partition.file.Get(), partition.persistedSize);
      if (!status) {
        return status;
      }
      bytes = static_cast<const std::byte*>(map.Data());
      detail::StageFileHeader fileHeader;
      std::memcpy(&fileHeader, bytes, sizeof(fileHeader));
      if (!MagicEquals(fileHeader.magic, detail::kStageFileMagic) ||
          fileHeader.version != detail::kStorageVersion ||
          fileHeader.headerSize != sizeof(fileHeader) || fileHeader.partitionId != partitionId ||
          fileHeader.partitionCount != spillPartitions_.size()) {
        return Status::Corruption("stage file header is invalid");
      }
      bytes += sizeof(fileHeader);
      byteCount = partition.persistedSize - sizeof(fileHeader);
    } else {
      bytes = partition.buffer.data();
      byteCount = partition.buffer.size();
    }
    if (byteCount == 0) {
      return Status::Corruption("used stage partition is empty");
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
    const uint64_t maximumGroupsInFile = byteCount / sizeof(detail::StageChunkHeader);
    if (groupEstimateValid) {
      approximateGroups = std::min(approximateGroups, maximumGroupsInFile);
    }
    if (groupEstimateValid && approximateGroups <= std::numeric_limits<size_t>::max()) {
      groupIndexes.reserve(static_cast<size_t>(approximateGroups));
      groups.reserve(static_cast<size_t>(approximateGroups));
    }

    uint64_t offset = 0;
    while (offset < byteCount) {
      if (sizeof(detail::StageChunkHeader) > byteCount - offset) {
        return Status::Corruption("stage file has a partial chunk header");
      }
      detail::StageChunkHeader chunk;
      std::memcpy(&chunk, bytes + offset, sizeof(chunk));
      if (chunk.magic != detail::kStageChunkMagic || chunk.version != detail::kStorageVersion ||
          chunk.headerSize != sizeof(chunk) || chunk.columnHash == 0 || chunk.recordCount == 0 ||
          chunk.columnSize == 0 || chunk.chunkSize < sizeof(chunk) + chunk.columnSize ||
          chunk.chunkSize > byteCount - offset) {
        return Status::Corruption("stage chunk header is invalid");
      }
      if (detail::MixHashes(chunk.columnHash, chunk.rowId) % spillPartitions_.size() !=
          partitionId) {
        return Status::Corruption("stage chunk is in the wrong partition");
      }

      const std::byte* chunkBytes = bytes + offset;
      const char* columnData = reinterpret_cast<const char*>(chunkBytes + sizeof(chunk));
      const std::string_view column(columnData, chunk.columnSize);
      const StageGroupKey groupKey{chunk.columnHash, chunk.rowId, column};
      auto [iterator, inserted] = groupIndexes.emplace(groupKey, groups.size());
      if (inserted) {
        groups.push_back(
            StageGroup{.column = column, .columnHash = chunk.columnHash, .rowId = chunk.rowId});
      }
      StageGroup& group = groups[iterator->second];
      size_t cursor = sizeof(chunk) + chunk.columnSize;
      for (uint32_t recordIndex = 0; recordIndex < chunk.recordCount; ++recordIndex) {
        if (sizeof(uint64_t) > chunk.chunkSize - cursor) {
          return Status::Corruption("stage chunk has a partial key hash");
        }
        uint64_t keyHash = 0;
        std::memcpy(&keyHash, chunkBytes + cursor, sizeof(keyHash));
        cursor += sizeof(keyHash);
        uint32_t keySize = 0;
        uint32_t valueSize = 0;
        if (keyHash == 0 || !ReadVarint32(chunkBytes, chunk.chunkSize, cursor, keySize) ||
            !ReadVarint32(chunkBytes, chunk.chunkSize, cursor, valueSize) || keySize == 0 ||
            keySize > chunk.chunkSize - cursor || valueSize > chunk.chunkSize - cursor - keySize) {
          return Status::Corruption("stage chunk record is invalid");
        }
        const char* keyData = reinterpret_cast<const char*>(chunkBytes + cursor);
        const std::byte* valueData = chunkBytes + cursor + keySize;
        group.entries.push_back(EntryView{.key = std::string_view(keyData, keySize),
                                          .value = std::span<const std::byte>(valueData, valueSize),
                                          .keyHash = keyHash});
        cursor += static_cast<size_t>(keySize) + valueSize;
      }
      if (cursor != chunk.chunkSize) {
        return Status::Corruption("stage chunk has trailing bytes");
      }
      offset += chunk.chunkSize;
    }
    if (offset != byteCount) {
      return Status::Corruption("stage file has trailing bytes");
    }

    for (const StageGroup& group : groups) {
      Status status = WriteStageGroup(group);
      if (!status) {
        return status;
      }
    }
    return Status::Ok();
  }

  Status WriteStageGroup(const StageGroup& group) {
    std::vector<OwnedEntry> existing;
    Status status = Status::Ok();
    if (!options_.oneShotBuild) {
      status = LoadExistingRow(group.column, group.columnHash, group.rowId, existing);
      if (!status) {
        return status;
      }
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
    const uint64_t metadataOffset = AlignUp8(sizeof(header) + header.columnSize);
    const uint64_t localIndexOffset = AlignUp64(metadataOffset + header.keyMetadataBytesSize);
    const uint64_t offsetsOffset = localIndexOffset + header.bucketCount;
    const uint64_t recordsOffset = localIndexOffset + static_cast<uint64_t>(header.bucketCount) *
                                                          (header.recordOffsetWidth + 1);
    KeyMetadataView metadata;
    status = ParseKeyMetadata(
        std::span<const std::byte>(block.data() + metadataOffset, header.keyMetadataBytesSize),
        metadata);
    if (!status) {
      return status;
    }
    const std::span<const std::byte> records(block.data() + recordsOffset, header.recordBytesSize);
    entries.reserve(header.itemCount);
    for (uint32_t index = 0; index < header.bucketCount; ++index) {
      const bool interleaved = header.recordOffsetWidth == 3;
      const uint64_t bucketOffset = localIndexOffset + static_cast<uint64_t>(index) * 4;
      const uint8_t control =
          std::to_integer<uint8_t>(block[interleaved ? bucketOffset : localIndexOffset + index]);
      if (control == 0) {
        continue;
      }
      const std::byte* encodedOffset =
          interleaved ? block.data() + bucketOffset + 1
                      : block.data() + offsetsOffset +
                            static_cast<uint64_t>(index) * header.recordOffsetWidth;
      const uint32_t recordOffset = ReadRecordOffset(encodedOffset, header.recordOffsetWidth);
      RowRecordView record;
      status = ParseRowRecord(records, recordOffset, metadata, record);
      if (!status) {
        return status;
      }
      OwnedEntry entry;
      status = DecodeRecordKey(metadata, record, entry.key);
      if (!status) {
        return status;
      }
      if (KeyFingerprint(NormalizeHash(detail::HashString(entry.key))) != control) {
        return Status::Corruption("row bucket fingerprint does not match its key");
      }
      entry.value.resize(record.valueSize);
      if (record.valueSize != 0) {
        std::memcpy(entry.value.data(), record.value, record.valueSize);
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
    if (entries.empty() || entries.size() > std::numeric_limits<uint32_t>::max()) {
      return Status::InvalidArgument("row has an unsupported number of entries");
    }

    auto dedupBucketCountForEntries = [](uint64_t entryCount) {
      const uint64_t requiredBuckets =
          static_cast<uint64_t>(static_cast<long double>(entryCount) / 0.75) + 1;
      return RoundUpPowerOfTwo(requiredBuckets, kControlGroupWidth);
    };
    auto finalBucketCountForEntries = [](uint64_t entryCount) {
      const uint64_t requiredBuckets =
          static_cast<uint64_t>(static_cast<long double>(entryCount) / kRowLoadFactor) + 1;
      return RoundUpPowerOfTwo(requiredBuckets, kControlGroupWidth);
    };

    uint32_t uniqueCount = 0;
    uint32_t bucketCount = 0;
    std::vector<TemporaryKeySlot> temporary;
    std::vector<uint32_t> uniqueEntryIndices;
    std::vector<std::byte> oneShotControls;
    std::vector<uint32_t> oneShotBucketEntries;

    if (options_.oneShotBuild) {
      const uint64_t bucketCount64 = finalBucketCountForEntries(entries.size());
      if (bucketCount64 == 0 || bucketCount64 > std::numeric_limits<uint32_t>::max()) {
        return Status::InvalidArgument("row local index is too large");
      }
      bucketCount = static_cast<uint32_t>(bucketCount64);
      const uint32_t groupCount = bucketCount / kControlGroupWidth;
      oneShotControls.assign(bucketCount, std::byte{0});
      oneShotBucketEntries.assign(bucketCount, std::numeric_limits<uint32_t>::max());

      // The scratch table has the exact persisted probing layout. Its 1-byte
      // controls plus 4-byte entry indices replace the normal 16-byte
      // TemporaryKeySlot table, and the selected bucket is reused directly
      // when entry indices become record offsets below. The one-shot contract
      // guarantees unique keys, so this path deliberately does not probe old
      // keys for duplicates.
      for (size_t entryIndex = 0; entryIndex < entries.size(); ++entryIndex) {
        const EntryView& entry = entries[entryIndex];
        const uint64_t keyHash = NormalizeHash(entry.keyHash);
        const uint32_t startGroup = static_cast<uint32_t>(keyHash) & (groupCount - 1);
        uint32_t bucketIndex = bucketCount;
        for (uint32_t groupProbe = 0; groupProbe < groupCount; ++groupProbe) {
          const uint32_t groupIndex = (startGroup + groupProbe) & (groupCount - 1);
          const uint32_t groupBase = groupIndex * kControlGroupWidth;
          const std::byte* controls = oneShotControls.data() + groupBase;
          const uint16_t emptyLanes = ControlMatchMask(controls, 0);
          if (emptyLanes != 0) {
            bucketIndex = groupBase + std::countr_zero(emptyLanes);
            break;
          }
        }
        if (bucketIndex == bucketCount) {
          return Status::Corruption("one-shot row control table is unexpectedly full");
        }
        oneShotControls[bucketIndex] = static_cast<std::byte>(KeyFingerprint(keyHash));
        oneShotBucketEntries[bucketIndex] = static_cast<uint32_t>(entryIndex);
      }
      uniqueCount = static_cast<uint32_t>(entries.size());
    } else {
      // The update-capable path grows according to unique keys. Duplicate
      // writes replace an entry index and retain last-write-wins semantics.
      const uint64_t initialEntryEstimate =
          std::min<uint64_t>({entries.size(), kMaximumDedupPresizeEntries,
                              RoutingTargetEntries(targetEntriesPerRow_)});
      const uint64_t initialBucketCount = dedupBucketCountForEntries(initialEntryEstimate);
      if (initialBucketCount == 0 || initialBucketCount > std::numeric_limits<uint32_t>::max()) {
        return Status::InvalidArgument("row local index is too large");
      }
      temporary.resize(static_cast<size_t>(initialBucketCount));
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

      const uint64_t bucketCount64 = finalBucketCountForEntries(uniqueCount);
      if (bucketCount64 == 0 || bucketCount64 > std::numeric_limits<uint32_t>::max()) {
        return Status::InvalidArgument("row local index is too large");
      }
      bucketCount = static_cast<uint32_t>(bucketCount64);
      uniqueEntryIndices.reserve(uniqueCount);
      for (const TemporaryKeySlot& slot : temporary) {
        if (slot.keyHash != 0) {
          uniqueEntryIndices.push_back(static_cast<uint32_t>(slot.entryIndex));
        }
      }
      if (uniqueEntryIndices.size() != uniqueCount) {
        return Status::Corruption("row unique-key table has an inconsistent item count");
      }
    }

    const KeyEncodingPlan encoding = ChooseKeyEncoding(entries, uniqueEntryIndices);
    std::vector<std::byte> keyMetadata;
    Status status = SerializeKeyMetadata(encoding, keyMetadata);
    if (!status) {
      return status;
    }
    if (encoding.encodedBytes < keyMetadata.size()) {
      return Status::Corruption("row key encoding size is inconsistent");
    }
    const uint64_t recordBytesSize = encoding.encodedBytes - keyMetadata.size();
    if (recordBytesSize > std::numeric_limits<uint32_t>::max() ||
        column.size() > std::numeric_limits<uint32_t>::max() ||
        keyMetadata.size() > std::numeric_limits<uint32_t>::max()) {
      return Status::InvalidArgument(
          "one routed row exceeds the 4 GiB compact-row limit; increase "
          "expectedEntryCountPerColumn or reduce target row size");
    }
    const uint8_t recordOffsetWidth = recordBytesSize <= 0x00ffffff ? 3 : 4;
    const uint64_t metadataOffset = AlignUp8(sizeof(detail::RowBlockHeader) + column.size());
    const uint64_t localIndexOffset = AlignUp64(metadataOffset + keyMetadata.size());
    uint64_t offsetsOffset = 0;
    uint64_t recordsOffset = 0;
    uint64_t blockSize = 0;
    uint64_t localIndexBytes = 0;
    if (!CheckedAdd(localIndexOffset, bucketCount, offsetsOffset) ||
        !CheckedMultiply(bucketCount, recordOffsetWidth + 1, localIndexBytes) ||
        !CheckedAdd(localIndexOffset, localIndexBytes, recordsOffset) ||
        !CheckedAdd(recordsOffset, recordBytesSize, blockSize) ||
        blockSize > std::numeric_limits<uint32_t>::max() ||
        blockSize > std::numeric_limits<size_t>::max()) {
      return Status::InvalidArgument("row block exceeds the 4 GiB format limit");
    }

    detail::RowBlockHeader header;
    header.sequence = sequence;
    header.columnHash = columnHash;
    header.rowId = rowId;
    header.columnSize = static_cast<uint32_t>(column.size());
    header.itemCount = uniqueCount;
    header.bucketCount = bucketCount;
    header.keyMetadataBytesSize = static_cast<uint32_t>(keyMetadata.size());
    header.recordBytesSize = static_cast<uint32_t>(recordBytesSize);
    header.blockSize = static_cast<uint32_t>(blockSize);
    header.recordOffsetWidth = recordOffsetWidth;

    block.assign(static_cast<size_t>(blockSize), std::byte{0});
    std::memcpy(block.data(), &header, sizeof(header));
    std::memcpy(block.data() + sizeof(header), column.data(), column.size());
    std::memcpy(block.data() + metadataOffset, keyMetadata.data(), keyMetadata.size());

    const uint32_t groupCount = bucketCount / kControlGroupWidth;
    const bool interleaved = recordOffsetWidth == 3;
    uint32_t recordCursor = 0;
    auto appendEntry = [&](uint32_t bucketIndex, size_t entryIndex, uint64_t keyHash) -> Status {
      if (entryIndex >= entries.size()) {
        return Status::Corruption("row builder has an invalid entry index");
      }
      const uint8_t fingerprint = KeyFingerprint(keyHash);
      if (interleaved) {
        std::byte* bucket =
            block.data() + localIndexOffset + static_cast<uint64_t>(bucketIndex) * 4;
        bucket[0] = static_cast<std::byte>(fingerprint);
        WriteRecordOffset(bucket + 1, recordCursor, recordOffsetWidth);
      } else {
        block[localIndexOffset + bucketIndex] = static_cast<std::byte>(fingerprint);
        WriteRecordOffset(
            block.data() + offsetsOffset + static_cast<uint64_t>(bucketIndex) * recordOffsetWidth,
            recordCursor, recordOffsetWidth);
      }

      const EntryView& entry = entries[entryIndex];
      const std::string_view suffix = EntrySuffix(encoding, entryIndex, entry.key);
      std::byte* record = block.data() + recordsOffset + recordCursor;
      recordCursor +=
          static_cast<uint32_t>(WriteVarint32(record, static_cast<uint32_t>(suffix.size())));
      record = block.data() + recordsOffset + recordCursor;
      recordCursor +=
          static_cast<uint32_t>(WriteVarint32(record, static_cast<uint32_t>(entry.value.size())));
      if (!encoding.prefixes.empty()) {
        block[recordsOffset + recordCursor++] =
            static_cast<std::byte>(encoding.prefixIdsByEntry[entryIndex]);
      }
      const uint32_t encodedSuffixSize =
          PackedByteCount(static_cast<uint32_t>(suffix.size()), encoding.bitsPerSymbol);
      WriteEncodedSuffix(block.data() + recordsOffset + recordCursor, suffix, encoding);
      recordCursor += encodedSuffixSize;
      if (!entry.value.empty()) {
        std::memcpy(block.data() + recordsOffset + recordCursor, entry.value.data(),
                    entry.value.size());
      }
      recordCursor += static_cast<uint32_t>(entry.value.size());
      return Status::Ok();
    };

    if (options_.oneShotBuild) {
      for (uint32_t bucketIndex = 0; bucketIndex < bucketCount; ++bucketIndex) {
        if (oneShotControls[bucketIndex] == std::byte{0}) {
          continue;
        }
        const uint32_t entryIndex = oneShotBucketEntries[bucketIndex];
        if (entryIndex >= entries.size()) {
          return Status::Corruption("one-shot bucket points outside the staged entries");
        }
        status = appendEntry(bucketIndex, entryIndex, NormalizeHash(entries[entryIndex].keyHash));
        if (!status) {
          return status;
        }
      }
    } else {
      for (const TemporaryKeySlot& temporarySlot : temporary) {
        if (temporarySlot.keyHash == 0) {
          continue;
        }
        const uint32_t startGroup = static_cast<uint32_t>(temporarySlot.keyHash) & (groupCount - 1);
        uint32_t bucketIndex = bucketCount;
        for (uint32_t groupProbe = 0; groupProbe < groupCount; ++groupProbe) {
          const uint32_t groupIndex = (startGroup + groupProbe) & (groupCount - 1);
          const uint32_t groupBase = groupIndex * kControlGroupWidth;
          std::byte* groupData = block.data() + localIndexOffset +
                                 static_cast<uint64_t>(groupBase) * (interleaved ? 4 : 1);
          const uint16_t emptyLanes = interleaved ? InterleavedControlMatchMask(groupData, 0)
                                                  : ControlMatchMask(groupData, 0);
          if (emptyLanes != 0) {
            bucketIndex = groupBase + std::countr_zero(emptyLanes);
            break;
          }
        }
        if (bucketIndex == bucketCount) {
          return Status::Corruption("row control table is unexpectedly full");
        }
        status = appendEntry(bucketIndex, temporarySlot.entryIndex, temporarySlot.keyHash);
        if (!status) {
          return status;
        }
      }
    }

    if (recordCursor != recordBytesSize) {
      return Status::Corruption("packed row record size mismatch");
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
    const RowShard& shard = *rowShards_[bucket.shardId];
    if (shard.map.IsMapped()) {
      const uint64_t columnOffset = bucket.blockOffset + sizeof(header);
      if (columnOffset > shard.map.Size() || column.size() > shard.map.Size() - columnOffset) {
        return Status::Corruption("row column extends beyond its value file");
      }
      const char* storedColumn = reinterpret_cast<const char*>(
          static_cast<const std::byte*>(shard.map.Data()) + columnOffset);
      matches = std::memcmp(storedColumn, column.data(), column.size()) == 0;
    } else {
      std::string storedColumn(header.columnSize, '\0');
      status = ReadRowAt(bucket.shardId, storedColumn.data(), storedColumn.size(),
                         bucket.blockOffset + sizeof(header));
      if (!status) {
        return status;
      }
      matches = storedColumn == column;
    }
    if (matches && outputHeader != nullptr) {
      *outputHeader = header;
    }
    return Status::Ok();
  }

  Status ValidateBlockHeader(const detail::RowBlockHeader& header) const {
    if (header.magic != detail::kRowBlockMagic || header.version != detail::kStorageVersion ||
        header.headerSize != sizeof(detail::RowBlockHeader) || header.columnHash == 0 ||
        header.sequence == 0 || !std::has_single_bit(header.bucketCount) ||
        header.bucketCount < kControlGroupWidth || header.itemCount > header.bucketCount ||
        (header.recordOffsetWidth != 3 && header.recordOffsetWidth != 4) ||
        header.keyMetadataBytesSize < sizeof(detail::RowKeyMetadataHeader)) {
      return Status::Corruption("row block header is invalid");
    }
    const uint64_t metadataOffset = AlignUp8(sizeof(detail::RowBlockHeader) + header.columnSize);
    const uint64_t localIndexOffset = AlignUp64(metadataOffset + header.keyMetadataBytesSize);
    uint64_t localIndexBytes = 0;
    uint64_t expectedSize = 0;
    if (!CheckedMultiply(header.bucketCount, header.recordOffsetWidth + 1, localIndexBytes) ||
        !CheckedAdd(localIndexOffset, localIndexBytes, expectedSize) ||
        !CheckedAdd(expectedSize, header.recordBytesSize, expectedSize) ||
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
      partition->spilled = false;
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
    targetEntriesPerRow_ = header.targetEntriesPerRow;
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
    targetEntriesPerRow_ = 0;
    shardCount_ = 0;
    spillPartitionCount_ = 0;
    stageMemoryLimitPerPartition_ = 0;
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
  uint32_t targetEntriesPerRow_ = 0;
  uint32_t shardCount_ = 0;
  uint32_t spillPartitionCount_ = 0;
  uint64_t stageMemoryLimitPerPartition_ = 0;
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

Status Database::PutStructs(std::string_view column, std::span<const StructEntry> entries) {
  return impl_ == nullptr ? Status::NotOpen("database is not open")
                          : impl_->PutStructs(column, entries);
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

Status Database::PutRowStructs(std::string_view column, uint64_t rowId,
                               std::span<const RowStructEntry> entries) {
  return impl_ == nullptr ? Status::NotOpen("database is not open")
                          : impl_->PutRowStructs(column, rowId, entries);
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

Status Database::GetRowStruct(std::string_view column, uint64_t rowId, std::string_view key,
                              std::vector<std::byte>& flatBufferBytes) const {
  return impl_ == nullptr ? Status::NotOpen("database is not open")
                          : impl_->Get(column, rowId, key, flatBufferBytes);
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
