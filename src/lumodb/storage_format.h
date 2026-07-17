#pragma once

#include <array>
#include <bit>
#include <cstdint>

namespace LumoDB::detail {

static_assert(std::endian::native == std::endian::little,
              "LumoDB currently stores fixed headers in little-endian form");

inline constexpr uint32_t kStorageVersion = 3;
inline constexpr std::array<char, 8> kRowValueFileMagic = {'L', 'U', 'M', 'R', 'V', '0', '0', '3'};
inline constexpr std::array<char, 8> kRowIndexFileMagic = {'L', 'U', 'M', 'R', 'I', '0', '0', '4'};
inline constexpr std::array<char, 8> kStageFileMagic = {'L', 'U', 'M', 'S', 'T', '0', '0', '2'};
inline constexpr uint32_t kRowBlockMagic = 0x33424f52;     // "ROB3".
inline constexpr uint32_t kStageRecordMagic = 0x32544753;  // "SGT2".
inline constexpr uint64_t kRoutingSeed = 0x9e3779b97f4a7c15ULL;

struct RowValueFileHeader {
  std::array<char, 8> magic = kRowValueFileMagic;
  uint32_t version = kStorageVersion;
  uint32_t headerSize = sizeof(RowValueFileHeader);
  uint64_t reserved = 0;
};

struct RowIndexFileHeader {
  std::array<char, 8> magic = kRowIndexFileMagic;
  uint32_t version = kStorageVersion;
  uint32_t headerSize = sizeof(RowIndexFileHeader);
  uint64_t bucketCount = 0;
  uint64_t itemCount = 0;
  uint64_t entryCount = 0;
  uint64_t nextSequence = 1;
  uint64_t routeCount = 0;
  uint64_t expectedEntryCountPerColumn = 0;
  uint64_t routingSeed = kRoutingSeed;
  uint32_t targetEntriesPerRow = 0;
  uint32_t shardCount = 0;
  uint32_t spillPartitionCount = 0;
  uint32_t expectedAutomaticColumnCount = 1;
  uint64_t expectedExplicitRowCount = 0;
};

// The index only points to immutable row blocks. Empty buckets have
// columnHash == 0; real hashes are normalized to a non-zero value.
struct RowIndexBucket {
  uint64_t columnHash = 0;
  uint64_t rowId = 0;
  uint64_t blockOffset = 0;
  uint64_t sequence = 0;
  uint32_t blockSize = 0;
  uint32_t shardId = 0;
  uint32_t itemCount = 0;
  uint32_t reserved = 0;
};

struct RowBlockHeader {
  uint32_t magic = kRowBlockMagic;
  uint16_t version = kStorageVersion;
  uint16_t headerSize = sizeof(RowBlockHeader);
  uint64_t sequence = 0;
  uint64_t columnHash = 0;
  uint64_t rowId = 0;
  uint32_t columnSize = 0;
  uint32_t itemCount = 0;
  uint32_t bucketCount = 0;
  uint32_t recordBytesSize = 0;
  uint32_t blockSize = 0;
  uint32_t reserved32 = 0;
  uint64_t reserved64 = 0;
};

// Compact 8-byte local bucket. keyFingerprint == 0 means empty. recordOffset
// is relative to the row's packed record region. The full key stored in the
// record is always compared, so a 32-bit fingerprint collision is harmless.
struct RowKeyBucket {
  uint32_t keyFingerprint = 0;
  uint32_t recordOffset = 0;
};

struct StageFileHeader {
  std::array<char, 8> magic = kStageFileMagic;
  uint32_t version = kStorageVersion;
  uint32_t headerSize = sizeof(StageFileHeader);
  uint32_t partitionId = 0;
  uint32_t partitionCount = 0;
  uint64_t reserved = 0;
};

struct StageRecordHeader {
  uint32_t magic = kStageRecordMagic;
  uint16_t version = kStorageVersion;
  uint16_t headerSize = sizeof(StageRecordHeader);
  uint64_t columnHash = 0;
  uint64_t keyHash = 0;
  uint64_t rowId = 0;
  uint32_t columnSize = 0;
  uint32_t keySize = 0;
  uint32_t valueSize = 0;
  uint32_t reserved = 0;
};

static_assert(sizeof(RowValueFileHeader) == 24);
static_assert(sizeof(RowIndexFileHeader) == 96);
static_assert(sizeof(RowIndexBucket) == 48);
static_assert(sizeof(RowBlockHeader) == 64);
static_assert(sizeof(RowKeyBucket) == 8);
static_assert(sizeof(StageFileHeader) == 32);
static_assert(sizeof(StageRecordHeader) == 48);

}  // namespace LumoDB::detail
