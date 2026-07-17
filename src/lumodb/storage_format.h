#pragma once

#include <array>
#include <bit>
#include <cstdint>

namespace LumoDB::detail {

static_assert(std::endian::native == std::endian::little,
              "LumoDB currently stores fixed headers in little-endian form");

inline constexpr uint32_t kStorageVersion = 4;
inline constexpr std::array<char, 8> kRowValueFileMagic = {'L', 'U', 'M', 'R', 'V', '0', '0', '4'};
inline constexpr std::array<char, 8> kRowIndexFileMagic = {'L', 'U', 'M', 'R', 'I', '0', '0', '5'};
inline constexpr std::array<char, 8> kStageFileMagic = {'L', 'U', 'M', 'S', 'T', '0', '0', '3'};
inline constexpr uint32_t kRowBlockMagic = 0x34424f52;    // "ROB4".
inline constexpr uint32_t kStageChunkMagic = 0x33434753;  // "SGC3".
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
  uint32_t keyMetadataBytesSize = 0;
  uint32_t recordBytesSize = 0;
  uint32_t blockSize = 0;
  uint8_t recordOffsetWidth = 0;
  std::array<uint8_t, 3> reserved8{};
  uint32_t reserved32 = 0;
  uint64_t reserved64 = 0;
};

// Stored before the local control/offset arrays. Prefixes are addressed by a
// one-byte ID in each record. Suffix bytes are either raw (bitsPerSymbol == 8)
// or losslessly packed through the stored row-local alphabet.
struct RowKeyMetadataHeader {
  uint8_t bitsPerSymbol = 8;
  uint8_t alphabetSize = 0;
  uint16_t prefixCount = 0;
  uint32_t prefixBytesSize = 0;
};

// After key metadata, 24-bit rows store sixteen interleaved
// {control, offset[3]} buckets per aligned 64-byte group. A 32-bit row stores
// one contiguous control byte per bucket followed by four-byte offsets.

struct StageFileHeader {
  std::array<char, 8> magic = kStageFileMagic;
  uint32_t version = kStorageVersion;
  uint32_t headerSize = sizeof(StageFileHeader);
  uint32_t partitionId = 0;
  uint32_t partitionCount = 0;
  uint64_t reserved = 0;
};

// A chunk shares column and row metadata across all of its records. Each
// record then stores only keyHash, varint key/value sizes, key bytes, and value
// bytes. chunkSize includes this header, the column, and every record.
struct StageChunkHeader {
  uint32_t magic = kStageChunkMagic;
  uint16_t version = kStorageVersion;
  uint16_t headerSize = sizeof(StageChunkHeader);
  uint32_t chunkSize = 0;
  uint32_t recordCount = 0;
  uint64_t columnHash = 0;
  uint64_t rowId = 0;
  uint32_t columnSize = 0;
  uint32_t reserved = 0;
};

static_assert(sizeof(RowValueFileHeader) == 24);
static_assert(sizeof(RowIndexFileHeader) == 96);
static_assert(sizeof(RowIndexBucket) == 48);
static_assert(sizeof(RowBlockHeader) == 72);
static_assert(sizeof(RowKeyMetadataHeader) == 8);
static_assert(sizeof(StageFileHeader) == 32);
static_assert(sizeof(StageChunkHeader) == 40);

}  // namespace LumoDB::detail
