#pragma once

#include <array>
#include <bit>
#include <cstdint>

namespace LumoDB::detail {

static_assert(std::endian::native == std::endian::little,
              "LumoDB currently stores fixed headers in little-endian form");

inline constexpr std::array<char, 8> kValueFileMagic = {'L', 'U', 'M', 'V',
                                                        '0', '0', '0', '2'};
inline constexpr std::array<char, 8> kLegacyValueFileMagic = {'L', 'U', 'M', 'V',
                                                              '0', '0', '0', '1'};
inline constexpr std::array<char, 8> kIndexFileMagic = {'L', 'U', 'M', 'I',
                                                        '0', '0', '0', '2'};
inline constexpr std::array<char, 8> kRowValueFileMagic = {'L', 'U', 'M', 'R',
                                                           'V', '0', '0', '1'};
inline constexpr std::array<char, 8> kRowIndexFileMagic = {'L', 'U', 'M', 'R',
                                                           'I', '0', '0', '2'};
inline constexpr uint32_t kStorageVersion = 1;
inline constexpr uint32_t kRecordMagic = 0x43455256;  // "VREC" little endian.
inline constexpr uint32_t kRowBlockMagic = 0x424C4F52;  // "ROLB" little endian.
inline constexpr uint8_t kBucketEmpty = 0;
inline constexpr uint8_t kBucketFilled = 1;
inline constexpr uint32_t kRowBucketEmpty = 0;
inline constexpr uint32_t kRowBucketFilled = 1;
inline constexpr uint32_t kRowBucketWriting = 2;

struct ValueFileHeader {
  std::array<char, 8> magic = kValueFileMagic;
  uint32_t version = kStorageVersion;
  uint32_t headerSize = sizeof(ValueFileHeader);
  uint64_t reserved = 0;
};

struct ValueRecordHeader {
  uint32_t magic = kRecordMagic;
  uint16_t version = kStorageVersion;
  uint16_t headerSize = sizeof(ValueRecordHeader);
  uint32_t columnSize = 0;
  uint32_t keySize = 0;
  uint64_t valueSize = 0;
};

struct LegacyValueRecordHeader {
  uint32_t magic = kRecordMagic;
  uint16_t version = kStorageVersion;
  uint16_t headerSize = sizeof(LegacyValueRecordHeader);
  uint64_t sequence = 0;
  uint64_t columnHash = 0;
  uint64_t keyHash = 0;
  uint32_t columnSize = 0;
  uint32_t keySize = 0;
  uint64_t valueSize = 0;
};

struct IndexFileHeader {
  std::array<char, 8> magic = kIndexFileMagic;
  uint32_t version = kStorageVersion;
  uint32_t headerSize = sizeof(IndexFileHeader);
  uint64_t bucketCount = 0;
  uint64_t itemCount = 0;
  uint64_t nextSequence = 1;
  std::array<uint8_t, 24> reserved = {};
};

struct IndexBucket {
  uint64_t hash = 0;
  uint64_t recordOffset = 0;
};

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
  uint64_t nextSequence = 1;
  uint32_t shardCount = 0;
  uint32_t reserved32 = 0;
  std::array<uint8_t, 16> reserved = {};
};

struct RowIndexBucket {
  uint32_t state = kRowBucketEmpty;
  uint32_t shardId = 0;
  uint64_t columnHash = 0;
  uint64_t rowId = 0;
  uint64_t blockOffset = 0;
  uint64_t blockSize = 0;
  uint64_t sequence = 0;
  uint64_t reserved64 = 0;
  uint64_t reserved65 = 0;
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
  uint32_t reserved32 = 0;
  uint64_t keyBytesSize = 0;
  uint64_t valueBytesSize = 0;
  uint64_t blockSize = 0;
};

struct RowKeyBucket {
  uint8_t state = kBucketEmpty;
  std::array<uint8_t, 7> reserved = {};
  uint64_t keyHash = 0;
  uint64_t keyOffset = 0;
  uint64_t keySize = 0;
  uint64_t valueOffset = 0;
  uint64_t valueSize = 0;
};

static_assert(sizeof(ValueFileHeader) == 24);
static_assert(sizeof(ValueRecordHeader) == 24);
static_assert(sizeof(LegacyValueRecordHeader) == 48);
static_assert(sizeof(IndexFileHeader) == 64);
static_assert(sizeof(IndexBucket) == 16);
static_assert(sizeof(RowValueFileHeader) == 24);
static_assert(sizeof(RowIndexFileHeader) == 64);
static_assert(sizeof(RowIndexBucket) == 64);
static_assert(sizeof(RowBlockHeader) == 72);
static_assert(sizeof(RowKeyBucket) == 48);

}  // namespace LumoDB::detail
