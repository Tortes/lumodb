#pragma once

#include <array>
#include <bit>
#include <cstdint>

namespace tdldb::detail {

static_assert(std::endian::native == std::endian::little,
              "tdldb currently stores fixed headers in little-endian form");

inline constexpr std::array<char, 8> kValueFileMagic = {'T', 'D', 'L', 'V',
                                                        '0', '0', '0', '1'};
inline constexpr std::array<char, 8> kIndexFileMagic = {'T', 'D', 'L', 'I',
                                                        '0', '0', '0', '1'};
inline constexpr uint32_t kStorageVersion = 1;
inline constexpr uint32_t kRecordMagic = 0x43455256;  // "VREC" little endian.
inline constexpr uint8_t kBucketEmpty = 0;
inline constexpr uint8_t kBucketFilled = 1;

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
  uint8_t state = kBucketEmpty;
  std::array<uint8_t, 7> reserved = {};
  uint64_t columnHash = 0;
  uint64_t keyHash = 0;
  uint64_t recordOffset = 0;
  uint64_t valueOffset = 0;
  uint64_t valueSize = 0;
  uint64_t recordSize = 0;
  uint64_t sequence = 0;
};

static_assert(sizeof(ValueFileHeader) == 24);
static_assert(sizeof(ValueRecordHeader) == 48);
static_assert(sizeof(IndexFileHeader) == 64);
static_assert(sizeof(IndexBucket) == 64);

}  // namespace tdldb::detail
