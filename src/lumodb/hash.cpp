#include "lumodb/hash.h"

#include <bit>
#include <cstddef>
#include <cstdint>

namespace LumoDB::detail {

uint64_t HashString(std::string_view value) {
  constexpr uint64_t kOffsetBasis = 14695981039346656037ULL;
  constexpr uint64_t kPrime = 1099511628211ULL;

  uint64_t hash = kOffsetBasis;
  for (unsigned char ch : value) {
    hash ^= static_cast<uint64_t>(ch);
    hash *= kPrime;
  }
  return hash;
}

uint64_t MixHashes(uint64_t columnHash, uint64_t keyHash) {
  uint64_t value = columnHash ^ std::rotl(keyHash, 23);
  value ^= value >> 30;
  value *= 0xbf58476d1ce4e5b9ULL;
  value ^= value >> 27;
  value *= 0x94d049bb133111ebULL;
  value ^= value >> 31;
  return value;
}

}  // namespace LumoDB::detail
