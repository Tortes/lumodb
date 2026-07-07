#pragma once

#include <cstdint>
#include <string_view>

namespace tdldb::detail {

uint64_t HashString(std::string_view value);
uint64_t MixHashes(uint64_t columnHash, uint64_t keyHash);

}  // namespace tdldb::detail
