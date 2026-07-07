#pragma once

#include <cstddef>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include "lumodb/database.h"

#define ASSERT_OK(expression)                         \
  do {                                                \
    const LumoDB::Status status = (expression);        \
    ASSERT_TRUE(status.IsOk()) << status.Message();   \
  } while (false)

#define EXPECT_OK(expression)                         \
  do {                                                \
    const LumoDB::Status status = (expression);        \
    EXPECT_TRUE(status.IsOk()) << status.Message();   \
  } while (false)

namespace LumoDB::test {

inline std::filesystem::path MakeTestDirectory(std::string_view name) {
  std::filesystem::path path =
      std::filesystem::temp_directory_path() / ("lumodb-test-" + std::string(name));
  std::filesystem::remove_all(path);
  std::filesystem::create_directories(path);
  return path;
}

inline std::vector<std::byte> MakeBytes(std::string_view value) {
  std::vector<std::byte> bytes(value.size());
  for (size_t index = 0; index < value.size(); ++index) {
    bytes[index] = static_cast<std::byte>(value[index]);
  }
  return bytes;
}

inline std::string BytesToString(const std::vector<std::byte>& bytes) {
  std::string value(bytes.size(), '\0');
  for (size_t index = 0; index < bytes.size(); ++index) {
    value[index] = static_cast<char>(bytes[index]);
  }
  return value;
}

}  // namespace LumoDB::test
